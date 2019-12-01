{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

-- | Locks for recurring, unending tasks. Acquiring a 'TaskLease' allows a worker
-- to exclusively work on a task for a period of time. The worker can then
-- transactionally commit the result of its work, failing if the lock has since
-- been reassigned to another worker (as it would in the case where the worker
-- failed to commit its results before its lease on the lock timed out).
-- All workers must have NTP configured correctly. A worker with a bad clock
-- could lock a task for longer than expected, or steal a lock before it
-- has actually timed out.
-- This design is based on the <https://github.com/apple/foundationdb/blob/master/layers/taskbucket/__init__.py task bucket layer>
-- in FoundationDB.

module FDBStreaming.TaskLease (
  secondsSinceEpoch,
  TaskSpace(..),
  taskSpace,
  TaskID(..),
  TaskName(..),
  AcquiredLease(..),
  isLeaseValid,
  ensureTask,
  EnsureTaskResult(..),
  tryAcquire,
  acquireRandom,
  acquireRandomUnbiased,
  HowAcquired(..),
  release,
  ReleaseResult(..),
  getTaskID,
  isLocked
) where

import           Control.Monad.IO.Class         ( liftIO )
import           Data.Binary.Get                ( runGet
                                                , getWord64le
                                                )
import           Data.ByteString                ( ByteString )
import           Data.ByteString.Lazy           ( fromStrict )
import           Data.Maybe                     ( fromMaybe )
import           Data.Sequence                  ( Seq )
import qualified Data.Sequence                  as Seq
import           Data.String                    ( IsString )
import           Data.Word                      ( Word64 )
import           GHC.Generics                   ( Generic )
import           System.Clock (Clock(Realtime), getTime, toNanoSecs)
import           System.Random                  ( randomRIO )

import           FoundationDB (Transaction)
import qualified FoundationDB as FDB
import           FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options as Op

-- | Uniquely identifies an acquired lease on a 'TaskLease'.
newtype AcquiredLease = AcquiredLease Int
  deriving (Show, Eq, Ord, Num, Generic)

-- | A unique integer identifying a unique task. Generated by 'createTask'. This
-- concept is needed in addition to the user-friendly task name because it
-- gives us a uniformly-distributed space we can use to implement unbiased
-- acquisition of a random task. See 'acquireRandom'.
newtype TaskID = TaskID Word64
  deriving (Show, Eq, Ord, Num, Generic)

-- | A user-provided name for identifying the task.
newtype TaskName = TaskName ByteString
  deriving (Show, Eq, Ord, IsString, Generic)

-- | Represents a logically related set of tasks.
newtype TaskSpace = TaskSpace Subspace
  deriving (Show, Eq, Ord)

taskSpace :: Subspace -> TaskSpace
taskSpace = TaskSpace

-- subspace structure constants

-- | integer one, little endian encoded
oneLE :: ByteString
oneLE = "\x01\x00\x00\x00\x00\x00\x00\x00"

secondsSinceEpoch :: IO Int
secondsSinceEpoch = do
  t <- getTime Realtime
  return $ fromIntegral (toNanoSecs t `div` 1_000_000_000)

available, locked, allTasks, count, lockVersions, timeouts :: ByteString
-- | 'available' records available (unleased) tasks.
--   key is (taskId, taskName). Value is empty.
--   Primarily used to be able to choose a random available task,
--   by getting firstGreaterOrEq to a randomly-chosen taskId.
available    = "av"
-- | 'locked' is a list of locked (leased) tasks.
-- key is (taskId). value is the time at which the lock expires
locked       = "lk"
-- | Lists all tasks, regardless of state.
-- key is (taskName, taskId). Value is empty.
allTasks     = "al"
-- | count stores how many tasks there are total. This is used to set the
-- bounds on the PRNG that selects a random task.
count        = "ct"
-- | Stores the current lease version for each task. The lease version is the
-- unique id of the current extant lease.
-- key is taskId, value is a little endian counter.
lockVersions = "lv"
--- | Stores the current timeout time of each lease.
--    Key is (timesOutAt, name). Value is empty.
--    Note that timesOutAt is first, so we can get the list of locks that have
--    timed out, using a range read.
--    TODO: rename to expiresAts or something! Timeout is misleading -- it's not
--    a duration; it's an instant.
timeouts     = "to"

availableKey :: TaskSpace -> TaskID -> TaskName -> ByteString
availableKey (TaskSpace ss) (TaskID tid) (TaskName nm) =
  FDB.pack ss [ FDB.Bytes available
              , FDB.Int (fromIntegral tid)
              , FDB.Bytes nm
              ]

parseAvailableKey :: TaskSpace -> ByteString -> (TaskID, TaskName)
parseAvailableKey (TaskSpace ss) bs = case FDB.unpack avSS bs of
  Right [FDB.Int tid, FDB.Bytes nm] -> (TaskID (fromIntegral tid), TaskName nm)
  xs -> error $ "parseAvailableKey failed! " ++ show xs

  where avSS = FDB.extend ss [FDB.Bytes available]

lockedKey :: TaskSpace -> TaskID -> TaskName -> ByteString
lockedKey (TaskSpace ss) (TaskID tid) _ =
  FDB.pack ss [ FDB.Bytes locked
              , FDB.Int (fromIntegral tid)
              ]

parseLockedValue :: ByteString -> Int
parseLockedValue v = case FDB.decodeTupleElems v of
  Right [FDB.Int timesOutAt] -> fromIntegral timesOutAt
  x -> error $ "failed to parse timeout " ++ show x

allTasksKey :: TaskSpace -> TaskID -> TaskName -> ByteString
allTasksKey (TaskSpace ss) (TaskID tid) (TaskName nm) =
  FDB.pack ss [ FDB.Bytes allTasks
              , FDB.Bytes nm
              , FDB.Int (fromIntegral tid)
              ]

countKey :: TaskSpace -> ByteString
countKey (TaskSpace ss) = FDB.pack ss [FDB.Bytes count]

lockVersionKey :: TaskSpace -> TaskID -> ByteString
lockVersionKey (TaskSpace ss) (TaskID tid) =
  FDB.pack ss [FDB.Bytes lockVersions, FDB.Int (fromIntegral tid)]

timeoutsKey :: TaskSpace -> Int -> TaskName -> ByteString
timeoutsKey (TaskSpace ss) seconds (TaskName nm) =
  FDB.pack ss [ FDB.Bytes timeouts
              , FDB.Int (fromIntegral seconds)
              , FDB.Bytes nm
              ]

parseTimeoutsKey :: TaskSpace -> ByteString -> (Int, TaskName)
parseTimeoutsKey (TaskSpace ss) k =
  case FDB.unpack timeoutSS k of
    Right [FDB.Int timeout, FDB.Bytes name] -> (fromIntegral timeout, TaskName name)
    x -> error $ "Failed to parse timeouts key: " ++ show x

  where timeoutSS = FDB.extend ss [ FDB.Bytes timeouts ]

-- TODO: incrementing and counting code is getting repeated everywhere. DRY out!
getCount :: TaskSpace -> Transaction Word64
getCount l = do
  let k = countKey l
  FDB.get k >>= FDB.await >>= \case
    Nothing -> return 0
    Just bs -> return $ (runGet getWord64le . fromStrict) bs

incrCount :: TaskSpace -> Transaction ()
incrCount l = do
  let k = countKey l
  FDB.atomicOp k (Op.add oneLE)

getAcquiredLease :: TaskSpace -> TaskID -> Transaction AcquiredLease
getAcquiredLease l taskID = do
  let k = lockVersionKey l taskID
  FDB.get k >>= FDB.await >>= \case
    Nothing -> return (AcquiredLease 0)
    Just bs -> return
               $ AcquiredLease
               $ fromIntegral
               $ (runGet getWord64le . fromStrict) bs

incrAcquiredLease :: TaskSpace -> TaskID -> Transaction ()
incrAcquiredLease l taskID = do
  let k = lockVersionKey l taskID
  FDB.atomicOp k (Op.add oneLE)

-- | Returns True iff the given lease is still the most recent lease for the
-- given task. Does not check whether the lease has expired.
isLeaseValid :: TaskSpace -> TaskID -> AcquiredLease -> Transaction Bool
isLeaseValid l taskID lease = do
  lease' <- getAcquiredLease l taskID
  return (lease == lease')

getTaskID :: TaskSpace -> TaskName -> Transaction (Maybe TaskID)
getTaskID (TaskSpace ss) (TaskName nm) = do
  let k = FDB.pack ss [FDB.Bytes allTasks, FDB.Bytes nm]
  r <- FDB.getKey (FDB.FirstGreaterThan k) >>= FDB.await
  case FDB.unpack ss r of
    Right [FDB.Bytes x, FDB.Bytes y, FDB.Int z]
      | x == allTasks && y == nm -> return (Just (fromIntegral z))
    _ -> return Nothing

data EnsureTaskResult = AlreadyExists TaskID | NewlyCreated TaskID
  deriving (Show, Eq, Ord)

-- | Ensure a task with the given name exists. If not, create it. Returns the
-- task's ID.
ensureTask :: TaskSpace -> TaskName -> Transaction EnsureTaskResult
ensureTask l taskName =
  getTaskID l taskName >>= \case
    Nothing -> createTask
    Just taskID -> return (AlreadyExists taskID)

  where

    createTask = do
      n <- getCount l
      let tid = TaskID n
      makeAvailable tid
      addToTaskList tid
      incrCount l
      return $ NewlyCreated (TaskID n)

    makeAvailable taskID = do
      let avk = availableKey l taskID taskName
      FDB.set avk ""

    addToTaskList taskID = do
      let alk = allTasksKey l taskID taskName
      FDB.set alk ""

-- | Clear the old timeout key for a previously locked task, if it exists.
clearOldTimeoutKey :: TaskSpace -> TaskID -> TaskName -> Transaction ()
clearOldTimeoutKey l taskID taskName = do
  let lkk = lockedKey l taskID taskName
  oldTimesOutAtBytes <- FDB.get lkk >>= FDB.await
  case fmap parseLockedValue oldTimesOutAtBytes of
    Nothing -> return ()
    Just oldTimesOutAt -> do
      let toClear = timeoutsKey l oldTimesOutAt taskName
      FDB.clear toClear

-- | Forces the acquisition of a lock, even if it was already locked. The caller
-- is responsible for ensuring that the lock is not already locked, if such
-- behavior is desired.
acquire :: TaskSpace -> TaskName -> Int -> Transaction AcquiredLease
acquire l taskName seconds =
  getTaskID l taskName >>= \case
    Nothing -> error "impossible happened: tried to acquire nonexistent lock"
    Just taskID -> do
      currTime <- liftIO secondsSinceEpoch
      let timesOutAt = currTime + seconds
      clearOldTimeoutKey l taskID taskName
      let lkk = lockedKey l taskID taskName
      FDB.set lkk (FDB.encodeTupleElems [FDB.Int (fromIntegral timesOutAt)])
      let avk = availableKey l taskID taskName
      FDB.clear avk
      let tok = timeoutsKey l timesOutAt taskName
      FDB.set tok ""
      incrAcquiredLease l taskID
      getAcquiredLease l taskID

-- | Returns the time at which the lock expires, in seconds since the epoch.
-- The time may be in the past, in which case the lock has expired.
-- Returns Nothing if the lock has never been acquired.
getTimesOutAt :: TaskSpace -> TaskName -> Transaction (Maybe Int)
getTimesOutAt l taskName =
  getTaskID l taskName >>= \case
    Nothing -> error "impossible happened: tried to acquire nonexistent lock"
    Just taskID ->
      FDB.get (lockedKey l taskID taskName) >>= FDB.await >>= \case
        Nothing -> return Nothing
        Just timesOutAtBytes -> return $ Just $ parseLockedValue timesOutAtBytes

-- | Returns true if the lease has been acquired and the current acquired lease
-- has not expired.
isLocked :: TaskSpace -> TaskName -> Transaction Bool
isLocked l taskName =
  getTimesOutAt l taskName >>= \case
    Nothing -> return False
    Just timesOutAt -> do
      currTime <- liftIO secondsSinceEpoch
      return (currTime <= timesOutAt)

tryAcquire :: TaskSpace -> TaskName -> Int -> Transaction (Maybe AcquiredLease)
tryAcquire l taskName seconds = do
  currentlyLocked <- isLocked l taskName
  if currentlyLocked
    then return Nothing
    else Just <$> acquire l taskName seconds

data ReleaseResult =
  -- | The lease was successfully released.
  ReleaseSuccess
  -- | The lease was not released, because it had already expired.
  | AlreadyExpired
  -- | The lease is invalid -- either it was never acquired, the provided
  -- 'AcquiredLease' token was created from a different 'TaskName' than the
  -- one provided to 'release', or there is no record of the provided
  -- 'TaskName' whatsoever.
  | InvalidLease
  deriving (Show, Eq, Ord)

-- | Attempts to release a lock that was acquired at AcquiredLease. Returns 'True'
-- if the lock was released successfully. Returns 'False' if the lock has
-- expired and has already been acquired by another worker. If 'False' is
-- returned, the caller may choose to cancel the transaction. This function does
-- not call 'FDB.cancel'.
release :: TaskSpace -> TaskName -> AcquiredLease -> Transaction ReleaseResult
release l taskName@(TaskName nm) lockVersion =
  getTaskID l taskName >>= \case
    Nothing -> return InvalidLease
    Just taskID -> getAcquiredLease l taskID >>= \case
      currVersion | currVersion > lockVersion -> return AlreadyExpired
      currVersion | currVersion < lockVersion -> return InvalidLease
      _ -> do
          let lkk = lockedKey l taskID taskName
          timesOutAtBytes <- FDB.get lkk >>= FDB.await
          case fmap parseLockedValue timesOutAtBytes of
            Nothing -> return InvalidLease
            Just timesOutAt -> do
              FDB.clear lkk
              FDB.clear (timeoutsKey l timesOutAt taskName)
              let avk = availableKey l taskID taskName
              FDB.set avk nm
              return ReleaseSuccess

expiredLocks :: TaskSpace -> Transaction (Seq TaskName)
expiredLocks ls@(TaskSpace ss) = do
  let timeoutSS = FDB.extend ss [FDB.Bytes timeouts ]
  --TODO: pass in time boundaries?
  currTime <- liftIO secondsSinceEpoch
  let end = FDB.pack timeoutSS [FDB.Int $ fromIntegral currTime ]
  let range = (FDB.subspaceRange timeoutSS)
                {FDB.rangeEnd = FDB.FirstGreaterOrEq end}
  res <- FDB.getEntireRange range
  return (fmap (\(k,_) -> snd (parseTimeoutsKey ls k)) res)

availableLocks :: TaskSpace -> Transaction (Seq TaskName)
availableLocks ts@(TaskSpace ss) = do
  let availableSS = FDB.extend ss [FDB.Bytes available]
  let range = FDB.subspaceRange availableSS
  res <- FDB.getEntireRange range
  return $ fmap (\(k,_) -> snd (parseAvailableKey ts k)) res

acquireRandomExpired :: TaskSpace
                     -> Int
                     -> Transaction (Maybe (TaskName, AcquiredLease, HowAcquired))
acquireRandomExpired l seconds = do
  expired <- FDB.withSnapshot (expiredLocks l)
  let n = length expired
  if n > 0
    then do
            i <- liftIO $ randomRIO (0, n - 1)
            let nm = Seq.index expired i
            tv <- acquire l nm seconds
            return $ Just (nm, tv, RandomExpired)
    else return Nothing

--TODO: remove when done debugging
data HowAcquired = RandomExpired | Available
  deriving (Show, Eq, Ord)

-- | Attempt to acquire a random task. Returns the task name and lock version.
-- This is faster than 'acquireRandomExpired' but can cause starvation of
-- expired tasks when there are fewer workers than tasks, if some workers
-- use deliver and others do not.
{-# DEPRECATED acquireRandom "Expired tasks can starve when number of workers is less than number of tasks. Use acquireRandomUnbiased instead." #-}
acquireRandom :: TaskSpace
              -> Int
              -> Transaction (Maybe (TaskName, AcquiredLease, HowAcquired))
acquireRandom l@(TaskSpace ss) seconds = do
  --TODO: fdb task bucket uses UUIDs so it doesn't need to spend latency on this
  -- get.
  n <- getCount l
  rand <- liftIO $ randomRIO (0,n)
  let mkQueryK m = FDB.pack availableSS [ FDB.Int (fromIntegral m) ]
  let queryK = mkQueryK rand
  resultK <- FDB.getKey (FDB.LastLessOrEq queryK) >>= FDB.await
  if not (FDB.contains availableSS resultK)
    --TODO: recurse w/ limit instead? Return watch?
    then do
      let queryK' = mkQueryK n
      resultK' <- FDB.getKey (FDB.LastLessOrEq queryK') >>= FDB.await
      if not (FDB.contains availableSS resultK')
        then acquireRandomExpired l seconds
        else acquireAvailable resultK'
    else acquireAvailable resultK

  where
    acquireAvailable k = case FDB.unpack availableSS k of
      Right [FDB.Int _tid, FDB.Bytes nm] -> do
         tv <- acquire l (TaskName nm) seconds
         return $ Just (TaskName nm, tv, Available)
      _ -> error $ "unexpected key format when acquiring lock: " ++ show k

    availableSS = FDB.extend ss [FDB.Bytes available]

-- | Acquire a random available or expired lease. Unlike the deprecated
-- 'acquireRandom', this is not biased towards available leases and is thus
-- completely fair -- all tasks will progress. However, this has higher overhead
-- -- it does two range reads to get all available and expired leases, while
-- in most cases, acquireRandom does O(1) work. For the number of tasks we are
-- typically using, however, this shouldn't be an issue.
acquireRandomUnbiased :: TaskSpace
                      -> Int
                      -> Transaction (Maybe (TaskName, AcquiredLease, HowAcquired))
acquireRandomUnbiased ts seconds = do
  -- TODO: many more tests to ensure that these snapshot reads are safe and
  -- that no two workers can acquire the same lease.
  expireds <- FDB.withSnapshot $ expiredLocks ts
  availables <- FDB.withSnapshot $ availableLocks ts
  let tasks = expireds <> availables
  let n = length tasks
  if n > 0
    then do
      rand <- liftIO $ randomRIO (0, n - 1)
      let taskName = fromMaybe (error "impossible") (tasks Seq.!? rand)
      tv <- acquire ts taskName seconds
      let how = if rand < length expireds then RandomExpired else Available
      return $ Just (taskName, tv, how)
    else return Nothing