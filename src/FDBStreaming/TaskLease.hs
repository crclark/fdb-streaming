{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

-- | Locks for recurring, unending tasks. Acquiring a 'TaskName' allows a worker
-- to exclusively work on a task for a period of time. The worker can then
-- transactionally commit the result of its work, failing if the lock has since
-- been reassigned to another worker (as it would in the case where the worker
-- failed to commit its results before its lease on the lock timed out).
-- All workers must have NTP configured correctly. A worker with a bad clock
-- could lock a task for longer than expected, or steal a lock before it
-- has actually timed out.
-- This design is based on the <https://github.com/apple/foundationdb/blob/master/layers/taskbucket/__init__.py task bucket layer>
-- in FoundationDB.
--
-- Currently, no logic in FDBStreaming requires mutual exclusion to function, so
-- even if NTP is misconfigured, the exactly-once semantics will be preserved.
-- However, locks are used to reduce conflicts between workers. Performance will
-- degrade if a worker has a rogue clock.
--
-- This implementation is inspired by the FoundationDB
-- <https://github.com/apple/foundationdb/blob/master/layers/taskbucket/__init__.py task bucket layer>, but has several significant differences.
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
  HowAcquired(..),
  release,
  ReleaseResult(..),
  getTaskID,
  isLocked
) where

import           Control.Monad.IO.Class         ( liftIO )
import           Data.ByteString                ( ByteString )
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
import FDBStreaming.Util (parseWord64le, addOneAtomic)

-- | Uniquely identifies an acquired lease on a 'TaskName'.
newtype AcquiredLease = AcquiredLease Int
  deriving (Show, Eq, Ord, Num, Generic)

-- | A unique integer identifying a unique task. Generated by 'ensureTask'. This
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

-- | Creates a task space from a FoundationDB subspace.
taskSpace :: Subspace -> TaskSpace
taskSpace = TaskSpace

-- | Returns the number of seconds since the epoch.
secondsSinceEpoch :: IO Int
secondsSinceEpoch = do
  t <- getTime Realtime
  return $ fromIntegral (toNanoSecs t `div` 1_000_000_000)

-- subspace structure constants
available, locked, allTasks, count, lockVersions, expiresAts :: ByteString
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
--    Key is (expiresAt, name). Value is empty.
--    Note that expiresAt is first, so we can get the list of locks that have
--    timed out, using a range read.
expiresAts     = "to"

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
  Right [FDB.Int expiresAt] -> fromIntegral expiresAt
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

expiresAtKey :: TaskSpace -> Int -> TaskName -> ByteString
expiresAtKey (TaskSpace ss) seconds (TaskName nm) =
  FDB.pack ss [ FDB.Bytes expiresAts
              , FDB.Int (fromIntegral seconds)
              , FDB.Bytes nm
              ]

parseExpiresAtKey :: TaskSpace -> ByteString -> (Int, TaskName)
parseExpiresAtKey (TaskSpace ss) k =
  case FDB.unpack expiresAtSS k of
    Right [FDB.Int expiresAt, FDB.Bytes name] -> (fromIntegral expiresAt, TaskName name)
    x -> error $ "Failed to parse expiresAts key: " ++ show x

  where expiresAtSS = FDB.extend ss [ FDB.Bytes expiresAts ]

getCount :: TaskSpace -> Transaction Word64
getCount l = do
  let k = countKey l
  FDB.get k >>= FDB.await >>= \case
    Nothing -> return 0
    Just bs -> return $ parseWord64le bs

incrCount :: TaskSpace -> Transaction ()
incrCount l = do
  let k = countKey l
  addOneAtomic k

getAcquiredLease :: TaskSpace -> TaskID -> Transaction AcquiredLease
getAcquiredLease l taskID = do
  let k = lockVersionKey l taskID
  FDB.get k >>= FDB.await >>= \case
    Nothing -> return (AcquiredLease 0)
    Just bs -> return $ AcquiredLease $ parseWord64le bs

incrAcquiredLease :: TaskSpace -> TaskID -> Transaction ()
incrAcquiredLease l taskID = do
  let k = lockVersionKey l taskID
  addOneAtomic k

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

-- | Tells us whether a given task already existed or was newly created by
-- 'ensureTask'.
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
clearOldExpiresAtKey :: TaskSpace -> TaskID -> TaskName -> Transaction ()
clearOldExpiresAtKey l taskID taskName = do
  let lkk = lockedKey l taskID taskName
  oldTimesOutAtBytes <- FDB.get lkk >>= FDB.await
  case fmap parseLockedValue oldTimesOutAtBytes of
    Nothing -> return ()
    Just oldTimesOutAt -> do
      let toClear = expiresAtKey l oldTimesOutAt taskName
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
      let expiresAt = currTime + seconds
      clearOldExpiresAtKey l taskID taskName
      let lkk = lockedKey l taskID taskName
      FDB.set lkk (FDB.encodeTupleElems [FDB.Int (fromIntegral expiresAt)])
      let avk = availableKey l taskID taskName
      FDB.clear avk
      let tok = expiresAtKey l expiresAt taskName
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
        Just expiresAtBytes -> return $ Just $ parseLockedValue expiresAtBytes

-- | Returns true if the lease has been acquired and the current acquired lease
-- has not expired.
isLocked :: TaskSpace -> TaskName -> Transaction Bool
isLocked l taskName =
  getTimesOutAt l taskName >>= \case
    Nothing -> return False
    Just expiresAt -> do
      currTime <- liftIO secondsSinceEpoch
      return (currTime <= expiresAt)

-- | Attempt to acquire the given task. If it is already locked by another
-- worker, returns 'Nothing'. Otherwise, returns the acquired lease.
tryAcquire :: TaskSpace -> TaskName -> Int -> Transaction (Maybe AcquiredLease)
tryAcquire l taskName seconds = do
  currentlyLocked <- isLocked l taskName
  if currentlyLocked
    then return Nothing
    else Just <$> acquire l taskName seconds

-- | Encodes all possible results of releasing a lease.
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
          expiresAtBytes <- FDB.get lkk >>= FDB.await
          case fmap parseLockedValue expiresAtBytes of
            Nothing -> return InvalidLease
            Just expiresAt -> do
              FDB.clear lkk
              FDB.clear (expiresAtKey l expiresAt taskName)
              let avk = availableKey l taskID taskName
              FDB.set avk nm
              return ReleaseSuccess

-- | Returns all tasks that were locked, but whose leases have expired.
expiredLocks :: TaskSpace -> Transaction (Seq TaskName)
expiredLocks ls@(TaskSpace ss) = do
  let timeoutSS = FDB.extend ss [FDB.Bytes expiresAts ]
  currTime <- liftIO secondsSinceEpoch
  let end = FDB.pack timeoutSS [FDB.Int $ fromIntegral currTime ]
  let range = (FDB.subspaceRangeQuery timeoutSS)
                {FDB.rangeEnd = FDB.FirstGreaterOrEq end}
  res <- FDB.getEntireRange range
  return (fmap (\(k,_) -> snd (parseExpiresAtKey ls k)) res)

-- | Returns all tasks in the available state. Tasks are in the available state
-- if they are newly created or they have been successfully released. If a worker
-- acquires a lease and dies, the task will not be in the available state, and
-- must be fetched using 'expiredLocks', which returns locked tasks with expired
-- leases.
availableLocks :: TaskSpace -> Transaction (Seq TaskName)
availableLocks ts@(TaskSpace ss) = do
  let availableSS = FDB.extend ss [FDB.Bytes available]
  let range = FDB.subspaceRangeQuery availableSS
  res <- FDB.getEntireRange range
  return $ fmap (\(k,_) -> snd (parseAvailableKey ts k)) res

data HowAcquired = RandomExpired | Available
  deriving (Show, Eq, Ord)

-- | Acquire a random available or expired lease.
acquireRandom :: TaskSpace
                      -> (TaskName -> Int)
                      -- ^ How long to lock the task, depending on its name.
                      -> Transaction (Maybe (TaskName, AcquiredLease, HowAcquired))
acquireRandom ts taskSeconds = do
  -- Note: these snapshot reads are safe because 'acquire' below will conflict
  -- if two workers concurrently try to acquire the same lease.
  expireds <- FDB.withSnapshot $ expiredLocks ts
  availables <- FDB.withSnapshot $ availableLocks ts
  let tasks = expireds <> availables
  let n = length tasks
  if n > 0
    then do
      rand <- liftIO $ randomRIO (0, n - 1)
      let taskName = fromMaybe (error "impossible") (tasks Seq.!? rand)
      tv <- acquire ts taskName (taskSeconds taskName)
      let how = if rand < length expireds then RandomExpired else Available
      return $ Just (taskName, tv, how)
    else return Nothing
