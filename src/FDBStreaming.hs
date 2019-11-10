{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BlockArguments #-}

module FDBStreaming (
  existing,
  produce,
  atLeastOnce,
  pipe,
  oneToOneJoin,
  groupBy,
  aggregate,
  benignIO,
  runLeaseStreamWorker
) where

import qualified FDBStreaming.AggrTable        as AT
import           FDBStreaming.Message           ( Message(fromMessage, toMessage) )
import           FDBStreaming.TaskLease        (TaskName(TaskName), secondsSinceEpoch)
import           FDBStreaming.TaskRegistry     as TaskRegistry (empty
                                                               , TaskRegistry
                                                               , taskRegistryLeaseDuration
                                                               , addTask
                                                               , runRandomTask)
import           FDBStreaming.Topic (TopicConfig(numPartitions), PartitionId, ReaderName, makeTopicConfig, readNAndCheckpoint', randPartition, writeTopic', watchPartition)
import qualified FDBStreaming.Topic.Constants  as C
import           FDBStreaming.Joins (get1to1JoinData, delete1to1JoinData, write1to1JoinData)

import           Control.Concurrent ( myThreadId, threadDelay)
import           Control.Concurrent.Async       ( Async
                                                , async
                                                , wait
                                                , waitAny
                                                )
import           Control.Exception (SomeException, catches, catch, Handler(Handler), throw)
import           Control.Monad (forM, forM_, forever, when, void, replicateM)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Control.Monad.State.Strict as State
import           Control.Monad.State.Strict     ( MonadState
                                                , StateT
                                                , gets
                                                , put
                                                )
import           Data.ByteString                ( ByteString )
import qualified Data.ByteString.Char8 as BS8
import           Data.Foldable                  ( toList )

import           Data.Sequence                  ( Seq() )
import qualified Data.Sequence                 as Seq
import           Data.Text.Encoding             ( decodeUtf8 )
import           Data.Witherable                ( catMaybes )
import           FoundationDB                  as FDB ( Transaction, Database, FutureIO, runTransaction, awaitIO, withSnapshot, await)
import           FoundationDB.Error            as FDB (FDBHsError(MaxRetriesExceeded), Error(Error,CError), CError(TransactionTimedOut, NotCommitted))
import qualified FoundationDB.Layer.Subspace   as FDB
import qualified FoundationDB.Layer.Tuple      as FDB
import           Data.Word                      ( Word8 )
import           System.Clock                   ( Clock(Monotonic)
                                                , diffTimeSpec
                                                , getTime
                                                , toNanoSecs
                                                )
import qualified System.Metrics                as Metrics
import           System.Metrics.Counter         ( Counter )
import           System.Metrics.Distribution    ( Distribution )
import qualified System.Metrics.Counter        as Counter
import qualified System.Metrics.Distribution   as Distribution
import           Text.Printf                    (printf)
import           UnliftIO.Exception             ( fromEitherIO )

data StreamEdgeMetrics = StreamEdgeMetrics
  { _messagesProcessed :: Counter
  , emptyReads :: Counter
  , batchLatency :: Distribution
  , messagesPerBatch :: Distribution
  , conflicts :: Counter
  }

registerStepMetrics :: (HasStreamConfig m, MonadIO m)
                    => StreamName -> m (Maybe StreamEdgeMetrics)
registerStepMetrics s = do
  sc <- getStreamConfig
  let sn = decodeUtf8 s
  forM (streamMetricsStore sc) $ \store -> liftIO $ do
    mp <- Metrics.createCounter ("stream." <> sn <> ".messagesProcessed") store
    er <- Metrics.createCounter ("stream." <> sn <> ".emptyReads") store
    bl <- Metrics.createDistribution ("stream." <> sn <> ".batchLatency") store
    mb <- Metrics.createDistribution ("stream." <> sn <> ".msgsPerBatch") store
    cs <- Metrics.createCounter ("stream." <> sn <> ".conflicts") store
    return (StreamEdgeMetrics mp er bl mb cs)

incrEmptyBatchCount :: Maybe StreamEdgeMetrics -> IO ()
incrEmptyBatchCount m = forM_ m $ Counter.inc . emptyReads

recordMsgsPerBatch :: Maybe StreamEdgeMetrics -> Int -> IO ()
recordMsgsPerBatch m n = forM_ m $ \metrics ->
  Distribution.add (messagesPerBatch metrics) (fromIntegral n)

incrConflicts :: Maybe StreamEdgeMetrics -> IO ()
incrConflicts m = forM_ m $ Counter.inc . conflicts

recordBatchLatency :: Integral a => Maybe StreamEdgeMetrics -> a -> IO ()
recordBatchLatency m x = forM_ m $ \metrics ->
  Distribution.add (batchLatency metrics) (fromIntegral x)

{-

High-level streaming combinators -- an experiment.

The streams need to be easy to maintain. How to change a topology over time is
an important consideration. What happens if I put a new step in the middle of
a pipeline? What happens if I remove a step? What happens if I change the logic
of a step?

After all of these modifications, what happens when I start my program again
with the existing DB? What happens if I do a rolling deployment, so some are
still running the old version of the code?

-}

-- TODO: rename to StepName?
type StreamName = ByteString

data GroupedBy k v = GroupedBy (Topic v) (v -> [k])

-- TODO: transient "topic" that isn't persisted, but instead fused into later
-- steps? Might be hard to find a way to do it without doubling all our code
-- (and worse, our MonadStream interface).
data Topic a = forall b . Message b => Topic
  { getTopicConfig :: TopicConfig
  , _topicMapFilter :: b -> IO (Maybe a) }

instance Functor Topic where
  fmap g (Topic c f) = Topic c (fmap (fmap g) . f)

-- | Registers an IO transformation to perform on each message if/when the
-- stream is consumed downstream. Return 'Nothing' to filter the stream. Side
-- effects here should be benign and relatively cheap -- they could be run many
-- times for the same input.
benignIO :: (a -> IO (Maybe b)) -> Topic a -> Topic b
benignIO g (Topic cfg (f :: c -> IO (Maybe a))) =
  Topic cfg $ \(x :: c) -> do
    y <- f x
    case y of
      Nothing -> return Nothing
      Just z -> g z

class Monad m => MonadStream m where
  -- | Read messages from an existing Topic.
  existing :: Message a => TopicConfig -> m (Topic a)
  existing tc = return (Topic tc (return . Just))

  -- TODO: if this handler type took a batch at a time,
  -- it would be easier to optimize -- imagine if it were to
  -- to do a get from FDB for each item -- it could do them all
  -- in parallel.
  -- TODO: produce isn't idempotent in cases of CommitUnknownResult
  produce :: Message a => StreamName -> IO (Maybe a) -> m (Topic a)
  -- TODO: better operation for externally-visible side effects. In practice, if
  -- number of threads per partition is > 1, we will potentially have a lot of
  -- repeated side effects per message. If we're e.g. sending emails or
  -- something similarly externally visible, that's not good. Perhaps we can
  -- introduce a atMostOnceSideEffect type for these sorts of things, that
  -- checkpoints and THEN performs side effects. The problem is if the thread
  -- dies after the checkpoint but before the side effect, or if the side effect
  -- fails. We could maintain a set of in-flight side effects, and remove them
  -- from the set once finished. In that case, we could try to recover by
  -- traversing the items in the set that are older than t.
  -- | Produce a side effect at least once for each message in the stream.
  -- In practice, this will _usually_ be more than once in the current
  -- implementation, if running multiple instances of the processor.
  -- NOTE: if using the new lease-based processor, this will usually just be
  -- once.
  atLeastOnce :: Message a => StreamName -> Topic a -> (a -> IO ()) -> m ()
  pipe :: (Message a, Message b)
       => StreamName
       -> Topic a
       -- TODO: let user take a batch of items at once, and potentially split
       -- one message into multiple downstream messages.
       -- i.e., below type should be (t a -> IO (t b))
       -> (a -> IO (Maybe b))
       -> m (Topic b)
  -- | Streaming one-to-one join. If the relationship is not actually one-to-one
  --   (i.e. the input join functions are not injective), some messages in the
  --   input streams could be lost.
  oneToOneJoin :: (Message a, Message b, Message c, Message d)
               => StreamName
               -> Topic a
               -> Topic b
               -> (a -> c)
               -> (b -> c)
               -> (a -> b -> d)
               -> m (Topic d)
  -- NOTE: the reason that this is a separate constructor from StreamAggregate
  -- is so that our helper functions can be combined more easily. It's easier to
  -- work with and refactor code that looks like @count . groupBy id@ rather
  -- than the less compositional @countBy id@. At least, that's what it looks
  -- like at the time of this writing. Kafka Streams does it that way. If it
  -- ends up not being worth it, simplify.
  groupBy :: (Message v, Message k)
          => (v -> [k])
          -> Topic v
          -> m (GroupedBy k v)
  -- TODO: maybe consolidate TableValue and TableSemigroup
  -- TODO: if we're exporting helpers anyway, maybe no need for classes
  -- at all.
  aggregate :: (Message v, Message k, AT.TableSemigroup aggr)
            => StreamName
            -> GroupedBy k v
            -> (v -> aggr)
            -> m (AT.AggrTable k aggr)

class HasStreamConfig m where
  getStreamConfig :: m FDBStreamConfig

makeTopic :: (Message a, HasStreamConfig m, Monad m)
          => StreamName
          -> m (Topic a)
makeTopic sn = do
  sc <- getStreamConfig
  let tc = makeTopicConfig (streamConfigDB sc) (streamConfigSS sc) sn
  return
    $ Topic tc (return . Just)

forEachPartition :: Monad m => Topic a -> (PartitionId -> m ()) -> m ()
forEachPartition (Topic cfg _) = forM_ [0 .. numPartitions cfg - 1]

-- This mess is caused by the ball of mud that is logErrors. For
-- LeaseBasedStreamWorker, we need to pass through an extra value indicating
-- whether the lease is still valid, so the crazy IO action passed to logErrrors
-- needs to pass Maybe something to accommodate that. We don't need to pass
-- anything else when using StreamWorker, so this combinator adds a dummy value.
-- TODO: refactor logErrors into smaller pieces so we can avoid this mess.
withNothing :: (a,b) -> (a,b, Maybe c)
withNothing (x,y) = (x,y, Nothing)

-- | An implementation of the streaming system that uses a distributed lease to
-- ensure mutual exclusion for each worker process. This reduces DB conflicts
-- and CPU contention, and increases throughput and scalability.
newtype LeaseBasedStreamWorker a =
  LeaseBasedStreamWorker
  { unLeaseBasedStreamWorker :: StateT (FDBStreamConfig, TaskRegistry) IO a}
  deriving (Functor, Applicative, Monad, MonadState (FDBStreamConfig, TaskRegistry), MonadIO)

instance HasStreamConfig LeaseBasedStreamWorker where
  getStreamConfig = gets fst

taskRegistry :: LeaseBasedStreamWorker TaskRegistry
taskRegistry = snd <$> State.get

-- | Repeatedly run a transaction so long as another transaction returns True.
-- Both transactions are run in one larger transaction, to ensure correctness.
-- Unfortunately, the performance overhead of this was too great at the time
-- of this writing, and the pipeline eventually falls behind.
_doWhileValid :: Database
                       -> FDBStreamConfig
                       -> Maybe StreamEdgeMetrics
                       -> StreamName
                       -> Transaction Bool
                       -> Transaction (Int, Async ())
                       -> IO ()
_doWhileValid db streamCfg metrics sn stillValid action = do
  wasValidMaybe <- logErrors streamCfg metrics sn $ runTransaction db $
    stillValid >>= \case
      True -> do
        (msgsProcessed, w) <- action
        return (msgsProcessed, w, Just True)
      False -> do
        tid <- liftIO myThreadId
        liftIO $ putStrLn $ show tid ++ " lease no longer valid."
        f <- liftIO $ async $ return ()
        return (0, f, Just False)
  case wasValidMaybe of
    Just wasValid -> when wasValid
                     $ _doWhileValid db streamCfg metrics sn stillValid action
    -- Nothing means an exception was thrown; try again.
    Nothing -> _doWhileValid db streamCfg metrics sn stillValid action

-- | Like doWhileValid, but doesn't transactionally check that the lease is
-- still good -- just uses the system clock to approximately run as long as we
-- have the lock. Any logic this thing runs needs to be safe even without locks,
-- which is the case for our stream processing steps as of the time of this
-- writing -- the locks are just there to reduce duplicate wasted work from
-- multiple threads working on the same thing.
doForSeconds :: Int -> IO () -> IO ()
doForSeconds n f = do
  startTime <- secondsSinceEpoch
  let go = do currTime <- secondsSinceEpoch
              when (currTime <= startTime + n) (f >> go)
  go


mkTaskName :: StreamName -> PartitionId -> TaskName
mkTaskName sn pid = TaskName $ BS8.pack (show sn ++ "_" ++ show pid)

instance MonadStream LeaseBasedStreamWorker where

  produce sn m = do
    t <- makeTopic sn
    let tc = getTopicConfig t
    (cfg@FDBStreamConfig{msgsPerBatch, streamConfigDB}, taskReg) <- State.get
    metrics <- registerStepMetrics sn
    let job _stillValid _release = doForSeconds (taskRegistryLeaseDuration taskReg)
                                   $ void
                                   $ logErrors cfg metrics sn
                                   $ runTransaction streamConfigDB
                                   $ withNothing
                                   <$> produceStep msgsPerBatch tc m
    taskReg' <- liftIO
                $ runTransaction streamConfigDB
                $ addTask taskReg (TaskName sn) job
    put (cfg, taskReg')
    return t

  atLeastOnce sn inTopic step = do
    cfg@FDBStreamConfig{streamConfigDB} <- getStreamConfig
    leaseDuration <- taskRegistryLeaseDuration <$> taskRegistry
    metrics <- registerStepMetrics sn
    let job pid _stillValid _release = doForSeconds leaseDuration
                                        $ void
                                        $ logErrors cfg metrics sn
                                        $ runTransaction streamConfigDB
                                        $ withNothing
                                        <$> consumeStep cfg inTopic sn step metrics pid
    forEachPartition inTopic $ \pid -> do
      let taskName = mkTaskName sn pid
      taskReg <- taskRegistry
      taskReg' <- liftIO
                  $ runTransaction streamConfigDB
                  $ addTask taskReg taskName (job pid)
      put (cfg, taskReg')

  pipe sn inTopic step = do
    cfg@FDBStreamConfig{streamConfigDB} <- getStreamConfig
    leaseDuration <- taskRegistryLeaseDuration <$> taskRegistry
    outTopic <- makeTopic sn
    metrics <- registerStepMetrics sn
    let outCfg = getTopicConfig outTopic
    let job pid _stillValid _release = doForSeconds leaseDuration
                                        $ void
                                        $ logErrors cfg metrics sn
                                        $ runTransaction streamConfigDB
                                        $ withNothing
                                        <$> pipeStep cfg inTopic outCfg sn step metrics pid
    forEachPartition inTopic $ \pid -> do
      let taskName = mkTaskName sn pid
      taskReg <- taskRegistry
      taskReg' <- liftIO
                  $ runTransaction streamConfigDB
                  $ addTask taskReg taskName (job pid)
      put (cfg, taskReg')
    return outTopic

  oneToOneJoin sn lt rt pl pr c = do
    cfg@FDBStreamConfig{streamConfigDB} <- getStreamConfig
    leaseDuration <- taskRegistryLeaseDuration <$> taskRegistry
    outTopic <- makeTopic sn
    let outCfg = getTopicConfig outTopic
    metrics <- registerStepMetrics sn
    let lname = sn <> "0"
    let rname = sn <> "1"
    let ljob pid _stillValid _release =
          doForSeconds leaseDuration
          $ void
          $ logErrors cfg metrics sn
          $ runTransaction streamConfigDB
          $ withNothing
          <$> oneToOneJoinStep cfg sn lname lt 0 outCfg pl c metrics pid
    let rjob pid _stillValid _release =
          doForSeconds leaseDuration
          $ void
          $ logErrors cfg metrics sn
          $ runTransaction streamConfigDB
          $ withNothing
          <$> oneToOneJoinStep cfg sn rname rt 1 outCfg pr (flip c) metrics pid
    forEachPartition lt $ \pid -> do
      let lTaskName = TaskName $ BS8.pack (show lname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      taskReg' <- liftIO
                  $ runTransaction streamConfigDB
                  $ addTask taskReg lTaskName (ljob pid)
      put (cfg, taskReg')
    forEachPartition rt $ \pid -> do
      let rTaskName = TaskName $ BS8.pack (show rname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      taskReg' <- liftIO
                   $ runTransaction streamConfigDB
                   $ addTask taskReg rTaskName (rjob pid)
      put (cfg, taskReg')
    return outTopic

  groupBy k t = return (GroupedBy t k)

  aggregate sn groupedBy@(GroupedBy inTopic _) toAggr = do
    cfg@FDBStreamConfig{streamConfigDB} <- getStreamConfig
    leaseDuration <- taskRegistryLeaseDuration <$> taskRegistry
    let table = getAggrTable cfg sn
    metrics <- registerStepMetrics sn
    let job pid _stillValid _release = doForSeconds leaseDuration
                                       $ void
                                       $ logErrors cfg metrics sn
                                       $ runTransaction streamConfigDB
                                       $ withNothing
                                       <$> aggregateStep cfg sn groupedBy toAggr metrics pid
    forEachPartition inTopic $ \pid -> do
      taskReg <- taskRegistry
      let taskName = TaskName $ BS8.pack (show sn ++ "_" ++ show pid)
      taskReg' <- liftIO
                  $ runTransaction streamConfigDB
                  $ addTask taskReg taskName (job pid)
      put (cfg, taskReg')
    return table

-- TODO: what if we have recently removed steps from our topology? Old leases
-- will be registered forever. Need to remove old ones.
registerAllLeases :: FDBStreamConfig -> LeaseBasedStreamWorker a -> IO (a, TaskRegistry)
registerAllLeases cfg =
  fmap (fmap snd)
  . flip State.runStateT (cfg, TaskRegistry.empty (taskRegSS cfg) (leaseDuration cfg))
  . unLeaseBasedStreamWorker

runLeaseStreamWorker :: Int -> FDBStreamConfig -> LeaseBasedStreamWorker a -> IO ()
runLeaseStreamWorker numThreads cfg topology = do
  -- TODO: need a way for user to get the pure result of their topology. Need
  -- a pure runner or something.
  (_pureResult, taskReg) <- registerAllLeases cfg topology
  threads <- replicateM numThreads $ async $ forever $
    runRandomTask (streamConfigDB cfg) taskReg >>= \case
      False -> threadDelay (taskRegistryLeaseDuration taskReg * 1000000 `div` 2)
      True -> return ()
  _ <- waitAny threads
  return ()

-- TODO: need to think about grouping and windowing. Windowing appears to be a
-- special case of grouping, where the groupBy function is a function of time.
-- But is a sliding window a special case of groupBy? Seems like it might not be
-- since consecutive messages can be assigned to multiple groups.
-- Come to think of it, does groupBy need to assign each item to only one group?
-- Could it ever make sense to assign a single object to a set of groups?
-- What if the set of groups is rather large (such as all sliding windows the
-- object falls within)?

-- It looks like kafka streams only allows sliding windows for joins, which
-- would certainly simplify matters.

-- For groupBy, I think we could introduce a type GroupedBy a b, representing
-- things of type b grouped by type a. Introduction would be by
-- groupBy :: (b -> a) -> Stream b -> Stream (GroupedBy a b)
-- elimination would be by
-- aggregateBy :: Monoid m => (b -> m) -> Stream (GroupedBy a b) -> Stream (a, m)
-- or something similar. Problem being that we can't produce an (a,m) until we
-- know we have seen everything with a particular a key. So perhaps it should be
-- persisted to a state m associated with each Stream -- Stream m a b?
-- Or perhaps don't parametrize, and instead require that the Stream state be a
-- k/v store (i.e. table), and give the user an interface into that?

-- Note: we don't need a separate type for nested groupby -- we can simply group
-- by a tuple. No need to support GroupedBy a (GroupedBy b c).

-- TODO: think about joins.
-- I believe we can build joins out of FDB by listening to both upstream topics,
-- writing a k/v when we receive one half of the join tuple, check if the tuple
-- has been completed (i.e., the half we just received is the last of the two
-- halves), and then emit a downstream message of the tuple.
-- tentative type:
-- joinOn :: (a -> e) -> (b -> e) -> Stream a -> Stream b -> Stream (a,b)
-- possible user errors/gotchas:
-- 1. non-unique join values
-- 2. must be the case for all x,y :: JoinValue that
--    (toMessage x == toMessage y) iff x == y. That is, they will be joined by
--    the equality of the serializations of the values, not the Haskell values
--    themselves.
-- stuff to deal with on our side:
-- 1. How do we garbage collect halves that never get paired up?
--    The intermediate data must necessarily be keyed by the projected value,
--    which is itself not ordered by anything useful.

-- TODO: what about truncating old data by timestamp?

-- | reads a batch of messages from a stream and checkpoints so that the same
-- value of 'ReaderName' is guaranteed to never receive the same messages again
-- in subsequent calls to this function.
readPartitionBatchExactlyOnce
  :: Topic a
  -> Maybe StreamEdgeMetrics
  -> ReaderName
  -> PartitionId
  -> Word8
  -> Transaction (Seq a)
readPartitionBatchExactlyOnce (Topic outCfg mapFilter) metrics rn pid n = do
  rawMsgs <- readNAndCheckpoint' outCfg pid rn n
  liftIO $ when (Seq.null rawMsgs) (incrEmptyBatchCount metrics)
  liftIO $ recordMsgsPerBatch metrics (Seq.length rawMsgs)
  let msgs = fmap (fromMessage . snd) rawMsgs
  msgs' <- liftIO $ forM msgs mapFilter
  return $ catMaybes msgs'

getAggrTable :: FDBStreamConfig -> StreamName -> AT.AggrTable k v
getAggrTable sc sn = AT.AggrTable
  $ FDB.extend (streamConfigSS sc) [C.topics, FDB.Bytes sn, C.aggrTable]

-- TODO: other persistence backends
-- TODO: should probably rename to TopologyConfig
data FDBStreamConfig = FDBStreamConfig
  { streamConfigDB :: FDB.Database
  , streamConfigSS :: FDB.Subspace
  -- ^ subspace that will contain all state for the stream topology
  , streamMetricsStore :: Maybe Metrics.Store
  , useWatches :: Bool
  -- ^ If true, use FDB watches to wait for new messages in each worker thread.
  -- Otherwise, read continuously, sleeping for a short time if no new messages
  -- are available. In exeperiments so far, it seems that setting this to false
  -- significantly reduces the total load on FDB, increases throughput,
  -- and reduces end-to-end latency (surprisingly).
  , msgsPerBatch :: Word8
  -- ^ Number of messages to process per transaction per thread per partition
  , leaseDuration :: Int
  -- ^ Length of time an individual worker should work on a single stage of the
  -- pipeline before stopping and trying to work on something else. Higher
  -- values are more efficient in normal operation, but if enough machines fail,
  -- higher values can be a worst-case lower bound on end-to-end latency.
  -- Only applies to pipelines run with the LeaseBasedStreamWorker monad.
  }

taskRegSS :: FDBStreamConfig -> FDB.Subspace
taskRegSS cfg = FDB.extend (streamConfigSS cfg) [FDB.Bytes "leases"]

waitLogging :: Async () -> IO ()
waitLogging w = catches (wait w)
  [ Handler (\(e :: SomeException) ->
      printf "Caught %s while watching a topic partition"
             (show e))]

-- | The core loop body for every stream job. Throttles the job based on any
-- errors that occur, records timing metrics.
logErrors
  :: FDBStreamConfig
  -> Maybe StreamEdgeMetrics
  -> StreamName
  -> IO (Int, Async (), Maybe a)
  -> IO (Maybe a)
logErrors FDBStreamConfig{ useWatches } metrics sn x =
  flip catches
    [ Handler
      (\case
       Error (MaxRetriesExceeded (CError TransactionTimedOut)) ->
        threadDelay 15000 >> return Nothing
       CError TransactionTimedOut ->
        threadDelay 15000 >> return Nothing
       e -> throw e
      )
    , Handler
      (\case
        Error (MaxRetriesExceeded (CError NotCommitted)) -> do
          incrConflicts metrics
          threadDelay 15000
          return Nothing
        e -> throw e
      )

    , Handler
        (\(e :: SomeException) -> do
          tid <- myThreadId
          printf "%s on thread %s caught %s\n" (show sn) (show tid) (show e)
          return Nothing
        )
    ]
    $ do
    -- Problem: busy looping, constantly reading for more data, is wasteful.
    -- Possible solution: create a watch after each iteration, and only loop
    -- again once the watch is delivered. Subsequent problem: we need to pass
    -- the partition id of the partition with new messages into the iteration
    -- body. However, if we do that, we could conceivably cause all the reader
    -- threads to synchronize, with all of them waking up on each write and
    -- contending the same partition, which breaks the reader scalability we
    -- created with the partitions.
    -- On the other hand, we can't watch and then read a random partition,
    -- because then we might read the wrong one and the message could be delayed
    -- in a low-write situation.
    -- On the third or fourth hand, perhaps the readers would only get synched
    -- in low-write situations where contention wouldn't matter, anyway -- if
    -- tons of messages are coming in, presumably threads will be spending more
    -- time working than waiting, and they won't be woken up together for
    -- exactly the same write.
    -- NOTE: in practice, watches weren't notifying readers fast enough,
    -- and the pipeline fell behind.
    -- w <- x
    -- awaitTopicOrTimeout 500 w
        --NOTE: a small delay here (<10 milliseconds) helps us do more
        -- msgs/second
        threadDelay 150
        t1 <- getTime Monotonic
        (numProcessed, w, result) <- x `catch`
          \(e :: FDB.Error) -> case e of
            Error (MaxRetriesExceeded (CError TransactionTimedOut)) -> do
              t2 <- getTime Monotonic
              let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
              printf "%s timed out after %d ms, assuming we processed no messages.\n"
                     (show sn)
                     timeMillis
              f <- async $ return ()
              return (0, f, Nothing)
            _ -> throw e
        t2 <- getTime Monotonic
        let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
        recordBatchLatency metrics timeMillis
        if numProcessed == 0 && not useWatches
           then threadDelay 1000000
           else waitLogging w
        return result

mfutureToAsync :: Maybe (FutureIO ()) -> IO (Async ())
mfutureToAsync Nothing = async $ return ()
mfutureToAsync (Just f) = async $ fromEitherIO $ awaitIO f

produceStep :: Message a
            => Word8
            -> TopicConfig
            -> IO (Maybe a)
            -> FDB.Transaction (Int, Async ())
produceStep batchSize outCfg step = do
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  xs <- liftIO $ catMaybes <$> replicateM (fromIntegral batchSize) step
  p' <- liftIO $ randPartition outCfg
  writeTopic' outCfg p' (fmap toMessage xs)
  w <- liftIO $ async $ return ()
  return (length xs, w)

consumeStep :: FDBStreamConfig
            -> Topic a
            -> StreamName
            -> (a -> IO ())
            -> Maybe StreamEdgeMetrics
            -> PartitionId
            -> FDB.Transaction (Int, Async ())
consumeStep FDBStreamConfig{ useWatches, msgsPerBatch }
            t@(Topic inCfg _) sn step metrics pid = do
    xs <- readPartitionBatchExactlyOnce t metrics sn pid msgsPerBatch

    liftIO $ mapM_ step xs
    if null xs || not useWatches
      then do
        w <- liftIO $ async $ return ()
        return (length xs, w)
      else do
        w <- watchPartition inCfg pid
        w' <- liftIO $ mfutureToAsync $ Just w
        return (length xs, w')

pipeStep :: Message b
         => FDBStreamConfig
         -> Topic a
         -> TopicConfig
         -> StreamName
         -> (a -> IO (Maybe b))
         -> Maybe StreamEdgeMetrics
         -> PartitionId
         -> Transaction (Int, Async ())
pipeStep  FDBStreamConfig { useWatches, msgsPerBatch }
          inTopic@(Topic inCfg _)
          outCfg
          sn
          step
          metrics
          pid = do
  inMsgs <- readPartitionBatchExactlyOnce inTopic
                                          metrics
                                          sn
                                          pid
                                          msgsPerBatch
  ys <- catMaybes . toList <$> liftIO (mapM step inMsgs)
  let outMsgs = fmap toMessage ys
  p' <- liftIO $ randPartition outCfg
  writeTopic' outCfg p' outMsgs
  -- TODO: merge below into one fn
  w <- if null inMsgs || not useWatches
          then return Nothing
          else Just <$> watchPartition inCfg pid
  w' <- liftIO $ mfutureToAsync w
  return (length inMsgs, w')

oneToOneJoinStep :: forall a b c d . (Message a, Message b, Message c, Message d)
                 => FDBStreamConfig
                 -> StreamName
                 -> ByteString
                 -- ^ unique name for checkpointing this operation. Can't
                 -- be the same as the StreamName because self-joins would
                 -- break.
                 -> Topic a
                 -> Int
                 -- ^ Index of the stream being consumed. 0 for left side of
                 -- join, 1 for right. This will be refactored to support
                 -- n-way joins in the future.
                 -> TopicConfig
                 -> (a -> c)
                 -> (a -> b -> d)
                 -> Maybe StreamEdgeMetrics
                 -> PartitionId
                 -> Transaction (Int, Async ())
oneToOneJoinStep FDBStreamConfig{ streamConfigSS
                                , useWatches
                                , msgsPerBatch}
                 sn
                 checkpointName
                 lInTopic@(Topic lCfg _)
                 streamJoinIx
                 outCfg
                 pl
                 combiner
                 metrics
                 pid = do
  -- Read from one of the two join streams, and for each
  -- message read, compute the join key. Using the join key, look in the
  -- join table to see if the partner is already there. If so, write the tuple
  -- downstream. If not, write the one message we do have to the join table.
  -- TODO: think of a way to garbage collect items that never get joined.
    lMsgs <- readPartitionBatchExactlyOnce lInTopic
                                           metrics
                                           checkpointName
                                           pid
                                           msgsPerBatch
    let otherIx = if streamJoinIx == 0 then 1 else 0
    joinFutures <- forM (fmap pl lMsgs)
                        (withSnapshot . get1to1JoinData streamConfigSS sn otherIx)
    joinData <- Seq.zip lMsgs <$> mapM await joinFutures
    toWrite <- fmap (catMaybes . toList) $ forM joinData $ \(lmsg, d) -> do
      let k = pl lmsg
      case d of
        Just (rmsg :: b) -> do
          delete1to1JoinData streamConfigSS sn k
          return $ Just $ combiner lmsg rmsg
        Nothing -> do
          write1to1JoinData streamConfigSS sn k streamJoinIx (lmsg :: a)
          return Nothing
    p' <- liftIO $ randPartition outCfg
    writeTopic' outCfg p' (map toMessage toWrite)
    w <- if null lMsgs || not useWatches
            then return Nothing
            else Just <$> watchPartition lCfg pid
    w' <- liftIO $ mfutureToAsync w
    return (length lMsgs, w')

aggregateStep :: forall v k aggr . (Message k, AT.TableSemigroup aggr)
              => FDBStreamConfig
              -> StreamName
              -> GroupedBy k v
              -> (v -> aggr)
              -> Maybe StreamEdgeMetrics
              -> PartitionId
              -> FDB.Transaction (Int, Async ())
aggregateStep c@FDBStreamConfig { useWatches, msgsPerBatch }
              sn
              (GroupedBy inTopic@(Topic inCfg _) toKeys)
              toAggr
              metrics
              pid = do
  let table = getAggrTable c sn
  msgs <- readPartitionBatchExactlyOnce inTopic
                                        metrics
                                        sn
                                        pid
                                        msgsPerBatch
  forM_ msgs $ \msg -> forM_ (toKeys msg) $ \k ->
    AT.mappendTable table k (toAggr msg)
  w <- if null msgs || not useWatches
          then return Nothing
          -- TODO: wrap watchPartition so we don't have to destructure Topic
          else Just <$> watchPartition inCfg pid
  w' <- liftIO $ mfutureToAsync w
  return (length msgs, w')
