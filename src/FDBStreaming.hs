{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming where

import qualified FDBStreaming.AggrTable        as AT
import           FDBStreaming.Message           ( Message(..) )
import           FDBStreaming.Topic
import qualified FDBStreaming.Topic.Constants  as C

import           Control.Concurrent
import           Control.Concurrent.Async       ( Async
                                                , async
                                                , wait
                                                , waitEither
                                                )
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Reader           ( MonadReader
                                                , ReaderT
                                                , ask
                                                , runReaderT
                                                )
import           Data.ByteString                ( ByteString )
import           Data.Foldable                  ( toList )

import           Data.Maybe                     ( catMaybes )
import           Data.Sequence                  ( Seq(..) )
import qualified Data.Sequence                 as Seq
import           Data.Text.Encoding             ( decodeUtf8 )
import           FoundationDB                  as FDB
import           FoundationDB.Error            as FDB
import           FoundationDB.Layer.Subspace   as FDB
import           FoundationDB.Layer.Tuple      as FDB
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
import           Text.Printf
import           UnliftIO.Exception             ( fromEitherIO )


-- | We limit the number of retries so we can catch conflict errors and record
-- them in the EKG stats. Doing so is helpful for learning how to optimize
-- throughput and latency.
lowRetries :: TransactionConfig
lowRetries = FDB.defaultConfig { maxRetries = 3, timeout = 1000 }

data StreamEdgeMetrics = StreamEdgeMetrics
  { messagesProcessed :: Counter
  , emptyReads :: Counter
  , batchLatency :: Distribution
  , messagesPerBatch :: Distribution
  , conflicts :: Counter
  }

registerStepMetrics :: (MonadReader FDBStreamConfig m, MonadIO m)
                    => StreamName -> m (Maybe StreamEdgeMetrics)
registerStepMetrics s = do
  sc <- ask
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
newtype Topic a = Topic { getTopicConfig :: TopicConfig }

-- Let's see if tagless final solves our constraint problems
class Monad m => MonadStream m where
  existing :: Message a => TopicConfig -> m (Topic a)

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
  consume :: Message a => StreamName -> Topic a -> (a -> IO ()) -> m ()
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
  aggregate :: (Message v, Message k, AT.TableValue aggr, AT.TableSemigroup aggr)
            => StreamName
            -> GroupedBy k v
            -> (v -> aggr)
            -> m (AT.AggrTable k aggr)

-- TODO: better name
newtype StreamWorker a = StreamWorker { unStreamWorker :: ReaderT FDBStreamConfig IO a}
  deriving (Functor, Applicative, Monad, MonadReader FDBStreamConfig, MonadIO)

makeTopic :: (MonadReader FDBStreamConfig m)
          => StreamName
          -> m (Topic a)
makeTopic sn = do
  sc <- ask
  return
    $ Topic
    $ makeTopicConfig (streamConfigDB sc) (streamConfigSS sc) sn

runPartitionedForever :: (MonadReader FDBStreamConfig m, MonadIO m)
                      => StreamName
                      -> Topic a
                      -> (Maybe StreamEdgeMetrics -> PartitionId -> IO (Int, Async ()))
                      -> m ()
runPartitionedForever sn (Topic cfg) run = do
  scfg <- ask
  metrics <- registerStepMetrics sn
  liftIO $ putStrLn $ "starting " ++ show sn
  liftIO $ forM_ [0 .. (numPartitions cfg) - 1] $ \pid ->
    replicateM_ (threadsPerEdge scfg)
      $ void
      $ forkIO
      $ foreverLogErrors scfg metrics sn
      $ run metrics pid

instance MonadStream StreamWorker where
  existing tc = return (Topic tc)

  produce sn m = do
    t <- makeTopic sn
    FDBStreamConfig{msgsPerBatch} <- ask
    runPartitionedForever sn t (\_ _ -> runProduceStep msgsPerBatch t m)
    return t

  consume sn inTopic step = do
    cfg <- ask
    runPartitionedForever sn inTopic (runConsumeStep cfg inTopic sn step)

  pipe sn inTopic step = do
    cfg <- ask
    outTopic <- makeTopic sn
    runPartitionedForever sn inTopic (runPipeStep cfg inTopic outTopic sn step)
    return outTopic

  oneToOneJoin sn lt rt pl pr c = do
    cfg <- ask
    outTopic <- makeTopic sn
    -- TODO: currently oneToOneJoin assumes both input topics have same number
    -- of partitions. To fix, call runPartitionedForever twice, once with a
    -- function for the left topic, once for a function for the right topic.
    runPartitionedForever sn lt (runOneToOneJoinStep cfg sn lt rt outTopic pl pr c)
    return outTopic

  groupBy k t = return (GroupedBy t k)

  aggregate sn groupedBy@(GroupedBy inTopic _) toAggr = do
    cfg <- ask
    let table = getAggrTable cfg sn
    runPartitionedForever sn inTopic (runAggregateStep cfg sn groupedBy toAggr)
    return table

runStreamWorker :: FDBStreamConfig -> StreamWorker a -> IO a
runStreamWorker cfg = flip runReaderT cfg . unStreamWorker

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
  :: Message a
  => Topic a
  -> Maybe StreamEdgeMetrics
  -> ReaderName
  -> PartitionId
  -> Word8
  -> Transaction (Seq a)
readPartitionBatchExactlyOnce (Topic outCfg) metrics rn pid n = do
  rawMsgs <- readNAndCheckpoint' outCfg pid rn n
  liftIO $ when (Seq.null rawMsgs) (incrEmptyBatchCount metrics)
  liftIO $ recordMsgsPerBatch metrics (Seq.length rawMsgs)
  return $ fmap (fromMessage . snd) rawMsgs

getAggrTable :: FDBStreamConfig -> StreamName -> AT.AggrTable k v
getAggrTable sc sn = AT.AggrTable
  $ extend (streamConfigSS sc) [C.topics, Bytes sn, C.aggrTable]

-- TODO: move join stuff to new namespace
subspace1to1JoinForKey
  :: Message k => FDBStreamConfig -> StreamName -> k -> Subspace
subspace1to1JoinForKey sc sn k = extend
  (streamConfigSS sc)
  [C.topics, Bytes sn, C.oneToOneJoin, Bytes (toMessage k)]

delete1to1JoinData
  :: Message k => FDBStreamConfig -> StreamName -> k -> Transaction ()
delete1to1JoinData c sn k = do
  let ss     = subspace1to1JoinForKey c sn k
  let (x, y) = rangeKeys $ subspaceRange ss
  clearRange x y

write1to1JoinData
  :: (Message k, Message a, Message b)
  => FDBStreamConfig
  -> StreamName
  -> k
  -> Either a b
  -> Transaction ()
write1to1JoinData c sn k x = do
  let ss = subspace1to1JoinForKey c sn k
  case x of
    Left  y -> set (pack ss [Bool True]) (toMessage y)
    Right y -> set (pack ss [Bool False]) (toMessage y)

get1to1JoinData
  :: (Message k, Message a)
  => FDBStreamConfig
  -> StreamName
  -> Bool
                    -- ^ True for the left stream, False for the right
                    -- TODO: replace this with an int and support n-way
                    -- joins?
  -> k
  -> Transaction (Future (Maybe a))
get1to1JoinData cfg sn isLeft k = do
  let ss = subspace1to1JoinForKey cfg sn k
  let key = pack ss [Bool isLeft]
  --liftIO $ putStrLn $ "get1to1JoinData: k is " ++ show key
  f <- get key
  return (fmap (fmap fromMessage) f)

-- TODO: other persistence backends
-- TODO: should probably rename to TopologyConfig
data FDBStreamConfig = FDBStreamConfig
  { streamConfigDB :: FDB.Database
  , streamConfigSS :: FDB.Subspace
  -- ^ subspace that will contain all state for the stream topology
  , streamMetricsStore :: Maybe Metrics.Store
  , threadsPerEdge :: Int
  , useWatches :: Bool
  -- ^ If true, use FDB watches to wait for new messages in each worker thread.
  -- Otherwise, read continuously, sleeping for a short time if no new messages
  -- are available. In exeperiments so far, it seems that setting this to false
  -- significantly reduces the total load on FDB, increases throughput,
  -- and reduces end-to-end latency (surprisingly).
  , msgsPerBatch :: Word8
  -- ^ Number of messages to process per transaction per thread per partition
  }

waitLogging :: Async () -> IO ()
waitLogging w = catches (wait w)
  [ Handler (\(e :: SomeException) ->
      printf "Caught %s while watching a topic partition"
             (show e))]

foreverLogErrors
  :: FDBStreamConfig
  -> Maybe StreamEdgeMetrics
  -> StreamName
  -> IO (Int, Async ())
  -> IO ()
foreverLogErrors FDBStreamConfig{ useWatches } metrics sn x =
  forever
    $ handle
        (\(e :: SomeException) -> do
          tid <- myThreadId
          printf "%s on thread %s caught %s\n" (show sn) (show tid) (show e)
        )
    $ handle
        (\case
          Error (MaxRetriesExceeded (CError NotCommitted)) -> do
            incrConflicts metrics
            threadDelay 25000
          e -> throw e
        )
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
    -- w <- x
    -- awaitTopicOrTimeout 500 w
        --NOTE: a small delay here (<10 milliseconds) helps us do more
        -- msgs/second
        threadDelay 150
        t1 <- getTime Monotonic
        (numProcessed, w) <- x `catch`
          \(e :: FDB.Error) -> case e of
            Error (MaxRetriesExceeded (CError TransactionTimedOut)) -> do
              t2 <- getTime Monotonic
              let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
              printf "%s timed out after %d ms, assuming we processed no messages.\n"
                     (show sn)
                     timeMillis
              f <- async $ return ()
              return (0, f)
            _ -> throw e
        t2 <- getTime Monotonic
        let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
        recordBatchLatency metrics timeMillis
        if numProcessed == 0 && not useWatches
           then threadDelay 1000000
           else waitLogging w

mfutureToAsync :: Maybe (FutureIO ()) -> IO (Async ())
mfutureToAsync Nothing = async $ return ()
mfutureToAsync (Just f) = async $ fromEitherIO $ awaitIO f

runProduceStep :: Message a
               => Word8
               -> Topic a
               -> (IO (Maybe a))
               -> IO (Int, Async ())
runProduceStep batchSize (Topic outCfg) step = do
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  xs <- catMaybes <$> replicateM (fromIntegral batchSize) step
  writeTopic outCfg (fmap toMessage xs)
  w <- async $ return ()
  return (length xs, w)

runConsumeStep :: Message a
               => FDBStreamConfig
               -> Topic a
               -> StreamName
               -> (a -> IO ())
               -> Maybe StreamEdgeMetrics
               -> PartitionId
               -> IO (Int, Async ())
runConsumeStep FDBStreamConfig{ streamConfigDB, useWatches, msgsPerBatch } t@(Topic inCfg) sn step metrics pid = do
  (xs, w) <- runTransactionWithConfig lowRetries streamConfigDB $ do
    xs <- readPartitionBatchExactlyOnce t metrics sn pid msgsPerBatch
    if null xs || not useWatches
      then return (xs, Nothing)
      else (xs,) . Just <$> watchPartition inCfg pid
  mapM_ step xs
  w' <- mfutureToAsync w
  return (length xs, w')

runPipeStep :: (Message a, Message b)
            => FDBStreamConfig
            -> Topic a
            -> Topic b
            -> StreamName
            -> (a -> IO (Maybe b))
            -> Maybe StreamEdgeMetrics
            -> PartitionId
            -> IO (Int, Async ())
runPipeStep  FDBStreamConfig { streamConfigDB, useWatches, msgsPerBatch }
             inTopic@(Topic inCfg)
             (Topic outCfg)
             sn
             step
             metrics
             pid =
  runTransactionWithConfig lowRetries streamConfigDB $ do
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

runOneToOneJoinStep :: forall a b c d . (Message a, Message b, Message c, Message d)
                    => FDBStreamConfig
                    -> StreamName
                    -> Topic a
                    -> Topic b
                    -> Topic d
                    -> (a -> c)
                    -> (b -> c)
                    -> (a -> b -> d)
                    -> Maybe StreamEdgeMetrics
                    -> PartitionId
                    -> IO (Int, Async ())
runOneToOneJoinStep c@FDBStreamConfig{ streamConfigDB, useWatches, msgsPerBatch}
                    sn
                    lInTopic@(Topic lCfg)
                    rInTopic@(Topic rCfg)
                    (Topic outCfg)
                    pl
                    pr
                    combiner
                    metrics
                    pid = do
  -- TODO: the below two transactions are virtually identical. Need to find a
  -- way to eliminate the repetition.
  -- What this does is read from one of the two join streams, and for each
  -- message read, compute the join key. Using the join key, look in the
  -- join table to see if the partner is already there. If so, write the tuple
  -- downstream. If not, write the one message we do have to the join table.
  -- TODO: think of a way to garbage collect items that never get joined.
  l <- async $ FDB.runTransactionWithConfig lowRetries streamConfigDB $ do
    lMsgs <- readPartitionBatchExactlyOnce lInTopic
                                           metrics
                                           sn
                                           pid
                                           msgsPerBatch
    joinFutures <- forM (fmap pl lMsgs)
                        (withSnapshot . get1to1JoinData c sn False)
    joinData <- Seq.zip lMsgs <$> mapM await joinFutures
    toWrite <- fmap (catMaybes . toList) $ forM joinData $ \(lmsg, d) -> do
      let k = pl lmsg
      case d of
        Just (rmsg :: b) -> do
          delete1to1JoinData c sn k
          return $ Just $ combiner lmsg rmsg
        Nothing -> do
          write1to1JoinData c sn k (Left lmsg :: Either a b)
          return Nothing
    p' <- liftIO $ randPartition outCfg
    writeTopic' outCfg p' (map toMessage toWrite)
    w <- if null lMsgs || not useWatches
            then return Nothing
            else Just <$> watchPartition lCfg pid
    return (length lMsgs, w)
  (rlen, rw) <- FDB.runTransactionWithConfig lowRetries streamConfigDB $ do
    rMsgs    <- readPartitionBatchExactlyOnce rInTopic
                                              metrics
                                              sn
                                              pid
                                              msgsPerBatch
    joinFutures <- forM (fmap pr rMsgs)
                        (withSnapshot . get1to1JoinData c sn True)
    joinData <- Seq.zip rMsgs <$> mapM await joinFutures
    toWrite <- fmap (catMaybes . toList) $ forM joinData $ \(rmsg, d) -> do
      let k = pr rmsg
      case d of
        Just (lmsg :: a) -> do
          delete1to1JoinData c sn k
          return $ Just $ combiner lmsg rmsg
        Nothing -> do
          write1to1JoinData c sn k (Right rmsg :: Either a b)
          return Nothing
    -- TODO: I think we could dry out the above by using a utility function to
    -- return the list of stuff to write and then call swap before writing.
    p' <- liftIO $ randPartition outCfg
    writeTopic' outCfg p' (map toMessage toWrite)
    w <- if null rMsgs || not useWatches
            then return Nothing
            else Just <$> watchPartition rCfg pid
    return (length rMsgs, w)
  (llen, lw) <- wait l
  lw' <- mfutureToAsync lw
  rw' <- mfutureToAsync rw
  w' <- async $ waitEither lw' rw'
  return (llen + rlen, void w')

runAggregateStep :: forall v k aggr . (Message v, Message k, AT.TableValue aggr, AT.TableSemigroup aggr)
                 => FDBStreamConfig
                 -> StreamName
                 -> GroupedBy k v
                 -> (v -> aggr)
                 -> Maybe StreamEdgeMetrics
                 -> PartitionId
                 -> IO (Int, Async ())
runAggregateStep c@FDBStreamConfig { streamConfigDB, useWatches, msgsPerBatch }
                 sn
                 (GroupedBy inTopic@(Topic inCfg) toKeys)
                 toAggr
                 metrics
                 pid = do
  let table = getAggrTable c sn
  FDB.runTransactionWithConfig lowRetries streamConfigDB $ do
    msgs <- readPartitionBatchExactlyOnce inTopic
                                          metrics
                                          sn
                                          pid
                                          msgsPerBatch
    forM_ msgs $ \msg -> forM_ (toKeys msg) $ \k ->
      AT.mappendTable table k (toAggr msg)
    w <- if null msgs || not useWatches
            then return Nothing
            else Just <$> watchPartition inCfg pid
    w' <- liftIO $ mfutureToAsync w
    return (length msgs, w')
