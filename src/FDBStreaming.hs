{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstrainedClassMethods #-}

module FDBStreaming
  ( MonadStream,
    Stream(getTopicConfig),
    streamWatermark,
    FDBStreamConfig(..),
    existing,
    produce,
    atLeastOnce,
    pipe,
    oneToOneJoin,
    groupBy,
    aggregate,
    getAggrTable,
    benignIO,
    runLeaseStreamWorker,
    -- * watermarks and triggers
    run,
    triggerBy,
    watermarkBy,
    -- * Advanced Usage
    StreamStep(..),
    getWatermark, --TODO: don't export?
  )
where

import Control.Concurrent (myThreadId, threadDelay)
import Control.Concurrent.Async (async, waitAny)
import Control.Exception
  ( Handler (Handler),
    SomeException,
    catch,
    catches,
    throw,
  )
import Control.Monad (forM, forM_, forever, replicateM, void, when)
import Control.Monad.Identity (Identity, runIdentity)
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Control.Monad.State.Strict as State
import Control.Monad.State.Strict (MonadState, StateT, gets)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import Data.Foldable (toList, for_)
import Data.Sequence (Seq ())
import qualified Data.Sequence as Seq
import Data.Text.Encoding (decodeUtf8)
import Data.Traversable (for)
import Data.Void (Void)
import Data.Witherable (catMaybes, mapMaybe, Filterable)
import Data.Word (Word8)
import qualified FDBStreaming.AggrTable as AT
import FDBStreaming.Joins
  ( delete1to1JoinData,
    get1to1JoinData,
    write1to1JoinData,
  )
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.TaskLease (TaskName (TaskName), secondsSinceEpoch)
import FDBStreaming.TaskRegistry as TaskRegistry
  ( TaskRegistry,
    addTask,
    empty,
    runRandomTask,
  )
import FDBStreaming.Topic
  ( PartitionId,
    ReaderName,
    TopicConfig (numPartitions, topicCustomMetadataSS, topicConfigDB, topicSS),
    getCheckpoints,
    makeTopicConfig,
    randPartition,
    readNAndCheckpoint',
    writeTopic',
  )
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkSS,
    getCurrentWatermark,
    getWatermark,
    minWatermark,
    setWatermark
  )
import FoundationDB as FDB
  ( Database,
    Transaction,
    await,
    runTransaction,
    withSnapshot,
  )
import FoundationDB.Error as FDB
  ( CError (NotCommitted, TransactionTimedOut),
    Error (CError, Error),
    FDBHsError (MaxRetriesExceeded),
  )
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import FoundationDB.Versionstamp
  ( Versionstamp(CompleteVersionstamp),
    VersionstampCompleteness(Complete),
    TransactionVersionstamp(TransactionVersionstamp)
  )
import Safe.Foldable (minimumDef, minimumMay)
import System.Clock (Clock (Monotonic), diffTimeSpec, getTime, toNanoSecs)
import qualified System.Metrics as Metrics
import System.Metrics.Counter (Counter)
import qualified System.Metrics.Counter as Counter
import System.Metrics.Distribution (Distribution)
import qualified System.Metrics.Distribution as Distribution
import Text.Printf (printf)

data StreamEdgeMetrics
  = StreamEdgeMetrics
      { _messagesProcessed :: Counter,
        emptyReads :: Counter,
        batchLatency :: Distribution,
        messagesPerBatch :: Distribution,
        conflicts :: Counter
      }

registerStepMetrics ::
  (HasStreamConfig m, MonadIO m) =>
  StepName ->
  m (Maybe StreamEdgeMetrics)
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

type StepName = ByteString

data GroupedBy k v = GroupedBy (Stream v) (v -> [k])

-- TODO: find a way to make this a sum type. We want a UnitStream that just
-- returns () so we can use it as the fake upstream of the produce operator, so
-- that we can use the standard StreamProcessor for produce, instead of a separate
-- type.
-- The difficulty is that we promise StreamProcessor that we will feed it
-- versionstamps, too, which we don't want to do for produce -- produce should
-- be write-only for efficiency. Maybe we should have another stream processing
-- type after all.
data Stream a
  = forall b.
    Message b =>
    Stream
      { getTopicConfig :: TopicConfig
      , isWatermarked :: Bool
      -- ^ True iff the processor writing to this stream
      -- or a periodic job is watermarking it. If True, 'watermarkSS' will
      -- return a non-Nothing result. When using 'existing'
      -- to access a stream created elsewhere, this will
      -- default to false, even if it really is watermarked.
      -- If you know it is indeed watermarked and you want
      -- downstream data to be watermarked, too, manually set
      -- this to true.
      , _topicMapFilter :: b -> IO (Maybe a)
        -- ^ Quick and dirty "fusion" -- composable mapping and filtering
        -- without the overhead of writing the intermediate results to FDB.
        -- Any side effects in this action will be repeated by all downstream
        -- consumers of this topic, so they should be idempotent.
      }

topicWatermarkSS :: TopicConfig -> WatermarkSS
topicWatermarkSS = flip FDB.extend [FDB.Bytes "wm"] . topicCustomMetadataSS

-- | Returns the subspace in which the given stream's watermarks are stored.
--
watermarkSS :: Stream a -> Maybe WatermarkSS
watermarkSS stream =
  if isWatermarked stream
    then Just
           $ topicWatermarkSS
           $ getTopicConfig stream
    else Nothing

-- | Returns the current watermark for the given stream, if it can be determined.
streamWatermark :: Stream a -> Transaction (Maybe Watermark)
streamWatermark = undefined

instance Functor Stream where
  fmap g (Stream c ps f) = Stream c ps (fmap (fmap g) . f)

instance Filterable Stream where
  mapMaybe g (Stream c ps f) = Stream c ps (fmap (g =<<) . f)

-- | Registers an IO transformation to perform on each message if/when the
-- stream is consumed downstream. Return 'Nothing' to filter the stream. Side
-- effects here should be benign and relatively cheap -- they could be run many
-- times for the same input.
benignIO :: (a -> IO (Maybe b)) -> Stream a -> Stream b
benignIO g (Stream cfg ps (f :: c -> IO (Maybe a))) =
  Stream cfg ps $ \(x :: c) -> do
    y <- f x
    case y of
      Nothing -> return Nothing
      Just z -> g z

-- | Type specifying how the output stream of a stream processor should be
-- watermarked.
data WatermarkBy a =
  -- | Watermark the output stream by taking the minimum checkpoint across
  -- all partitions of all input streams, getting the watermark of all those
  -- streams as of those checkpoints, and persisting that as the watermark at
  -- the current database version. Because this logic is somewhat expensive,
  -- it is not run transactionally with the stream processor's core logic.
  -- Instead, it is executed periodically.
  DefaultWatermark
  -- | A function that assigns a watermark to the output of a stream processor.
  --
  -- This function will be called on only one output event for each batch of events
  -- processed.
  --
  -- If this function returns a watermark that is less than the watermark it
  -- returned on a previous invocation for the stream, its output will be
  -- ignored, because watermarks must be monotonically increasing.
  --
  -- This watermark will be applied transactionally with each batch processed.
  | CustomWatermark (a -> Transaction Watermark)
  -- | Do not watermark the output stream. This can be used for batch inputs,
  -- or in cases where there are no concerns about the completeness of data
  -- in aggregation tables. Naturally, this option is most performant.
  | NoWatermark

producesWatermark :: WatermarkBy a -> Bool
producesWatermark NoWatermark = False
producesWatermark _           = True

-- TODO: think more about what type params we actually need.
-- Do we need both outMsg and runResult? Do we need inMsg, or
-- instead should we have inStream?
data StreamStep inMsg outMsg runResult where

  -- | A step that writes to a topic. This would usually be used for testing and
  -- such, since it doesn't have any checkpointing mechanism.
  WriteOnlyProcessor :: Message a => {
    writeOnlyWatermarkBy :: WatermarkBy a,
    writeOnlyProduce :: Transaction (Maybe a)
  } -> StreamStep Void a (Stream a)

  StreamProcessor :: Message b => {
    streamProcessorInStream     :: Stream a,
    streamProcessorWatermarkBy  :: WatermarkBy b,
    streamProcessorProcessBatch :: Seq (Versionstamp 'Complete, a)
                                -> Transaction (Seq b)
    } -> StreamStep a b (Stream b)

  -- TODO: the tuple input here is kind of a lie. It would
  -- be the user's job to tie them together into pairs, like we
  -- do with the 1-to-1 join logic.
  Stream2Processor :: Message b => {
    stream2ProcessorInStreamL :: Stream a1,
    stream2ProcessorInStreamR :: Stream a2,
    stream2ProcessorWatermarkBy :: WatermarkBy b,
    -- TODO: passing in the TopicConfig so that the step knows
    -- where to write its state. Probably all user-provided
    -- batch processing callbacks should take a subspace that they
    -- can use to write per-processor state. That way, deleting a
    -- processing step would be guaranteed to clean up everything
    -- the user created, too.
    stream2ProcessorRunBatchL :: (TopicConfig
                              -> Seq (Versionstamp 'Complete, a1)
                              -> Transaction (Seq b)),
    stream2ProcessorRunBatchR :: (TopicConfig
                              -> Seq (Versionstamp 'Complete, a2)
                              -> Transaction (Seq b))
  } -> StreamStep (a1,a2) b (Stream b)

  TableProcessor :: (Ord k, AT.TableKey k, AT.TableSemigroup aggr) => {
    tableProcessorGroupedBy :: GroupedBy k v,
    tableProcessorAggregation :: v -> aggr,
    -- | An optional custom watermark function for this aggregation.
    -- NOTE: the input to the watermark function will be only the
    -- value that will be monoidally appended to the value already
    -- stored in the table at key k, not the full value stored at
    -- k. This is done for efficiency reasons. If you want the full
    -- value at key k, you will need to fetch it yourself inside
    -- your watermark function. --TODO: is that possible?
    tableProcessorWatermarkBy :: WatermarkBy (k, aggr)
  } -> StreamStep v (k, aggr) (AT.AggrTable k aggr)

  TriggeringTableProcessor :: {
    --TODO: now that we can't constrain this to being a plain TableProcessor,
    --we could have nested triggering table processors. Add another type tag?
    --live with it?
    triggeringTableProcessorTableProcessor :: StreamStep v (k, aggr) (AT.AggrTable k aggr),
    triggeringTableProcessorTriggerBy :: (Versionstamp 'Complete, v, [k])
                                      -> Transaction [(k,aggr)]
  } -> StreamStep v (k, aggr) (AT.AggrTable k aggr)

watermarkBy :: (b -> Transaction Watermark) -> StreamStep a b r -> StreamStep a b r
watermarkBy f s@WriteOnlyProcessor{} = s { writeOnlyWatermarkBy = CustomWatermark f }
watermarkBy f s@StreamProcessor{} = s { streamProcessorWatermarkBy = CustomWatermark f }
watermarkBy f s@Stream2Processor{} = s { stream2ProcessorWatermarkBy = CustomWatermark f }
watermarkBy f (TableProcessor g a _) = TableProcessor g a (CustomWatermark f)
watermarkBy f (TriggeringTableProcessor s t) = TriggeringTableProcessor (watermarkBy f s) t

triggerBy :: ((Versionstamp 'Complete, v, [k]) -> Transaction [(k,aggr)])
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
triggerBy f tbp@(TableProcessor{}) = TriggeringTableProcessor tbp f
triggerBy f (TriggeringTableProcessor tbp _) = TriggeringTableProcessor tbp f

-- The reason we are using a tagless final interface like this in addition to
-- the record types, when it seems like we might only need one, is because we
-- want the benefits of both. The tagless final interface allows us to interpret
-- the DAG in multiple ways, including checking for errors, forward
-- compatibility checks, running with different strategies, etc. The record
-- interface allows us to use a builder style to modify our stream processors
-- at a distance, with functions like 'watermarkBy'.
-- TODO: hey, are we even tagless final anymore? Looks like we have returned to
-- where we started, more or less.
class Monad m => MonadStream m where
  run :: StepName -> StreamStep inMsg outMsg runResult -> m runResult

-- | Monad that traverses a stream DAG, building up a transaction that updates
-- the watermarks for all streams that don't have custom watermarks defined.
-- This transaction will be run periodically, while custom watermarks are
-- committed transactionally with the batch of events being processed.
--
-- The reason this traverses the DAG is so that the watermarks are updated in
-- topological order. This ensures that each stream sees the latest watermark
-- info for its parent streams. The downside is that the transaction may be
-- somewhat large for very large DAGs.
newtype DefaultWatermarker a = DefaultWatermarker
  {runDefaultWatermarker :: StateT (Transaction ()) Identity a}
  deriving (Functor, Applicative, Monad, MonadState (Transaction ()))

instance MonadStream DefaultWatermarker where
  run = undefined --TODO

-- | Runs the standard watermark logic for a stream. For each input stream,
-- finds the minimum checkpoint across all its partitions. For each of these
-- checkpoints, find the corresponding watermark for the corresponding input
-- stream. The minimum of these watermarks is our new watermark. This watermark
-- is persisted as the watermark for the output topic.
--
-- This function assumes that all parent watermarks are monotonically
-- increasing, for efficiency. If that's not true, chaos will ensue.
--
-- This function also assumes that all input streams are actually
-- watermarked. If not, we produce the default watermark (Jan 1 1970).
defaultWatermark :: [TopicConfig]
                 -- ^ All parent input streams
                 -> StepName
                 -- ^ The name of the step producing the output stream
                 -> Stream a
                 -- ^ The output stream we are watermarking
                 -> Transaction ()
defaultWatermark parents sn outStream = for_ (watermarkSS outStream) \wmSS -> do
  minCheckpointsF <- withSnapshot $ for parents \parent -> do
                       chkptsF <- getCheckpoints parent sn
                       return
                         $ flip fmap chkptsF \ckpts -> (parent, minimumDef minBound ckpts)
  ourOldWMF <- getCurrentWatermark wmSS
  minCheckpoints <- for minCheckpointsF await
  parentWMsF <- for minCheckpoints
                    \(parent, CompleteVersionstamp (TransactionVersionstamp v _) _) ->
                      getWatermark (topicWatermarkSS parent) v
  parentsWMs <- for parentWMsF await
  let minParentWM = minimumMay (catMaybes parentsWMs)
  ourOldWM <- await ourOldWMF
  case (ourOldWM, minParentWM) of
    -- parents don't have watermarks, and we don't. Don't bother writing.
    (Nothing, Nothing) -> return ()
    (Nothing, Just newWM) -> setWatermark wmSS newWM
    (Just oldWM, Nothing) -> return ()
    (Just oldWM, Just newWM) | newWM > oldWM -> setWatermark wmSS newWM
    (Just _, Just _) -> return ()

-- | Read messages from an existing Stream.
--
-- If you want events downstream of this to have watermarks, set
-- 'isWatermarked' to @True@ on the result if you know that the existing Stream
-- has a watermark; this function doesn't have enough information to
-- determine that itself.
existing :: (Message a, MonadStream m) => TopicConfig -> m (Stream a)
existing tc = return (Stream tc False (return . Just))

-- TODO: if this handler type took a batch at a time,
-- it would be easier to optimize -- imagine if it were to
-- to do a get from FDB for each item -- it could do them all
-- in parallel.
-- TODO: produce isn't idempotent in cases of CommitUnknownResult.
-- TODO: for a lot of applications, we won't actually want to blindly persist
-- the raw input unchanged -- imagine if we were reading from Kafka. Totally
-- redundant. This should be wrapped up into a generalized Stream type that can
-- read from non-FDB sources (or just pull in anything). Then we would just
-- need pipe.
produce :: (Message a, MonadStream m) => StepName -> Transaction (Maybe a) -> m (Stream a)
produce sn f =
  run sn
    $ WriteOnlyProcessor NoWatermark f

-- TODO: better operation for externally-visible side effects. Perhaps we can
-- introduce an atMostOnceSideEffect type for these sorts of things, that
-- checkpoints and THEN performs side effects. The problem is if the thread
-- dies after the checkpoint but before the side effect, or if the side effect
-- fails. We could maintain a set of in-flight side effects, and remove them
-- from the set once finished. In that case, we could try to recover by
-- traversing the items in the set that are older than t.

-- | Produce a side effect at least once for each message in the stream.
-- TODO: is this going to leave traces of an empty topic in FDB?
atLeastOnce :: (Message a, MonadStream m) => StepName -> Stream a -> (a -> IO ()) -> m ()
atLeastOnce sn input f = void $ do
  run sn
    $ StreamProcessor {streamProcessorInStream = input,
                       streamProcessorWatermarkBy = NoWatermark,
                       streamProcessorProcessBatch = (mapM (\(_,x) -> liftIO $ f x))}

-- I think we can do so if we remove the DB from the topic config internals. Not
-- sure why it's there in the first place. Alternatively, maybe we should remove
-- the output stream from the processor record -- defer the responsibility of
-- creating it to the functions of the MonadStream class? OTOH, if we make it
-- MonadStream's responsibility, we have to repeat the implementation in each
-- instance of that class, instead of doing it here once.
pipe ::
  (Message a, Message b, MonadStream m) =>
  StepName ->
  Stream a ->
  -- TODO: let user take a batch of items at once, and potentially split
  -- one message into multiple downstream messages.
  -- i.e., below type should be (t a -> IO (t b))
  -- TODO: this should allow adding arbitrary ops to the transaction, so
  -- that we can implement other cool stuff in terms of this abstraction, like
  -- secondary indexes.
  (a -> IO (Maybe b)) ->
  m (Stream b)
pipe sn input f =
  run sn
    $ pipe' input f

pipe' ::
  Message b =>
  Stream a ->
  -- TODO: let user take a batch of items at once, and potentially split
  -- one message into multiple downstream messages.
  -- i.e., below type should be (t a -> IO (t b))
  (a -> IO (Maybe b)) ->
  StreamStep a b (Stream b)
pipe' input f =
  StreamProcessor {streamProcessorInStream = input,
                   streamProcessorWatermarkBy = if isWatermarked input
                                                   then DefaultWatermark
                                                   else NoWatermark,
                   streamProcessorProcessBatch = (\b -> catMaybes <$> forM b (\(_,x) -> liftIO $ f x))}

-- | Streaming one-to-one join. If the relationship is not actually one-to-one
--   (i.e. the input join functions are not injective), some messages in the
--   input streams could be lost.
oneToOneJoin ::
  (Message a, Message b, Message c, Message d, MonadStream m) =>
  StepName ->
  Stream a ->
  Stream b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  m (Stream d)
oneToOneJoin sn in1 in2 p1 p2 j =
  run sn
    $ oneToOneJoin' in1 in2 p1 p2 j

oneToOneJoin' ::
  (Message a, Message b, Message c, Message d) =>
  Stream a ->
  Stream b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  StreamStep (a, b) d (Stream d)
oneToOneJoin' inl inr pl pr j =
  let
    lstep = \cfg -> oneToOneJoinStep cfg 0 pl j
    rstep = \cfg -> oneToOneJoinStep cfg 1 pr (flip j)
    in Stream2Processor inl
                        inr
                        (if isWatermarked inl && isWatermarked inr
                            then DefaultWatermark
                            else NoWatermark)
                        lstep
                        rstep

-- NOTE: the reason that this is a separate constructor from StreamAggregate
-- is so that our helper functions can be combined more easily. It's easier to
-- work with and refactor code that looks like @count . groupBy id@ rather
-- than the less compositional @countBy id@. At least, that's what it looks
-- like at the time of this writing. Kafka Streams does it that way. If it
-- ends up not being worth it, simplify.
-- TODO: implement one-to-many joins in terms of this?
groupBy ::
  (MonadStream m) =>
  (v -> [k]) ->
  Stream v ->
  m (GroupedBy k v)
groupBy k t = return (GroupedBy t k)

aggregate ::
  (Message v, Ord k, AT.TableKey k, AT.TableSemigroup aggr, MonadStream m) =>
  StepName ->
  GroupedBy k v ->
  (v -> aggr) ->
  m (AT.AggrTable k aggr)
aggregate sn groupedBy f =
  run sn
    $ aggregate' groupedBy f

aggregate' ::
  (Message v, Ord k, AT.TableKey k, AT.TableSemigroup aggr) =>
  GroupedBy k v ->
  (v -> aggr) ->
  StreamStep v (k, aggr) (AT.AggrTable k aggr)
aggregate' groupedBy@(GroupedBy input _) f =
  TableProcessor
    groupedBy
    f
    (if isWatermarked input then DefaultWatermark else NoWatermark)

class HasStreamConfig m where
  getStreamConfig :: m FDBStreamConfig

-- TODO: having the DB and SS inside the TopicConfig really clumsifies the
-- interface for the builder-style streams, and also requires static analysis of
-- the DAG to have an FDB connection. Fix this.
makeStream ::
  (Message a, HasStreamConfig m, Monad m) =>
  StepName ->
  -- | isWatermarked
  Bool ->
  m (Stream a)
makeStream sn isWm = do
  sc <- getStreamConfig
  let tc = makeTopicConfig (streamConfigDB sc) (streamConfigSS sc) sn
  return $
    Stream tc isWm (return . Just)

forEachPartition :: Monad m => Stream a -> (PartitionId -> m ()) -> m ()
forEachPartition (Stream cfg _ _) = forM_ [0 .. numPartitions cfg - 1]

-- | An implementation of the streaming system that uses distributed leases to
-- ensure mutual exclusion for each worker process. This reduces DB conflicts
-- and CPU contention, and increases throughput and scalability.
newtype LeaseBasedStreamWorker a
  = LeaseBasedStreamWorker
      {unLeaseBasedStreamWorker :: StateT (FDBStreamConfig, TaskRegistry) IO a}
  deriving (Functor, Applicative, Monad, MonadState (FDBStreamConfig, TaskRegistry), MonadIO)

instance HasStreamConfig LeaseBasedStreamWorker where
  getStreamConfig = gets fst

taskRegistry :: LeaseBasedStreamWorker TaskRegistry
taskRegistry = snd <$> State.get

{-
-- | Repeatedly run a transaction so long as another transaction returns True.
-- Both transactions are run in one larger transaction, to ensure correctness.
-- Unfortunately, the performance overhead of this was too great at the time
-- of this writing, and the pipeline eventually falls behind.
_doWhileValid ::
  Database ->
  FDBStreamConfig ->
  Maybe StreamEdgeMetrics ->
  StepName ->
  Transaction Bool ->
  Transaction (Int, Async ()) ->
  IO ()
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
    Just wasValid ->
      when wasValid $
        _doWhileValid db streamCfg metrics sn stillValid action
    -- Nothing means an exception was thrown; try again.
    Nothing -> _doWhileValid db streamCfg metrics sn stillValid action
-}

-- | Like doWhileValid, but doesn't transactionally check that the lease is
-- still good -- just uses the system clock to approximately run as long as we
-- have the lock. Any logic this thing runs needs to be safe even without locks,
-- which is the case for our stream processing steps as of the time of this
-- writing -- the locks are just there to reduce duplicate wasted work from
-- multiple threads working on the same thing.
doForSeconds :: Int -> IO () -> IO ()
doForSeconds n f = do
  startTime <- secondsSinceEpoch
  let go = do
        currTime <- secondsSinceEpoch
        when (currTime <= startTime + n) (f >> go)
  go

mkTaskName :: StepName -> PartitionId -> TaskName
mkTaskName sn pid = TaskName $ BS8.pack (show sn ++ "_" ++ show pid)

runCustomWatermark :: WatermarkBy a -> Transaction (Seq a) -> Transaction (Seq a)
runCustomWatermark (CustomWatermark f) t = error "TODO"
runCustomWatermark _ t = t

instance MonadStream LeaseBasedStreamWorker where

  run sn WriteOnlyProcessor {writeOnlyProduce,
                             writeOnlyWatermarkBy} = do
    output <- makeStream sn (producesWatermark writeOnlyWatermarkBy)
    (cfg@FDBStreamConfig {msgsPerBatch, streamConfigDB}, taskReg) <- State.get
    metrics <- registerStepMetrics sn
    let job _stillValid _release =
          doForSeconds (leaseDuration cfg)
            $ void
            $ logErrors cfg metrics sn
            $ runTransaction streamConfigDB
            -- TODO: if we trust the user to give us a monotonic
            -- watermark function, custom watermarking could use versionstamped
            -- operations for improved performance.
            $ runCustomWatermark writeOnlyWatermarkBy
            $ produceStep msgsPerBatch (getTopicConfig output) writeOnlyProduce
    liftIO
      $ runTransaction streamConfigDB
      $ addTask taskReg (TaskName sn) (leaseDuration cfg) job
    return output

  run sn StreamProcessor { streamProcessorInStream
                        , streamProcessorWatermarkBy
                        , streamProcessorProcessBatch} = do
    output <- makeStream sn (producesWatermark streamProcessorWatermarkBy)
    let outCfg = getTopicConfig output
    (cfg@FDBStreamConfig {streamConfigDB}, _) <- State.get
    metrics <- registerStepMetrics sn
    let job pid _stillValid _release =
          doForSeconds (leaseDuration cfg)
            $ void
            $ logErrors cfg metrics sn
            $ runTransaction streamConfigDB
            $ runCustomWatermark streamProcessorWatermarkBy --TODO: complete
            $ pipeStep cfg streamProcessorInStream outCfg sn streamProcessorProcessBatch metrics pid
    forEachPartition streamProcessorInStream $ \pid -> do
      let taskName = mkTaskName sn pid
      taskReg <- taskRegistry
      liftIO
        $ runTransaction streamConfigDB
        $ addTask taskReg taskName (leaseDuration cfg) (job pid)
    return output

  run sn (Stream2Processor inl inr _watermarker ls rs) = do
    cfg@FDBStreamConfig {streamConfigDB} <- getStreamConfig
    output <- makeStream sn (isWatermarked inl && isWatermarked inr)
    let outCfg = getTopicConfig output
    metrics <- registerStepMetrics sn
    let lname = sn <> "0"
    let rname = sn <> "1"
    let ljob pid _stillValid _release =
          doForSeconds (leaseDuration cfg)
            $ void
            $ logErrors cfg metrics sn
            $ runTransaction streamConfigDB
            $ pipeStep cfg inl outCfg lname (ls outCfg) metrics pid
    let rjob pid _stillValid _release =
          doForSeconds (leaseDuration cfg)
            $ void
            $ logErrors cfg metrics sn
            $ runTransaction streamConfigDB
            $ pipeStep cfg inr outCfg rname (rs outCfg) metrics pid
    forEachPartition inl $ \pid -> do
      let lTaskName = TaskName $ BS8.pack (show lname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      liftIO
        $ runTransaction streamConfigDB
        $ addTask taskReg lTaskName (leaseDuration cfg) (ljob pid)
    forEachPartition inr $ \pid -> do
      let rTaskName = TaskName $ BS8.pack (show rname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      liftIO
        $ runTransaction streamConfigDB
        $ addTask taskReg rTaskName (leaseDuration cfg) (rjob pid)
    return output

  run sn TableProcessor { tableProcessorGroupedBy
                        , tableProcessorAggregation } = do
    cfg@FDBStreamConfig {streamConfigDB} <- getStreamConfig
    let table = getAggrTable cfg sn
    let (GroupedBy inStream _) = tableProcessorGroupedBy
    metrics <- registerStepMetrics sn
    let job pid _stillValid _release =
          doForSeconds (leaseDuration cfg)
            $ void
            $ logErrors cfg metrics sn
            $ runTransaction streamConfigDB
            $ aggregateStep cfg
                            sn
                            tableProcessorGroupedBy
                            tableProcessorAggregation
                            metrics
                            pid
    forEachPartition inStream $ \pid -> do
      taskReg <- taskRegistry
      let taskName = TaskName $ BS8.pack (show sn ++ "_" ++ show pid)
      liftIO
        $ runTransaction streamConfigDB
        $ addTask taskReg taskName (leaseDuration cfg) (job pid)
    return table

  run _sn TriggeringTableProcessor{} = error "triggers not implemented"

-- TODO: what if we have recently removed steps from our topology? Old leases
-- will be registered forever. Need to remove old ones.
registerAllLeases :: FDBStreamConfig -> LeaseBasedStreamWorker a -> IO (a, TaskRegistry)
registerAllLeases cfg wkr = do
  tr <- TaskRegistry.empty (taskRegSS cfg)
  fmap (fmap snd)
    $ flip State.runStateT (cfg, tr)
    $ unLeaseBasedStreamWorker wkr

runLeaseStreamWorker :: Int -> FDBStreamConfig -> LeaseBasedStreamWorker a -> IO ()
runLeaseStreamWorker numThreads cfg topology = do
  -- TODO: need a way for user to get the pure result of their topology. Need
  -- a pure runner or something.
  (_pureResult, taskReg) <- registerAllLeases cfg topology
  threads <- replicateM numThreads $ async $ forever $
    runRandomTask (streamConfigDB cfg) taskReg >>= \case
      False -> threadDelay (leaseDuration cfg * 1000000 `div` 2)
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
readPartitionBatchExactlyOnce ::
  Stream a ->
  Maybe StreamEdgeMetrics ->
  ReaderName ->
  PartitionId ->
  Word8 ->
  Transaction (Seq (Versionstamp 'Complete, a))
readPartitionBatchExactlyOnce (Stream outCfg _ mapFilter) metrics rn pid n = do
  rawMsgs <- readNAndCheckpoint' outCfg pid rn n
  liftIO $ when (Seq.null rawMsgs) (incrEmptyBatchCount metrics)
  liftIO $ recordMsgsPerBatch metrics (Seq.length rawMsgs)
  let msgs = fmap (\(vs, e) -> (vs, fromMessage e)) rawMsgs
  msgs' <- liftIO $ forM msgs \(vs, e) -> mapFilter e >>= \case
             Nothing -> return Nothing
             Just e' -> return $ Just (vs,e')
  return $ catMaybes msgs'

getAggrTable :: FDBStreamConfig -> StepName -> AT.AggrTable k v
getAggrTable sc sn = AT.AggrTable {..}
  where
    aggrTableSS =
      FDB.extend (streamConfigSS sc) [C.topics, FDB.Bytes sn, C.aggrTable]
    aggrTableNumPartitions = 2

-- TODO: other persistence backends
-- TODO: should probably rename to TopologyConfig
data FDBStreamConfig
  = FDBStreamConfig
      { streamConfigDB :: FDB.Database,
        -- | subspace that will contain all state for the stream topology
        streamConfigSS :: FDB.Subspace,
        streamMetricsStore :: Maybe Metrics.Store,
        -- | Number of messages to process per transaction per thread per partition
        msgsPerBatch :: Word8,
        -- | Length of time an individual worker should work on a single stage of the
        -- pipeline before stopping and trying to work on something else. Higher
        -- values are more efficient in normal operation, but if enough machines fail,
        -- higher values can be a worst-case lower bound on end-to-end latency.
        -- Only applies to pipelines run with the LeaseBasedStreamWorker monad.
        leaseDuration :: Int
      }

taskRegSS :: FDBStreamConfig -> FDB.Subspace
taskRegSS cfg = FDB.extend (streamConfigSS cfg) [FDB.Bytes "leases"]

-- | The core loop body for every stream job. Throttles the job based on any
-- errors that occur, records timing metrics.
logErrors ::
  FDBStreamConfig ->
  Maybe StreamEdgeMetrics ->
  StepName ->
  IO (Seq a) ->
  IO ()
logErrors _ metrics sn x =
  flip
    catches
    [ Handler
        ( \case
            Error (MaxRetriesExceeded (CError TransactionTimedOut)) ->
              threadDelay 15000
            CError TransactionTimedOut ->
              threadDelay 15000
            e -> throw e
        ),
      Handler
        ( \case
            Error (MaxRetriesExceeded (CError NotCommitted)) -> do
              incrConflicts metrics
              threadDelay 15000
            e -> throw e
        ),
      Handler
        ( \(e :: SomeException) -> do
            tid <- myThreadId
            printf "%s on thread %s caught %s\n" (show sn) (show tid) (show e)
        )
    ]
    $ do
      threadDelay 150
      t1 <- getTime Monotonic
      numProcessed <- length <$> x
        `catch` \(e :: FDB.Error) -> case e of
          Error (MaxRetriesExceeded (CError TransactionTimedOut)) -> do
            t2 <- getTime Monotonic
            let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
            printf
              "%s timed out after %d ms, assuming we processed no messages.\n"
              (show sn)
              timeMillis
            return mempty
          _ -> throw e
      t2 <- getTime Monotonic
      let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
      recordBatchLatency metrics timeMillis
      when (numProcessed == 0) (threadDelay 1000000)

-- | Given a function that computes a watermark for a new output value and a
-- versionstamp, run the function and, if the output watermark is greater
-- than the current watermark for this stream, sets the stream watermark
-- to the output watermark.
watermarkLogic :: Maybe ((Versionstamp 'Complete, a) -> Transaction Watermark)
               -> Transaction (Seq (Versionstamp 'Complete, a))
               -> Transaction (Seq (Versionstamp 'Complete, a))
watermarkLogic Nothing t = t
watermarkLogic _ _ = undefined

produceStep ::
  Message a =>
  Word8 ->
  TopicConfig ->
  Transaction (Maybe a) ->
  FDB.Transaction (Seq a)
produceStep batchSize outCfg step = do
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  xs <- catMaybes <$> replicateM (fromIntegral batchSize) step
  p' <- liftIO $ randPartition outCfg
  writeTopic' outCfg p' (fmap toMessage xs)
  return (Seq.fromList xs)

pipeStep ::
  (Message b) =>
  FDBStreamConfig ->
  Stream a ->
  TopicConfig ->
  StepName ->
  (Seq (Versionstamp 'Complete, a) -> Transaction (Seq b)) ->
  Maybe StreamEdgeMetrics ->
  PartitionId ->
  Transaction (Seq b)
pipeStep
  FDBStreamConfig {msgsPerBatch}
  inTopic
  outCfg
  sn
  transformBatch
  metrics
  pid = do
    inMsgs <-
      readPartitionBatchExactlyOnce
        inTopic
        metrics
        sn
        pid
        msgsPerBatch
    ys <- transformBatch inMsgs
    let outMsgs = fmap toMessage ys
    p' <- liftIO $ randPartition outCfg
    writeTopic' outCfg p' outMsgs
    return ys

oneToOneJoinStep ::
  forall a1 a2 b c .
  (Message a1, Message a2, Message c) =>
  TopicConfig ->
  -- | Index of the stream being consumed. 0 for left side of
  -- join, 1 for right. This will be refactored to support
  -- n-way joins in the future.
  Int ->
  (a1 -> c) ->
  (a1 -> a2 -> b) ->
  Seq (Versionstamp 'Complete, a1)
  -> Transaction (Seq b)
oneToOneJoinStep outputTopicCfg streamJoinIx pl combiner msgs = do
  -- Read from one of the two join streams, and for each
  -- message read, compute the join key. Using the join key, look in the
  -- join table to see if the partner is already there. If so, write the tuple
  -- downstream. If not, write the one message we do have to the join table.
  -- TODO: think of a way to garbage collect items that never get joined.
  let sn = "1:1"
  let joinSS = topicCustomMetadataSS outputTopicCfg --TODO: pass in only this?
  let otherIx = if streamJoinIx == 0 then 1 else 0
  joinFutures <-
      forM
        (fmap (pl . snd) msgs)
        -- TODO: changed where we are storing join data -- high potential for bugs here
        (withSnapshot . get1to1JoinData joinSS sn otherIx)
  joinData <- Seq.zip msgs <$> mapM await joinFutures
  fmap catMaybes $ forM joinData $ \(lmsg, d) -> do
    let k = pl (snd lmsg)
    case d of
      Just (rmsg :: a2) -> do
        delete1to1JoinData joinSS sn k
        return $ Just $ combiner (snd lmsg) rmsg
      Nothing -> do
        write1to1JoinData joinSS sn k streamJoinIx (snd lmsg :: a1)
        return Nothing

aggregateStep ::
  forall v k aggr.
  (Ord k, AT.TableKey k, AT.TableSemigroup aggr) =>
  FDBStreamConfig ->
  StepName ->
  GroupedBy k v ->
  (v -> aggr) ->
  Maybe StreamEdgeMetrics ->
  PartitionId ->
  FDB.Transaction (Seq (k,aggr))
aggregateStep
  c@FDBStreamConfig {msgsPerBatch}
  sn
  (GroupedBy inTopic toKeys)
  toAggr
  metrics
  pid = do
    let table = getAggrTable c sn
    msgs <- fmap snd <$>
      readPartitionBatchExactlyOnce
        inTopic
        metrics
        sn
        pid
        msgsPerBatch
    let kvs = [(k,toAggr v) | v <- toList msgs, k <- toKeys v]
    AT.mappendBatch table pid kvs
    return (Seq.fromList kvs)
