{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ConstrainedClassMethods #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}

module FDBStreaming
  ( MonadStream,
    Stream(streamTopicConfig),
    isStreamWatermarked,
    getStreamWatermark,
    JobConfig (..),
    existing,
    existingWatermarked,
    produce,
    atLeastOnce,
    pipe,
    oneToOneJoin,
    groupBy,
    GroupedBy (..),
    aggregate,
    getAggrTable,
    benignIO,
    runStream,
    runPure,

    -- * watermarks and triggers
    run,
    --triggerBy,
    watermarkBy,

    -- * Advanced Usage
    StreamStep (..),
    topicWatermarkSS
  )
where

import Control.Concurrent (myThreadId, threadDelay)
import Control.Concurrent.Async (async, waitAny)
import Control.Exception
  ( Handler (Handler),
    SomeException,
    bracket,
    catch,
    catches,
    throw,
  )
import Control.Monad (forever, replicateM, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Identity (Identity)
import qualified Control.Monad.Reader as Reader
import Control.Monad.Reader (MonadReader, ReaderT)
import qualified Control.Monad.State.Strict as State
import Control.Monad.State.Strict (MonadState, StateT)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS8
import Data.Foldable (for_, toList)
import Data.Sequence (Seq ())
import qualified Data.Sequence as Seq
import Data.Text.Encoding (decodeUtf8)
import Data.Traversable (for)
import Data.Void (Void)
import Data.Witherable (catMaybes, witherM)
import Data.Word (Word8)
import qualified FDBStreaming.AggrTable as AT
import FDBStreaming.Joins
  ( delete1to1JoinData,
    get1to1JoinData,
    write1to1JoinData,
  )
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.TaskLease (TaskName (TaskName), secondsSinceEpoch)
import FDBStreaming.Stream.Internal (Stream (Stream, streamWatermarkSS, streamReadAndCheckpoint, streamMinReaderPartitions, streamTopicConfig, streamName, setUpState, destroyState), StreamName, StreamReadAndCheckpoint, isStreamWatermarked, streamConsumerCheckpointSS, getStreamWatermark)
import FDBStreaming.JobConfig(JobConfig(JobConfig, jobConfigDB, msgsPerBatch, numStreamThreads, numPeriodicJobThreads, streamMetricsStore, jobConfigSS, leaseDuration), JobSubspace)
import FDBStreaming.TaskRegistry as TaskRegistry
  ( TaskRegistry,
    addTask,
    empty,
    runRandomTask,
  )
import FDBStreaming.Topic
  ( PartitionId,
    ReaderName,
    TopicConfig (topicCustomMetadataSS),
    getCheckpoints,
    makeTopicConfig,
    randPartition,
    readNAndCheckpoint',
    writeTopic',
  )
import qualified FDBStreaming.Topic as Topic
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkSS,
    getWatermark,
    setWatermark,
  )
import FoundationDB as FDB
  ( Transaction,
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
  ( TransactionVersionstamp (TransactionVersionstamp),
    Versionstamp (CompleteVersionstamp),
    VersionstampCompleteness (Complete),
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
  for (streamMetricsStore sc) $ \store -> liftIO $ do
    mp <- Metrics.createCounter ("stream." <> sn <> ".messagesProcessed") store
    er <- Metrics.createCounter ("stream." <> sn <> ".emptyReads") store
    bl <- Metrics.createDistribution ("stream." <> sn <> ".batchLatency") store
    mb <- Metrics.createDistribution ("stream." <> sn <> ".msgsPerBatch") store
    cs <- Metrics.createCounter ("stream." <> sn <> ".conflicts") store
    return (StreamEdgeMetrics mp er bl mb cs)

incrEmptyBatchCount :: Maybe StreamEdgeMetrics -> IO ()
incrEmptyBatchCount m = for_ m $ Counter.inc . emptyReads

recordMsgsPerBatch :: Maybe StreamEdgeMetrics -> Int -> IO ()
recordMsgsPerBatch m n = for_ m $ \metrics ->
  Distribution.add (messagesPerBatch metrics) (fromIntegral n)

incrConflicts :: Maybe StreamEdgeMetrics -> IO ()
incrConflicts m = for_ m $ Counter.inc . conflicts

recordBatchLatency :: Integral a => Maybe StreamEdgeMetrics -> a -> IO ()
recordBatchLatency m x = for_ m $ \metrics ->
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

-- | The name of a step in the pipeline. A step can be thought of as a function
-- that consumes a stream and writes to a stream, a table, or a side effect.
type StepName = ByteString

data GroupedBy k v
  = GroupedBy
      { groupedByInStream :: Stream v,
        groupedByFunction :: v -> [k]
      }

-- | The watermark subspace for this topic. This can be used to manually get
-- and set the watermark on a topic, outside of the stream processing system.
topicWatermarkSS :: TopicConfig -> WatermarkSS
topicWatermarkSS = flip FDB.extend [FDB.Bytes "wm"] . topicCustomMetadataSS

-- | Returns the subspace in which the given stream's watermarks are stored.
watermarkSS :: JobSubspace -> Stream a -> Maybe WatermarkSS
watermarkSS jobSS stream = case streamWatermarkSS stream of
  Nothing -> Nothing
  Just wmSS -> Just (wmSS jobSS)

-- | Registers an IO transformation to perform on each message if/when the
-- stream is consumed downstream. Return 'Nothing' to filter the stream. Side
-- effects here should be benign and relatively cheap -- they could be run many
-- times for the same input.
benignIO :: (a -> IO (Maybe b)) -> Stream a -> Stream b
benignIO g (Stream rc np wmSS stc streamName setUp destroy) =
  build $ \cfg rn pid ss n state -> do
    xs <- rc cfg rn pid ss n state
    flip witherM xs $ \(mv, x) -> liftIO (g x) >>= \case
      Nothing -> return Nothing
      Just y -> return $ Just (mv, y)

  where build x = Stream x np wmSS stc streamName  setUp destroy

-- | Type specifying how the output stream of a stream processor should be
-- watermarked.
data WatermarkBy a
  = -- | Watermark the output stream by taking the minimum checkpoint across
    -- all partitions of all input streams, getting the watermark of all those
    -- streams as of those checkpoints, and persisting that as the watermark at
    -- the current database version. Because this logic is somewhat expensive,
    -- it is not run transactionally with the stream processor's core logic.
    -- Instead, it is executed periodically.
    DefaultWatermark
  | -- | A function that assigns a watermark to the output of a stream processor.
    --
    -- This function will be called on only one output event for each batch of events
    -- processed.
    --
    -- If this function returns a watermark that is less than the watermark it
    -- returned on a previous invocation for the stream, its output will be
    -- ignored, because watermarks must be monotonically increasing.
    --
    -- This watermark will be applied transactionally with each batch processed.
    CustomWatermark (a -> Transaction Watermark)
  | -- | Do not watermark the output stream. This can be used for batch inputs,
    -- or in cases where there are no concerns about the completeness of data
    -- in aggregation tables. Naturally, this option is most performant.
    NoWatermark

producesWatermark :: WatermarkBy a -> Bool
producesWatermark NoWatermark = False
producesWatermark _ = True

isDefaultWatermark :: WatermarkBy a -> Bool
isDefaultWatermark DefaultWatermark = True
isDefaultWatermark _ = False

data TriggerBy --TODO: decide what this needs to be

-- TODO: think more about what type params we actually need.
-- Do we need both outMsg and runResult? Do we need inMsg, or
-- instead should we have inStream?
data StreamStep inMsg outMsg runResult where
  -- | A step that writes to a topic. This would usually be used for testing and
  -- such, since it doesn't have any checkpointing mechanism.
  WriteOnlyProcessor ::
    Message a =>
    { writeOnlyWatermarkBy :: WatermarkBy a,
      writeOnlyProduce :: Transaction (Maybe a)
    } ->
    StreamStep Void a
      (Stream a)
  StreamProcessor ::
    Message b =>
    { streamProcessorInStream :: Stream a,
      streamProcessorWatermarkBy :: WatermarkBy b,
      streamProcessorProcessBatch ::
        Seq (Maybe (Versionstamp 'Complete), a) ->
        Transaction (Seq b)
    } ->
    StreamStep a b
      (Stream b)
  -- TODO: the tuple input here is kind of a lie. It would
  -- be the user's job to tie them together into pairs, like we
  -- do with the 1-to-1 join logic.
  Stream2Processor ::
    Message b =>
    { stream2ProcessorInStreamL :: Stream a1,
      stream2ProcessorInStreamR :: Stream a2,
      stream2ProcessorWatermarkBy :: WatermarkBy b,
      -- TODO: passing in the TopicConfig so that the step knows
      -- where to write its state. Probably all user-provided
      -- batch processing callbacks should take a subspace that they
      -- can use to write per-processor state. That way, deleting a
      -- processing step would be guaranteed to clean up everything
      -- the user created, too.
      stream2ProcessorRunBatchL ::
        ( TopicConfig ->
          Seq (Maybe (Versionstamp 'Complete), a1) ->
          Transaction (Seq b)
        ),
      stream2ProcessorRunBatchR ::
        ( TopicConfig ->
          Seq (Maybe (Versionstamp 'Complete), a2) ->
          Transaction (Seq b)
        )
    } ->
    StreamStep (a1, a2) b
      (Stream b)
  TableProcessor ::
    (Ord k, AT.TableKey k, AT.TableSemigroup aggr) =>
    { tableProcessorGroupedBy :: GroupedBy k v,
      tableProcessorAggregation :: v -> aggr,
      -- | An optional custom watermark function for this aggregation.
      -- NOTE: the input to the watermark function is only the
      -- value that will be monoidally appended to the value already
      -- stored in the table at key k, not the full value stored at
      -- k. This is done for efficiency reasons. If you want the full
      -- value at key k, you will need to fetch it yourself inside
      -- your watermark function.
      tableProcessorWatermarkBy :: WatermarkBy (k, aggr),
      tableProcessorTriggerBy :: Maybe TriggerBy
    } ->
    StreamStep v (k, aggr)
      (AT.AggrTable k aggr)

stepWatermarkBy :: StreamStep inMsg outMsg runResult -> WatermarkBy outMsg
stepWatermarkBy WriteOnlyProcessor {..} = writeOnlyWatermarkBy
stepWatermarkBy StreamProcessor {..} = streamProcessorWatermarkBy
stepWatermarkBy Stream2Processor {..} = stream2ProcessorWatermarkBy
stepWatermarkBy TableProcessor {..} = tableProcessorWatermarkBy

stepProducesWatermark :: StreamStep inMsg outMsg runResult -> Bool
stepProducesWatermark = producesWatermark . stepWatermarkBy

isStepDefaultWatermarked :: StreamStep a b r -> Bool
isStepDefaultWatermarked = isDefaultWatermark . stepWatermarkBy

watermarkBy :: (b -> Transaction Watermark) -> StreamStep a b r -> StreamStep a b r
watermarkBy f (WriteOnlyProcessor _ x) = WriteOnlyProcessor (CustomWatermark f) x
watermarkBy f (StreamProcessor input _ p) = StreamProcessor input (CustomWatermark f) p
watermarkBy f (Stream2Processor inl inr _ pl pr) = Stream2Processor inl inr (CustomWatermark f) pl pr
watermarkBy f (TableProcessor g a _ trigger) = TableProcessor g a (CustomWatermark f) trigger

{- TODO
triggerBy :: ((Versionstamp 'Complete, v, [k]) -> Transaction [(k,aggr)])
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
triggerBy f tbp@(TableProcessor{}) = TriggeringTableProcessor tbp f
-}

-- The reason we are using a tagless final interface like this in addition to
-- the record types, when it seems like we might only need one, is because we
-- want the benefits of both. The tagless final interface allows us to interpret
-- the DAG in multiple ways, including checking for errors, forward
-- compatibility checks, running with different strategies, etc. The record
-- interface allows us to use a builder style to modify our stream processors
-- at a distance, with functions like 'watermarkBy'.
class Monad m => MonadStream m where
  run :: (Message inMsg) => StepName -> StreamStep inMsg outMsg runResult -> m runResult

-- | Monad that traverses a stream DAG, building up a transaction that updates
-- the watermarks for all streams that don't have custom watermarks defined.
-- This transaction will be run periodically, while custom watermarks are
-- committed transactionally with the batch of events being processed.
--
-- The reason this traverses the DAG is so that the watermarks are updated in
-- topological order. This ensures that each stream sees the latest watermark
-- info for its parent streams. The downside is that the transaction may be
-- somewhat large for very large DAGs.
-- NOTE: originally I was planning on watermarking the entire DAG in one
-- transaction (and making this StateT of Transaction, not IO), but I hit
-- an unexpected FDB behavior: https://github.com/apple/foundationdb/issues/2504
newtype DefaultWatermarker a
  = DefaultWatermarker
      {runDefaultWatermarker :: StateT (JobConfig, IO ()) Identity a}
  deriving (Functor, Applicative, Monad, MonadState (JobConfig, IO ()))

registerDefaultWatermarker ::
  JobConfig ->
  TaskRegistry ->
  DefaultWatermarker a ->
  IO ()
registerDefaultWatermarker cfg@(JobConfig {jobConfigDB = db}) taskReg x = do
  let job = snd $ State.execState (runDefaultWatermarker x) (cfg, return ())
  runTransaction db $ addTask taskReg "dfltWM" 1 \_ _ -> job

instance HasStreamConfig DefaultWatermarker where
  getStreamConfig = State.gets fst

watermarkNext :: Transaction () -> DefaultWatermarker ()
watermarkNext t = do
  JobConfig {jobConfigDB} <- getStreamConfig
  State.modify \(c, t') -> (c, t' >> runTransaction jobConfigDB t)

instance MonadStream DefaultWatermarker where
  run sn w@WriteOnlyProcessor {} = makeStream sn sn w --writer has no parents, nothing to do
  run sn step@StreamProcessor {streamProcessorInStream} = do
    JobConfig{jobConfigSS} <- getStreamConfig
    output <- makeStream sn sn step
    -- TODO:
    -- We need to check that a lot of stuff is in place before we can default
    -- watermark.
    -- 1. Does the user want the default watermark propagated to the output of
    --    this step?
    -- 2. Does the output topic have a watermark subspace? (It always will if
    --    the user specified a watermark for this step)
    -- 3. Does the input have a watermark subspace?
    -- 4. Is the input a stream inside FoundationDB? If not, we can't get the
    --    checkpoint for this reader that corresponds to a point in the
    --    watermark function.
    --
    -- This is a long list! Need to find a way to simplify it.
    case ( isStepDefaultWatermarked step
         , watermarkSS jobConfigSS output
         , streamWatermarkSS streamProcessorInStream
         , streamTopicConfig streamProcessorInStream) of
      (True, Just wmSS, Just _inWmSS, Just inTopic) ->
        watermarkNext (defaultWatermark [(inTopic, sn)] wmSS)
      _ -> return ()
    return output

  run sn step@Stream2Processor {stream2ProcessorInStreamL, stream2ProcessorInStreamR} = do
    JobConfig{jobConfigSS} <- getStreamConfig
    output <- makeStream sn sn step
    case ( isStepDefaultWatermarked step
         , watermarkSS jobConfigSS output
         , streamWatermarkSS stream2ProcessorInStreamL
         , streamTopicConfig stream2ProcessorInStreamL
         , streamWatermarkSS stream2ProcessorInStreamR
         , streamTopicConfig stream2ProcessorInStreamR) of
      (True, Just wmSS, Just _inWmSSL, Just inTopicL, Just _inWmSSR, Just inTopicR) ->
        watermarkNext
          $ defaultWatermark
            [ (inTopicL, sn <> "0"),
              (inTopicR, sn <> "1")
            ]
            wmSS
      _ -> return ()
    return output

  run sn step@TableProcessor {tableProcessorGroupedBy} = do
    cfg <- getStreamConfig
    let table = getAggrTable cfg sn
    let inStream = groupedByInStream tableProcessorGroupedBy
    case ( isStepDefaultWatermarked step
         , AT.aggrTableWatermarkSS table
         , streamTopicConfig inStream
         , streamWatermarkSS inStream) of
      (True, wmSS, Just inTopic, Just _inWMSS) ->
        watermarkNext
          $ defaultWatermark
              [(inTopic, sn)]
              wmSS
      _ -> return ()
    return table

-- | Runs the standard watermark logic for a stream. For each input stream,
-- finds the minimum checkpoint across all its partitions. For each of these
-- checkpoints, find the corresponding watermark for the corresponding input
-- stream. The minimum of these watermarks is our new watermark. This watermark
-- is persisted as the watermark for the output topic.
--
-- This function assumes that all parent watermarks are monotonically
-- increasing, for efficiency. If that's not true, the output watermark
-- won't be, either.
--
-- This function also assumes that all input streams are actually
-- watermarked. If not, we don't write a watermark.
defaultWatermark ::
  -- | All parent input streams, paired with the name
  -- of the reader whose checkpoints we use to look up
  -- watermarks.
  [(TopicConfig, ReaderName)] ->
  -- | The output stream we are watermarking
  WatermarkSS ->
  Transaction ()
defaultWatermark topicsAndReaders wmSS = do
  minCheckpointsF <- withSnapshot $ for topicsAndReaders \(parent, rn) -> do
    chkptsF <- getCheckpoints parent rn
    return $
      flip fmap chkptsF \ckpts ->
        (parent, minimumDef minBound ckpts)
  minCheckpoints <- for minCheckpointsF await
  parentWMsF <- for minCheckpoints \(parent, CompleteVersionstamp (TransactionVersionstamp v _) _) ->
    getWatermark (topicWatermarkSS parent) v
  parentsWMs <- for parentWMsF await
  let minParentWM = minimumMay (catMaybes parentsWMs)
  case minParentWM of
    Nothing -> return ()
    Just newWM -> setWatermark wmSS newWM

-- | Read messages from an existing Stream.
--
-- If you want events downstream of this to have watermarks, set
-- 'isWatermarked' to @True@ on the result if you know that the existing Stream
-- has a watermark; this function doesn't have enough information to
-- determine that itself.
existing :: (Message a, MonadStream m) => TopicConfig -> m (Stream a)
existing tc =
  return
  $ fmap fromMessage
  $ streamFromTopic tc
  $ Topic.topicName tc

-- | Read messages from an existing stream which is known to be watermarked.
-- If the existing stream is not watermarked, watermarks will not be propagated
-- downstream from this stream.
existingWatermarked :: (Message a, MonadStream m) => TopicConfig -> m (Stream a)
existingWatermarked tc =
  fmap (setStreamWatermarkByTopic tc) (existing tc)

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
  run sn $
    WriteOnlyProcessor NoWatermark f

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
  run sn $
    StreamProcessor
      { streamProcessorInStream = input,
        streamProcessorWatermarkBy = NoWatermark,
        streamProcessorProcessBatch = (mapM (\(_, x) -> liftIO $ f x))
      }

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
  (a -> IO (Maybe b)) ->
  m (Stream b)
pipe sn input f =
  run sn $
    pipe' input f

pipe' ::
  Message b =>
  Stream a ->
  (a -> IO (Maybe b)) ->
  StreamStep a b (Stream b)
pipe' input f =
  StreamProcessor
    { streamProcessorInStream = input,
      streamProcessorWatermarkBy =
        if isStreamWatermarked input
          then DefaultWatermark
          else NoWatermark,
      streamProcessorProcessBatch = (\b -> catMaybes <$> for b (\(_, x) -> liftIO $ f x))
    }

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
  run sn $
    oneToOneJoin' in1 in2 p1 p2 j

oneToOneJoin' ::
  (Message a, Message b, Message c, Message d) =>
  Stream a ->
  Stream b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  StreamStep (a, b) d (Stream d)
oneToOneJoin' inl inr pl pr j =
  let lstep = \cfg -> oneToOneJoinStep cfg 0 pl j
      rstep = \cfg -> oneToOneJoinStep cfg 1 pr (flip j)
   in Stream2Processor
        inl
        inr
        ( if isStreamWatermarked inl && isStreamWatermarked inr
            then DefaultWatermark
            else NoWatermark
        )
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
  run sn $
    aggregate' groupedBy f

aggregate' ::
  (Ord k, AT.TableKey k, AT.TableSemigroup aggr) =>
  GroupedBy k v ->
  (v -> aggr) ->
  StreamStep v (k, aggr) (AT.AggrTable k aggr)
aggregate' groupedBy@(GroupedBy input _) f =
  TableProcessor
    groupedBy
    f
    (if isStreamWatermarked input then DefaultWatermark else NoWatermark)
    Nothing

class HasStreamConfig m where
  getStreamConfig :: m JobConfig

-- TODO: having the DB and SS inside the TopicConfig really clumsifies the
-- interface for the builder-style streams, and also requires static analysis of
-- the DAG to have an FDB connection. Fix this.
makeStream ::
  (Message b, HasStreamConfig m, Monad m) =>
  StepName ->
  StreamName ->
  StreamStep a b r ->
  m (Stream b)
makeStream stepName streamName step = do
  sc <- getStreamConfig
  let streamTopicConfig = makeTopicConfig (jobConfigSS sc) stepName
  return
    $ fmap fromMessage
    $ (if stepProducesWatermark step
       then setStreamWatermarkByTopic streamTopicConfig
       else id)
    $ streamFromTopic streamTopicConfig streamName

-- | sets the watermark subspace to this stream to the watermark subspace of
-- the given topic. The given topic must be the topic in streamTopicConfig, and
-- the step writing to the topic must actually have a watermarking function (or
-- default watermark) set. Don't export this; too confusing for users.
setStreamWatermarkByTopic :: TopicConfig -> Stream a -> Stream a
setStreamWatermarkByTopic tc stream =
  stream { streamWatermarkSS = Just $ \_ -> topicWatermarkSS tc}

-- | Create a stream from a topic. Assumes the topic is not watermarked. If it
-- is and you want to propagate watermarks downstream, the caller must call
-- 'setStreamWatermarkByTopic' on the result.
--
-- You only need this function if you are trying to access a topic that was
-- created by another pipeline.
streamFromTopic :: TopicConfig -> StreamName -> Stream ByteString
streamFromTopic tc streamName =
  Stream
  { streamReadAndCheckpoint = \_cfg rn pid _chkptSS n _state -> do
      msgs <- readNAndCheckpoint' tc pid rn n
      return $ fmap (\(vs, x) -> (Just vs, x)) msgs
  , streamMinReaderPartitions = Topic.numPartitions tc
  , streamWatermarkSS = Nothing
  , streamTopicConfig = Just tc
  , streamName = streamName
  , setUpState = return ()
  , destroyState = const $ return ()
  }

forEachPartition :: Monad m => Stream a -> (PartitionId -> m ()) -> m ()
forEachPartition s = for_ [0 .. streamMinReaderPartitions s - 1]

-- | An implementation of the streaming system that uses distributed leases to
-- ensure mutual exclusion for each worker process. This reduces DB conflicts
-- and CPU contention, and increases throughput and scalability.
newtype LeaseBasedStreamWorker a
  = LeaseBasedStreamWorker
      {unLeaseBasedStreamWorker :: ReaderT (JobConfig, TaskRegistry) IO a}
  deriving (Functor, Applicative, Monad, MonadReader (JobConfig, TaskRegistry), MonadIO)

instance HasStreamConfig LeaseBasedStreamWorker where
  getStreamConfig = Reader.asks fst

taskRegistry :: LeaseBasedStreamWorker TaskRegistry
taskRegistry = snd <$> Reader.ask

{-
-- | Repeatedly run a transaction so long as another transaction returns True.
-- Both transactions are run in one larger transaction, to ensure correctness.
-- Unfortunately, the performance overhead of this was too great at the time
-- of this writing, and the pipeline eventually falls behind.
_doWhileValid ::
  Database ->
  JobConfig ->
  Maybe StreamEdgeMetrics ->
  StepName ->
  Transaction Bool ->
  Transaction (Int, Async ()) ->
  IO ()
_doWhileValid db streamCfg metrics sn stillValid action = do
  wasValidMaybe <- throttleByErrors metrics sn $ runTransaction db $
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

runCustomWatermark :: WatermarkBy a -> Transaction (Int, Seq a) -> Transaction (Int, Seq a)
runCustomWatermark (CustomWatermark f) t = do
  (n, xs) <- t
  case xs of
    (_ Seq.:|> x) -> void $ f x
    _ -> return ()
  return (n, xs)
runCustomWatermark _ t = t

outputStreamAndTopic
  :: (HasStreamConfig m, Message c, Monad m)
  => StepName
  -> StreamStep a b (Stream c)
  -> m (Stream c, TopicConfig)
outputStreamAndTopic stepName step = do
  sc <- getStreamConfig
  let streamTopicConfig = makeTopicConfig (jobConfigSS sc) stepName
  let stream = (if stepProducesWatermark step
                then setStreamWatermarkByTopic streamTopicConfig
                else id)
                $ streamFromTopic streamTopicConfig stepName
  return (fmap fromMessage stream, streamTopicConfig)

instance MonadStream LeaseBasedStreamWorker where
  run
    sn
    s@WriteOnlyProcessor
      { writeOnlyProduce,
        writeOnlyWatermarkBy
      } = do
      (outStream, outTopic) <- outputStreamAndTopic sn s
      (cfg@JobConfig {msgsPerBatch, jobConfigDB}, taskReg) <- Reader.ask
      metrics <- registerStepMetrics sn
      let job _stillValid _release =
            doForSeconds (leaseDuration cfg)
              $ void
              $ throttleByErrors metrics sn
              $ runTransaction jobConfigDB
              $ runCustomWatermark writeOnlyWatermarkBy
              $ produceStep msgsPerBatch outTopic writeOnlyProduce
      liftIO
        $ runTransaction jobConfigDB
        $ addTask taskReg (TaskName sn) (leaseDuration cfg) job
      return outStream
  run
    sn
    s@StreamProcessor
      { streamProcessorInStream,
        streamProcessorWatermarkBy,
        streamProcessorProcessBatch
      } = do
      (outStream, outTopic) <- outputStreamAndTopic sn s
      (cfg@JobConfig {jobConfigDB}, _) <- Reader.ask
      metrics <- registerStepMetrics sn
      -- NOTE: we must use a case here to do the pattern match to work around a
      -- GHC limitation. In 8.6, let gives a very confusing error message about
      -- variables escaping their scopes. In 8.8, it gives a more helpful
      -- "my brain just exploded" message that says to use a case statement.
      let job pid _stillValid _release =
            case streamProcessorInStream of
              Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
                bracket (runTransaction jobConfigDB setUpState) destroyState \state ->
                  doForSeconds (leaseDuration cfg)
                    $ void
                    $ throttleByErrors metrics sn
                    $ runTransaction jobConfigDB
                    $ runCustomWatermark streamProcessorWatermarkBy
                    $ pipeStep
                        cfg
                        streamProcessorInStream
                        streamReadAndCheckpoint
                        state
                        outTopic
                        sn
                        streamProcessorProcessBatch
                        metrics
                        pid
      forEachPartition streamProcessorInStream $ \pid -> do
        let taskName = mkTaskName sn pid
        taskReg <- taskRegistry
        liftIO
          $ runTransaction jobConfigDB
          $ addTask taskReg taskName (leaseDuration cfg) (job pid)
      return outStream
  run sn s@(Stream2Processor inl inr watermarker ls rs) = do
    cfg@JobConfig {jobConfigDB} <- getStreamConfig
    (outStream, outTopic) <- outputStreamAndTopic sn s
    metrics <- registerStepMetrics sn
    let lname = sn <> "0"
    let rname = sn <> "1"
    let ljob pid _stillValid _release =
          case inl of
              Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
                bracket (runTransaction jobConfigDB setUpState) destroyState \state ->
                  doForSeconds (leaseDuration cfg)
                    $ void
                    $ throttleByErrors metrics sn
                    $ runTransaction jobConfigDB
                    $ runCustomWatermark watermarker
                    $ pipeStep
                        cfg
                        inl
                        streamReadAndCheckpoint
                        state
                        outTopic
                        lname
                        (ls outTopic)
                        metrics
                        pid
    let rjob pid _stillValid _release =
          case inr of
              Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
                bracket (runTransaction jobConfigDB setUpState) destroyState \state ->
                  doForSeconds (leaseDuration cfg)
                    $ void
                    $ throttleByErrors metrics sn
                    $ runTransaction jobConfigDB
                    $ runCustomWatermark watermarker
                    $ pipeStep
                        cfg
                        inr
                        streamReadAndCheckpoint
                        state
                        outTopic
                        rname
                        (rs outTopic)
                        metrics
                        pid
    forEachPartition inl $ \pid -> do
      let lTaskName = TaskName $ BS8.pack (show lname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      liftIO
        $ runTransaction jobConfigDB
        $ addTask taskReg lTaskName (leaseDuration cfg) (ljob pid)
    forEachPartition inr $ \pid -> do
      let rTaskName = TaskName $ BS8.pack (show rname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      liftIO
        $ runTransaction jobConfigDB
        $ addTask taskReg rTaskName (leaseDuration cfg) (rjob pid)
    return outStream
  run
    sn
    TableProcessor
      { tableProcessorGroupedBy,
        tableProcessorAggregation,
        tableProcessorWatermarkBy
      } = do
      cfg@JobConfig {jobConfigDB} <- getStreamConfig
      let table = getAggrTable cfg sn
      let (GroupedBy inStream _) = tableProcessorGroupedBy
      metrics <- registerStepMetrics sn
      let job pid _stillValid _release =
            case inStream of
              Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
                bracket (runTransaction jobConfigDB setUpState) destroyState \state ->
                  doForSeconds (leaseDuration cfg)
                  $ void
                  $ throttleByErrors metrics sn
                  $ runTransaction jobConfigDB
                  $ runCustomWatermark tableProcessorWatermarkBy
                  $ aggregateStep
                    cfg
                    sn
                    tableProcessorGroupedBy
                    streamReadAndCheckpoint
                    state
                    tableProcessorAggregation
                    metrics
                    pid
      forEachPartition inStream $ \pid -> do
        taskReg <- taskRegistry
        let taskName = TaskName $ BS8.pack (show sn ++ "_" ++ show pid)
        liftIO
          $ runTransaction jobConfigDB
          $ addTask taskReg taskName (leaseDuration cfg) (job pid)
      return table

-- TODO: what if we have recently removed steps from our topology? Old leases
-- will be registered forever. Need to remove old ones.
registerContinuousLeases ::
  JobConfig ->
  LeaseBasedStreamWorker a ->
  IO (a, TaskRegistry)
registerContinuousLeases cfg wkr = do
  tr <- TaskRegistry.empty (continuousTaskRegSS cfg)
  x <-
    flip Reader.runReaderT (cfg, tr) $
      unLeaseBasedStreamWorker wkr
  return (x, tr)

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

getAggrTable :: JobConfig -> StepName -> AT.AggrTable k v
getAggrTable sc sn = AT.AggrTable {..}
  where
    aggrTableSS =
      FDB.extend (jobConfigSS sc) [C.topics, FDB.Bytes sn, C.aggrTable]
    aggrTableNumPartitions = 2


continuousTaskRegSS :: JobConfig -> FDB.Subspace
continuousTaskRegSS cfg = FDB.extend (jobConfigSS cfg) [FDB.Bytes "leasec"]

periodicTaskRegSS :: JobConfig -> FDB.Subspace
periodicTaskRegSS cfg = FDB.extend (jobConfigSS cfg) [FDB.Bytes "leasep"]

-- | The core loop body for every stream job. Throttles the job based on any
-- errors that occur, records timing metrics.
throttleByErrors ::
  Maybe StreamEdgeMetrics ->
  StepName ->
  IO (Int, Seq a) ->
  IO ()
throttleByErrors metrics sn x =
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
      (numConsumed, _) <- x
        `catch` \(e :: FDB.Error) -> case e of
          Error (MaxRetriesExceeded (CError TransactionTimedOut)) -> do
            t2 <- getTime Monotonic
            let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
            printf
              "%s timed out after %d ms, assuming we processed no messages.\n"
              (show sn)
              timeMillis
            return (0, mempty)
          _ -> throw e
      t2 <- getTime Monotonic
      let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
      recordBatchLatency metrics timeMillis
      when (numConsumed == 0) (threadDelay 1000000)

produceStep ::
  Message a =>
  Word8 ->
  TopicConfig ->
  Transaction (Maybe a) ->
  -- | Number of incoming messages consumed, outgoing messages
  FDB.Transaction (Int, Seq a)
produceStep batchSize outCfg step = do
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  xs <- catMaybes <$> Seq.replicateM (fromIntegral batchSize) step
  p' <- liftIO $ randPartition outCfg
  writeTopic' outCfg p' (fmap toMessage xs)
  return (0, xs)

-- | Returns number of messages consumed from upstream, and
-- new messages to send downstream.
pipeStep ::
  (Message b) =>
  JobConfig ->
  Stream a ->
  (StreamReadAndCheckpoint a state) ->
  state ->
  TopicConfig ->
  StepName ->
  (Seq (Maybe (Versionstamp 'Complete), a) -> Transaction (Seq b)) ->
  Maybe StreamEdgeMetrics ->
  PartitionId ->
  Transaction (Int, Seq b)
pipeStep
  cfg@JobConfig {msgsPerBatch}
  inStream
  readAndCheckpoint
  state
  outCfg
  sn
  transformBatch
  metrics
  pid = do
    let checkpointSS = streamConsumerCheckpointSS (jobConfigSS cfg) inStream sn
    inMsgs <-
      readAndCheckpoint
        cfg
        sn
        pid
        checkpointSS
        msgsPerBatch
        state
    liftIO $ when (Seq.null inMsgs) (incrEmptyBatchCount metrics)
    liftIO $ recordMsgsPerBatch metrics (Seq.length inMsgs)
    ys <- transformBatch inMsgs
    let outMsgs = fmap toMessage ys
    p' <- liftIO $ randPartition outCfg
    writeTopic' outCfg p' outMsgs
    return (length inMsgs, ys)

oneToOneJoinStep ::
  forall a1 a2 b c.
  (Message a1, Message a2, Message c) =>
  TopicConfig ->
  -- | Index of the stream being consumed. 0 for left side of
  -- join, 1 for right. This will be refactored to support
  -- n-way joins in the future.
  Int ->
  (a1 -> c) ->
  (a1 -> a2 -> b) ->
  Seq (Maybe (Versionstamp 'Complete), a1) ->
  Transaction (Seq b)
oneToOneJoinStep outputTopicCfg streamJoinIx pl combiner msgs = do
  -- Read from one of the two join streams, and for each
  -- message read, compute the join key. Using the join key, look in the
  -- join table to see if the partner is already there. If so, write the tuple
  -- downstream. If not, write the one message we do have to the join table.
  -- TODO: think of a way to garbage collect items that never get joined.
  let sn = "1:1"
  let joinSS = topicCustomMetadataSS outputTopicCfg
  let otherIx = if streamJoinIx == 0 then 1 else 0
  joinFutures <-
    for msgs \(_, msg) -> do
      let k = pl msg
      joinF <- withSnapshot $ get1to1JoinData joinSS sn otherIx k
      return (k, msg, joinF)
  joinData <- for joinFutures \(k, msg, joinF) -> do
    d <- await joinF
    return (k, msg, d)
  fmap catMaybes $ for joinData \(k, lmsg, d) -> do
    case d of
      Just (rmsg :: a2) -> do
        delete1to1JoinData joinSS sn k
        return $ Just $ combiner lmsg rmsg
      Nothing -> do
        write1to1JoinData joinSS sn k streamJoinIx (lmsg :: a1)
        return Nothing

aggregateStep ::
  forall v k aggr state.
  (Ord k, AT.TableKey k, AT.TableSemigroup aggr) =>
  JobConfig ->
  StepName ->
  GroupedBy k v ->
  StreamReadAndCheckpoint v state ->
  state ->
  (v -> aggr) ->
  Maybe StreamEdgeMetrics ->
  PartitionId ->
  FDB.Transaction (Int, Seq (k, aggr))
aggregateStep
  cfg@JobConfig {msgsPerBatch}
  sn
  (GroupedBy inStream toKeys)
  streamReadAndCheckpoint
  state
  toAggr
  metrics
  pid = do
    let table = getAggrTable cfg sn
    let checkpointSS = streamConsumerCheckpointSS (jobConfigSS cfg) inStream sn
    msgs <-
      fmap snd
        <$> streamReadAndCheckpoint
              cfg
              sn
              pid
              checkpointSS
              msgsPerBatch
              state
    liftIO $ when (Seq.null msgs) (incrEmptyBatchCount metrics)
    liftIO $ recordMsgsPerBatch metrics (Seq.length msgs)
    let kvs = Seq.fromList [(k, toAggr v) | v <- toList msgs, k <- toKeys v]
    AT.mappendBatch table pid kvs
    return (length msgs, kvs)

logErrors :: String -> IO () -> IO ()
logErrors ident =
  flip
    catches
    [ Handler \(e :: SomeException) -> do
        tid <- myThreadId
        printf "%s on thread %s caught %s\n" ident (show tid) (show e)
    ]

runStream :: JobConfig -> (forall m. MonadStream m => m a) -> IO ()
runStream
  cfg@JobConfig {jobConfigDB, numStreamThreads, numPeriodicJobThreads}
  topology = do
    -- Run threads for continuously-running stream steps
    (_pureResult, continuousTaskReg) <- registerContinuousLeases cfg topology
    continuousThreads <- replicateM numStreamThreads $ async $ forever $ logErrors "Continuous job" $
      runRandomTask jobConfigDB continuousTaskReg >>= \case
        False -> threadDelay (leaseDuration cfg * 1000000 `div` 2)
        True -> return ()
    -- Run threads for periodic jobs (watermarking, cleanup, etc)
    periodicTaskReg <- TaskRegistry.empty (periodicTaskRegSS cfg)
    registerDefaultWatermarker cfg periodicTaskReg topology
    periodicThreads <- replicateM numPeriodicJobThreads $ async $ forever $ logErrors "Periodic job" $
      runRandomTask jobConfigDB periodicTaskReg >>= \case
        False -> threadDelay 1000000
        True -> return ()
    _ <- waitAny (continuousThreads ++ periodicThreads)
    return ()

-- | MonadStream instance that just returns results out of topologies without
-- executing them.
newtype PureStream a = PureStream {unPureStream :: ReaderT JobConfig Identity a}
  deriving (Functor, Applicative, Monad, MonadReader JobConfig)

instance HasStreamConfig PureStream where
  getStreamConfig = Reader.ask

instance MonadStream PureStream where
  run sn s@WriteOnlyProcessor {} = makeStream sn sn s
  run sn s@StreamProcessor {} = makeStream sn sn s
  run sn s@Stream2Processor {} = makeStream sn sn s
  run sn TableProcessor {} = do
    cfg <- getStreamConfig
    let table = getAggrTable cfg sn
    return table

-- | Extracts the output of a MonadStream topology without side effects.
-- This can be used to return handles to any streams and tables you want
-- to read from outside of the stream processing paradigm.
runPure :: JobConfig -> (forall m. MonadStream m => m a) -> a
runPure cfg x = Reader.runReader (unPureStream x) cfg
