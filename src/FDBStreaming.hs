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
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

-- | FDBStreaming is a poorly-named, work-in-progress, proof-of-concept library
-- for large-scale processing of unbounded data sets and sources. It is inspired
-- by both Kafka Streams and the Dataflow Model. It uses <https://www.foundationdb.org/ FoundationDB>
-- for all data storage and worker coordination. It began as a project to try out
-- FoundationDB. FoundationDB enables us to support several features which are
-- not present in similar systems, and allowed FDBStreaming to be written with
-- surprisingly little code.
--
-- = Features
--
-- * Fault-tolerant, stateless workers.
-- * Durability provided by FoundationDB.
-- * Exactly-once semantics.
-- * Simple deployment like Kafka Streams -- deploy as many instances of a
--   binary executable, wherever and however you want, and they will
--   automatically distribute work amongst themselves.
-- * Simple infrastructure dependency: requires only FoundationDB, which is
--   used for both worker coordination and data storage.
-- * Dataflow-style watermarks and triggers (triggers not yet implemented).
-- * Distributed monoidal aggregations.
-- * Distributed joins.
--
-- = (Ostensibly) differentiating features
--
-- * Serve aggregation results directly from worker instances
--   and FoundationDB -- no separate output step to another database is required.
-- * Secondary indices can be defined on data streams,
--   allowing you to serve and look up individual records within the stream.
-- * Unlike Kafka Streams, no tricky locality limitations on joins.
-- * Easy extensibility to support additional distributed data structures,
--   thanks to FoundationDB. Insert incoming data into priority queues, queues,
--   spatial indices, hierarchical documents, tables, etc., with exactly-once
--   semantics provided transparently.
-- * Insert data into the stream directly with HTTP requests to workers; no
--   separate data store required.
-- * Small codebase.
-- * Group and window by anything -- event time, processing time, color, etc.
--   Unlike the Dataflow Model, event time is not a built-in concept, so complex
--   use cases can be easier to express.
--
-- = Current Limitations
--
-- * Individual messages must be less than 100 kB in size.
-- * At each processing step, processing an individual batch of messages must
--   take less than five seconds.
-- * Because the FoundationDB client library is single-threaded, running many
--   instances of your job executable with a few worker threads per process
--   often gives better performance than running a few instances with many
--   threads per process.
-- * End-to-end pipeline latency (time from when a message enters the pipeline
--   to the time it flows to the end of the pipeline) is generally on the order
--   of seconds.
-- * When storing unbounded stream data in FoundationDB, FDBStreaming performance
--   is bound by FoundationDB. Expect tens to low hundreds of thousands of messages per
--   second for the 'Topic' data structure, and perhaps millions for the 'AggrTable' structure. See <https://apple.github.io/foundationdb/performance.html FoundationDB's docs>
--   for more information about how performance scales. However, if you are only
--   storing monoidal aggregations in FoundationDB and reading data from another
--   data source (Kafka, S3, etc.), or are otherwise aggressively filtering or
--   shrinking the input data before it gets written to FoundationDB, you will
--   probably be bound by the speed at which you can read from the data source.
-- * No autoscaling -- the number of threads per 'StreamStep' must be specified
--   by the user. Autoscaling will be implemented in the future.
-- * No facilities for upgrading jobs with new code -- understanding what
--   changes are safe currently requires some understanding of FDBStreaming
--   internals. Guidelines and tools for identifying breaking changes will be
--   provided in the future.
--
-- = Core concepts
--
-- Each FDBStreaming job consists of unbounded 'Stream's of data, which serve as
-- input and output to 'StreamStep's, which map, filter, and otherwise transform
-- their inputs. Lastly, 'StreamStep's can also group and window their inputs to
-- perform aggregations, which are written to 'AT.AggrTable's. For scalability
-- reasons, aggregations must be commutative monoids. Many aggregations included
-- out-of-the-box in FDBStreaming are implemented in terms of FoundationDB's
-- atomic operations, which further improves performance.
--
-- 'Stream's are abstract representations of unbounded data sources. Internally,
-- they are represented as instructions for how to ask for the next @n@ items in
-- the stream, and how to checkpoint where we left off. Input 'Stream's can pull
-- data from external data sources, while intermediate streams created by
-- 'StreamStep's are persisted in FoundationDB in a 'Topic' data structure which
-- provides an API roughly similar to Kafka's.
--
-- 'StreamStep's read data from 'Stream's in small batches, transform the batch,
-- and write the results to another 'Stream' or 'AT.AggrTable'. Each batch is
-- processed in a single FoundationDB transaction, which is also used to
-- checkpoint our position in the stream. Furthermore, for advanced use cases,
-- the user can add any additional FoundationDB operations to the transaction.
-- Transactions trivially enable end-to-end exactly-once semantics. The user may
-- also perform arbitrary IO on each batch (the 'Transaction' monad is a
-- 'MonadIO'), but must take care that such IO actions are idempotent.
--
-- 'AT.AggrTable's can be thought of conceptually as @Monoid m => Map k m@. These
-- tables are stored in FoundationDB. Many data processing and analytics
-- questions can be framed in terms of monoidal aggregations, which these tables
-- represent. The API for these tables provides the ability to look up individual
-- values, as well as ranges of keys if the key type is ordered.
--
-- = Writing jobs
--
-- A stream processing job is expressed as a 'MonadStream' action. You should
-- always write a polymorphic action; this is required because your action will
-- be statically analyzed by multiple "interpreters" internally when you call
-- 'runJob' on the action.
--
-- Here is an example of a simple word count action:
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > import Data.Text (Text)
-- > import qualified Data.Text as Text
-- > import FDBStreaming
-- >
-- > wordCount :: MonadStream m => Stream Text -> m (AggrTable Text (Sum Int))
-- > wordCount txts = aggregate "counts" (groupBy Text.words txts) (const (Sum 1))
--
-- Given an existing stream of lines of text, we group the lines by the words
-- they contain, then convert each line into 1 in the 'Sum' monoid.
--
-- do notation is used to create DAGs. For example, a simple pipeline that
-- consumes tweets, collecting statistics on user tweet volume by date and total
-- word counts might look something like this
--
-- > tweetPipeline :: MonadStream m => Stream Tweet -> m ()
-- > tweetPipeline tweets = do
-- >   -- 'fmap' is used to register lazy operations on streams. This has the
-- >   -- benefit of avoiding uninteresting intermediate data in FoundationDB, but
-- >   -- each downstream consumers of tweets' will compute 'preprocess' once per
-- >   -- input tweet.
-- >   let tweets' = fmap preprocess tweets
-- >   let userDates = groupBy (\t -> [(user t, date t)]) tweets'
-- >   userDateTable <- aggregate "userDates" userDates (const (Sum 1))
-- >   counts <- wordCount (fmap text tweets')
-- >   return ()
--
-- The behavior of 'MonadStream''s bind operation is to declare the existence of
-- a data structure in FoundationDB -- either a 'Stream' or an 'AggrTable'. Thus,
-- you will find that each function that returns a 'MonadStream' action requires
-- a unique 'StreamName' or 'StepName'. This unique name is used to prefix the
-- keys in FoundationDB which store the state for the stream, table, and step.
-- Use caution when abstracting over these actions -- if you call one in a loop,
-- each call must be given a distinct name!
--
-- = Monitoring jobs
--
-- This library integrates with the @ekg-core@ package to emit metrics.
-- Currently, each worker for each step emits the following metrics:
--
-- * Counter of messages processed as @stream.<step_name>.messagesProcessed@
-- * Counter of empty batch reads as @stream.<step_name>.emptyReads@. This
--   counts how often the step tried to read upstream messages but found no new
--   messages. This can happen if an upstream step is having trouble keeping up,
--   or if no data is flowing through the pipeline.
-- * Distribution of batch latency: how long it takes to process a batch, in
--   milliseconds. @stream.<step_name>.batchLatency@
-- * Distribution of messages produced per batch as @stream.<stream_name>.msgsPerBatch@.
-- * Counter of transaction conflicts as @stream.<step_name>.conflicts@.
-- * Counter of timeouts as @stream.<step_name>.timeouts@.
module FDBStreaming
  ( MonadStream,
    JobConfig (..),
    runJob,
    runPure,

    -- * Streams
    Stream (streamTopic),
    isStreamWatermarked,
    getStreamWatermark,
    StreamStep,
    Message (..),

    -- * Creating streams
    existing,
    existingWatermarked,
    produce,
    atLeastOnce,
    pipe,
    oneToOneJoin,

    -- * Tables
    AT.AggrTable,
    groupBy,
    GroupedBy (..),
    aggregate,
    getAggrTable,
    benignIO,

    -- * More complex usage
    run,
    watermarkBy,
    withBatchSize,
    withOutputPartitions,
    pipe',
    oneToOneJoin',
    aggregate',

    -- * Advanced Usage
    topicWatermarkSS,
    WatermarkBy,
    streamFromTopic
  )
where

import Control.Concurrent (myThreadId, threadDelay)
import Control.Concurrent.Async (async, waitAny)
import Control.Exception
  ( Handler (Handler),
    SomeException,
    bracket,
    catches,
    throw,
  )
import Control.Logger.Simple (LogConfig (LogConfig), logError, logInfo, logWarn, setLogLevel, showText, withGlobalLogging)
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
import Data.Maybe (fromMaybe)
import Data.Sequence (Seq ())
import qualified Data.Sequence as Seq
import Data.Text.Encoding (decodeUtf8)
import Data.Traversable (for)
import Data.Witherable (catMaybes, witherM)
import Data.Word (Word16, Word8)
import qualified FDBStreaming.AggrTable as AT
import FDBStreaming.JobConfig (JobConfig (JobConfig, defaultNumPartitions, jobConfigDB, jobConfigSS, leaseDuration, logLevel, msgsPerBatch, numPeriodicJobThreads, numStreamThreads, streamMetricsStore), JobSubspace)
import FDBStreaming.Joins
  ( delete1to1JoinData,
    get1to1JoinData,
    write1to1JoinData,
  )
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.Stream.Internal (Stream (Stream, destroyState, setUpState, streamMinReaderPartitions, streamName, streamReadAndCheckpoint, streamTopic, streamWatermarkSS), StreamName, StreamReadAndCheckpoint, getStreamWatermark, isStreamWatermarked, streamConsumerCheckpointSS)
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
    Topic (topicCustomMetadataSS),
    getCheckpoints,
    makeTopic,
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
import qualified FoundationDB as FDB
import FoundationDB (Transaction, await, withSnapshot)
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

data StreamEdgeMetrics
  = StreamEdgeMetrics
      { _messagesProcessed :: Counter,
        emptyReads :: Counter,
        batchLatency :: Distribution,
        messagesPerBatch :: Distribution,
        conflicts :: Counter,
        timeouts :: Counter
      }

registerStepMetrics ::
  (HasJobConfig m, MonadIO m) =>
  StepName ->
  m (Maybe StreamEdgeMetrics)
registerStepMetrics s = do
  sc <- getJobConfig
  let sn = decodeUtf8 s
  for (streamMetricsStore sc) $ \store -> liftIO $ do
    mp <- Metrics.createCounter ("stream." <> sn <> ".messagesProcessed") store
    er <- Metrics.createCounter ("stream." <> sn <> ".emptyReads") store
    bl <- Metrics.createDistribution ("stream." <> sn <> ".batchLatency") store
    mb <- Metrics.createDistribution ("stream." <> sn <> ".msgsPerBatch") store
    cs <- Metrics.createCounter ("stream." <> sn <> ".conflicts") store
    tos <- Metrics.createCounter ("stream." <> sn <> ".timeouts") store
    return (StreamEdgeMetrics mp er bl mb cs tos)

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

incrTimeouts :: Maybe StreamEdgeMetrics -> IO ()
incrTimeouts m = for_ m $ Counter.inc . timeouts

runStreamTxn :: FDB.Database -> FDB.Transaction a -> IO a
runStreamTxn =
  FDB.runTransactionWithConfig FDB.TransactionConfig
    { FDB.idempotent = False,
      FDB.snapshotReads = False,
      -- No point in retrying since we are running in a loop
      -- anyway.
      maxRetries = 0,
      timeout = 5000
    }

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
topicWatermarkSS :: Topic -> WatermarkSS
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
  where
    build x = Stream x np wmSS stc streamName setUp destroy

-- | Specifies how the output stream of a stream processor should be
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

data StreamStepConfig
  = StreamStepConfig
      { -- | Specifies how many partitions the output stream or table should have.
        -- This determines how many threads downstream steps will need in order to
        -- consume the output. This overrides the default value
        stepOutputPartitions :: Maybe Word8,
        stepBatchSize :: Maybe Word16
      }
  deriving (Show, Eq)

defaultStreamStepConfig :: StreamStepConfig
defaultStreamStepConfig = StreamStepConfig Nothing Nothing

data StreamStep outMsg runResult where
  -- | A step that writes to a topic. This would usually be used for testing and
  -- such, since it doesn't have any checkpointing mechanism.
  WriteOnlyProcessor ::
    Message a =>
    { writeOnlyWatermarkBy :: WatermarkBy a,
      writeOnlyProduce :: Transaction (Maybe a),
      writeOnlyStreamStepConfig :: StreamStepConfig
    } ->
    StreamStep a
      (Stream a)
  StreamProcessor ::
    Message b =>
    { streamProcessorInStream :: Stream a,
      streamProcessorWatermarkBy :: WatermarkBy b,
      streamProcessorProcessBatch ::
        Seq (Maybe (Versionstamp 'Complete), a) ->
        Transaction (Seq b),
      streamProcessorStreamStepConfig :: StreamStepConfig
    } ->
    StreamStep b
      (Stream b)
  -- TODO: the tuple input here is kind of a lie. It would
  -- be the user's job to tie them together into pairs, like we
  -- do with the 1-to-1 join logic.
  Stream2Processor ::
    Message b =>
    { stream2ProcessorInStreamL :: Stream a1,
      stream2ProcessorInStreamR :: Stream a2,
      stream2ProcessorWatermarkBy :: WatermarkBy b,
      -- TODO: passing in the Topic so that the step knows
      -- where to write its state. Probably all user-provided
      -- batch processing callbacks should take a subspace that they
      -- can use to write per-processor state. That way, deleting a
      -- processing step would be guaranteed to clean up everything
      -- the user created, too.
      stream2ProcessorRunBatchL ::
        ( Topic ->
          Seq (Maybe (Versionstamp 'Complete), a1) ->
          Transaction (Seq b)
        ),
      stream2ProcessorRunBatchR ::
        ( Topic ->
          Seq (Maybe (Versionstamp 'Complete), a2) ->
          Transaction (Seq b)
        ),
      stream2ProcessorStreamStepConfig :: StreamStepConfig
    } ->
    StreamStep b
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
      tableProcessorTriggerBy :: Maybe TriggerBy,
      tableProcessorStreamStepConfig :: StreamStepConfig
    } ->
    StreamStep (k, aggr)
      (AT.AggrTable k aggr)

stepWatermarkBy :: StreamStep outMsg runResult -> WatermarkBy outMsg
stepWatermarkBy WriteOnlyProcessor {..} = writeOnlyWatermarkBy
stepWatermarkBy StreamProcessor {..} = streamProcessorWatermarkBy
stepWatermarkBy Stream2Processor {..} = stream2ProcessorWatermarkBy
stepWatermarkBy TableProcessor {..} = tableProcessorWatermarkBy

stepProducesWatermark :: StreamStep outMsg runResult -> Bool
stepProducesWatermark = producesWatermark . stepWatermarkBy

isStepDefaultWatermarked :: StreamStep b r -> Bool
isStepDefaultWatermarked = isDefaultWatermark . stepWatermarkBy

-- NOTE: GHC bug prevents us from using record update syntax in this function,
-- which would make this much cleaner. https://gitlab.haskell.org/ghc/ghc/issues/2595
watermarkBy :: (b -> Transaction Watermark) -> StreamStep b r -> StreamStep b r
watermarkBy f (WriteOnlyProcessor _ x stepCfg) = WriteOnlyProcessor (CustomWatermark f) x stepCfg
watermarkBy f (StreamProcessor input _ p stepCfg) = StreamProcessor input (CustomWatermark f) p stepCfg
watermarkBy f (Stream2Processor inl inr _ pl pr stepCfg) = Stream2Processor inl inr (CustomWatermark f) pl pr stepCfg
watermarkBy f (TableProcessor g a _ trigger stepCfg) = TableProcessor g a (CustomWatermark f) trigger stepCfg

getStepConfig :: StreamStep b r -> StreamStepConfig
getStepConfig WriteOnlyProcessor {writeOnlyStreamStepConfig} =
  writeOnlyStreamStepConfig
getStepConfig StreamProcessor {streamProcessorStreamStepConfig} =
  streamProcessorStreamStepConfig
getStepConfig Stream2Processor {stream2ProcessorStreamStepConfig} =
  stream2ProcessorStreamStepConfig
getStepConfig TableProcessor {tableProcessorStreamStepConfig} =
  tableProcessorStreamStepConfig

mapStepConfig :: (StreamStepConfig -> StreamStepConfig) -> StreamStep b r -> StreamStep b r
mapStepConfig f (WriteOnlyProcessor w x c) = WriteOnlyProcessor w x (f c)
mapStepConfig f (StreamProcessor i w p c) = StreamProcessor i w p (f c)
mapStepConfig f (Stream2Processor inl inr w pl pr c) = Stream2Processor inl inr w pl pr (f c)
mapStepConfig f (TableProcessor g a w t c) = TableProcessor g a w t (f c)

-- | Sets the number of messages to process per batch for the given step.
-- Overrides the default set in 'JobConfig.msgsPerBatch'.
--
-- Setting this too large
-- can cause problems. Start at a few hundred and benchmark carefully.
withBatchSize :: Word16 -> StreamStep b r -> StreamStep b r
withBatchSize n = mapStepConfig (\c -> c {stepBatchSize = Just n})

-- | Sets the number of partitions in the output 'Stream' or 'AT.AggrTable'.
-- Overrides the default set in 'JobConfig.defaultNumPartitions'.
--
-- Warning: if you override this for a stream, any aggregations that consume
-- the stream should be set to the same number of partitions for optimal
-- performance.
withOutputPartitions :: Word8 -> StreamStep b r -> StreamStep b r
withOutputPartitions np = mapStepConfig (\c -> c {stepOutputPartitions = Just np})

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

-- | A stream monad, whose actions represents a stream processing DAG.
class Monad m => MonadStream m where
  -- | Run a single named 'StreamStep', producing either a 'Stream' or an
  -- 'AT.AggrTable'. This function is used in conjunction with the
  -- 'pipe'', 'oneToOneJoin'', and 'aggregate'' functions, which return
  -- 'StreamStep's.
  run :: StepName -> StreamStep outMsg runResult -> m runResult

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
  runStreamTxn db $ addTask taskReg "dfltWM" 1 \_ _ -> job

instance HasJobConfig DefaultWatermarker where
  getJobConfig = State.gets fst

watermarkNext :: Transaction () -> DefaultWatermarker ()
watermarkNext t = do
  JobConfig {jobConfigDB} <- getJobConfig
  State.modify \(c, t') -> (c, t' >> runStreamTxn jobConfigDB t)

instance MonadStream DefaultWatermarker where
  run sn w@WriteOnlyProcessor {} = makeStream sn sn w --writer has no parents, nothing to do
  run sn step@StreamProcessor {streamProcessorInStream} = do
    JobConfig {jobConfigSS} <- getJobConfig
    output <- makeStream sn sn step
    -- TODO: There is a lot of stuff that must be checked before we can default
    -- watermark. Can it be simplified?
    -- We need to check:
    -- 1. Does the user want the default watermark propagated to the output of
    --    this step?
    -- 2. Does the output topic have a watermark subspace? (It always will if
    --    the user specified a watermark for this step)
    -- 3. Does the input have a watermark subspace?
    -- 4. Is the input a stream inside FoundationDB? If not, we can't get the
    --    checkpoint for this reader that corresponds to a point in the
    --    watermark function.
    case ( isStepDefaultWatermarked step,
           watermarkSS jobConfigSS output,
           streamWatermarkSS streamProcessorInStream,
           streamTopic streamProcessorInStream
         ) of
      (True, Just wmSS, Just _inWmSS, Just inTopic) ->
        watermarkNext (defaultWatermark [(inTopic, sn)] wmSS)
      _ -> return ()
    return output
  run sn step@Stream2Processor {stream2ProcessorInStreamL, stream2ProcessorInStreamR} = do
    JobConfig {jobConfigSS} <- getJobConfig
    output <- makeStream sn sn step
    case ( isStepDefaultWatermarked step,
           watermarkSS jobConfigSS output,
           streamWatermarkSS stream2ProcessorInStreamL,
           streamTopic stream2ProcessorInStreamL,
           streamWatermarkSS stream2ProcessorInStreamR,
           streamTopic stream2ProcessorInStreamR
         ) of
      (True, Just wmSS, Just _inWmSSL, Just inTopicL, Just _inWmSSR, Just inTopicR) ->
        watermarkNext $
          defaultWatermark
            [ (inTopicL, sn <> "0"),
              (inTopicR, sn <> "1")
            ]
            wmSS
      _ -> return ()
    return output
  run sn step@TableProcessor {tableProcessorGroupedBy} = do
    cfg <- getJobConfig
    let table =
          getAggrTable
            cfg
            sn
            (getStepNumPartitions cfg (getStepConfig step))
    let inStream = groupedByInStream tableProcessorGroupedBy
    case ( isStepDefaultWatermarked step,
           AT.aggrTableWatermarkSS table,
           streamTopic inStream,
           streamWatermarkSS inStream
         ) of
      (True, wmSS, Just inTopic, Just _inWMSS) ->
        watermarkNext $
          defaultWatermark
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
  [(Topic, ReaderName)] ->
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
existing :: (Message a, MonadStream m) => Topic -> m (Stream a)
existing tc =
  return
    $ fmap fromMessage
    $ streamFromTopic tc
    $ Topic.topicName tc

-- | Read messages from an existing stream which is known to be watermarked.
-- If the existing stream is not watermarked, watermarks will not be propagated
-- downstream from this stream.
existingWatermarked :: (Message a, MonadStream m) => Topic -> m (Stream a)
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
    WriteOnlyProcessor NoWatermark f defaultStreamStepConfig

-- TODO: better operation for externally-visible side effects. Perhaps we can
-- introduce an atMostOnceSideEffect type for these sorts of things, that
-- checkpoints and THEN performs side effects. The problem is if the thread
-- dies after the checkpoint but before the side effect, or if the side effect
-- fails. We could maintain a set of in-flight side effects, and remove them
-- from the set once finished. In that case, we could try to recover by
-- traversing the items in the set that are older than t.

-- | Produce a side effect at least once for each message in the stream.
-- TODO: is this going to leave traces of an empty topic in FDB?
atLeastOnce :: (MonadStream m) => StepName -> Stream a -> (a -> IO ()) -> m ()
atLeastOnce sn input f = void $ do
  run sn $
    StreamProcessor
      { streamProcessorInStream = input,
        streamProcessorWatermarkBy = NoWatermark,
        streamProcessorProcessBatch = (mapM (\(_, x) -> liftIO $ f x)),
        streamProcessorStreamStepConfig = defaultStreamStepConfig
      }

-- | Transforms the contents of a stream, outputting the results to a new Stream
-- stored in FoundationDB. Return 'Nothing' to filter the stream.
--
-- While this function is versatile, it should be used sparingly. Each
-- intermediate 'Stream' in FoundationDB has a significant cost in storage and
-- performance. Prefer to use 'fmap' and 'benignIO' on 'Stream's when possible,
-- as these functions register transformations that are applied lazily when the
-- stream is processed by downstream 'StreamStep's, which avoids persisting
-- uninteresting intermediate results to FoundationDB.
pipe ::
  (Message b, MonadStream m) =>
  StepName ->
  Stream a ->
  (a -> IO (Maybe b)) ->
  m (Stream b)
pipe sn input f =
  run sn $
    pipe' input f

-- | Like 'pipe', but returns a 'StreamStep' operation that can be further
-- customized before calling 'run' on it.
pipe' ::
  Message b =>
  Stream a ->
  (a -> IO (Maybe b)) ->
  StreamStep b (Stream b)
pipe' input f =
  StreamProcessor
    { streamProcessorInStream = input,
      streamProcessorWatermarkBy =
        if isStreamWatermarked input
          then DefaultWatermark
          else NoWatermark,
      streamProcessorProcessBatch = (\b -> catMaybes <$> for b (\(_, x) -> liftIO $ f x)),
      streamProcessorStreamStepConfig = defaultStreamStepConfig
    }

-- | Streaming one-to-one join. If the relationship is not actually one-to-one
--   (i.e. the input join functions are not injective), some messages in the
--   input streams could be lost.
--
-- If this step receives an item in one stream for a join key, but never receives
-- an item in the other stream, the received item will never be emitted downstream.
-- If this happens frequently, the space usage of this step in FoundationDB will
-- slowly increase. In the worst case, all of the input data for one Stream will
-- be stored in FoundationDB. In the future, we may allow this to be customized
-- to delete data that hasn't been joined within a time limit.
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

-- | Like 'oneToOneJoin', but returns a 'StreamStep', which can be further
-- customized before being passed to 'run'.
oneToOneJoin' ::
  (Message a, Message b, Message c, Message d) =>
  Stream a ->
  Stream b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  StreamStep d (Stream d)
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
        defaultStreamStepConfig

-- TODO: implement one-to-many joins in terms of this?

-- | Assigns each element of a stream to zero or more buckets. The contents of
-- these buckets can be monoidally reduced by 'aggregate' and 'aggregate''.
groupBy ::
  (v -> [k]) ->
  Stream v ->
  (GroupedBy k v)
groupBy k t = GroupedBy t k

-- | Given a grouped stream created by 'groupBy', convert all values in each
-- bucket into a monoidal value, then monoidally combine them. The result is
-- an 'AT.AggrTable' data structure in FoundationDB.
aggregate ::
  (Ord k, AT.TableKey k, AT.TableSemigroup aggr, MonadStream m) =>
  StepName ->
  GroupedBy k v ->
  (v -> aggr) ->
  m (AT.AggrTable k aggr)
aggregate sn groupedBy f =
  run sn $
    aggregate' groupedBy f

-- | Like 'aggregate', but returns a 'StreamStep' that must be passed to 'run'.
-- The 'StreamStep' can be passed to 'triggerBy' or 'watermarkBy' to further
-- customize its runtime behavior.
aggregate' ::
  (Ord k, AT.TableKey k, AT.TableSemigroup aggr) =>
  GroupedBy k v ->
  (v -> aggr) ->
  StreamStep (k, aggr) (AT.AggrTable k aggr)
aggregate' groupedBy@(GroupedBy input _) f =
  TableProcessor
    groupedBy
    f
    (if isStreamWatermarked input then DefaultWatermark else NoWatermark)
    Nothing
    defaultStreamStepConfig

class HasJobConfig m where
  getJobConfig :: m JobConfig

makeStream ::
  (Message b, HasJobConfig m, Monad m) =>
  StepName ->
  StreamName ->
  StreamStep b r ->
  m (Stream b)
makeStream stepName streamName step = do
  jc <- getJobConfig
  let streamTopic =
        makeTopic
          (jobConfigSS jc)
          stepName
          (getStepNumPartitions jc (getStepConfig step))
  return
    $ fmap fromMessage
    $ ( if stepProducesWatermark step
          then setStreamWatermarkByTopic streamTopic
          else id
      )
    $ streamFromTopic streamTopic streamName

-- | sets the watermark subspace to this stream to the watermark subspace of
-- the given topic. The given topic must be the topic in streamTopic, and
-- the step writing to the topic must actually have a watermarking function (or
-- default watermark) set. Don't export this; too confusing for users.
setStreamWatermarkByTopic :: Topic -> Stream a -> Stream a
setStreamWatermarkByTopic tc stream =
  stream {streamWatermarkSS = Just $ \_ -> topicWatermarkSS tc}

-- | Create a stream from a topic. Assumes the topic is not watermarked. If it
-- is and you want to propagate watermarks downstream, the caller must call
-- 'setStreamWatermarkByTopic' on the result.
--
-- You only need this function if you are trying to access a topic that was
-- created by another pipeline, or created manually.
streamFromTopic :: Topic -> StreamName -> Stream ByteString
streamFromTopic tc streamName =
  Stream
    { streamReadAndCheckpoint = \_cfg rn pid _chkptSS n _state -> do
        msgs <- readNAndCheckpoint' tc pid rn n
        return $ fmap (\(vs, x) -> (Just vs, x)) msgs,
      streamMinReaderPartitions = (fromIntegral $ Topic.numPartitions tc),
      streamWatermarkSS = Nothing,
      streamTopic = Just tc,
      streamName = streamName,
      setUpState = \_ _ _ _ -> return (),
      destroyState = const $ return ()
    }

forEachPartition :: Monad m => Stream a -> (PartitionId -> m ()) -> m ()
forEachPartition s = for_ [0 .. fromIntegral $ streamMinReaderPartitions s - 1]

-- | An implementation of the streaming system that uses distributed leases to
-- ensure mutual exclusion for each worker process. This reduces DB conflicts
-- and CPU contention, and increases throughput and scalability.
newtype LeaseBasedStreamWorker a
  = LeaseBasedStreamWorker
      {unLeaseBasedStreamWorker :: ReaderT (JobConfig, TaskRegistry) IO a}
  deriving (Functor, Applicative, Monad, MonadReader (JobConfig, TaskRegistry), MonadIO)

instance HasJobConfig LeaseBasedStreamWorker where
  getJobConfig = Reader.asks fst

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
  wasValidMaybe <- throttleByErrors metrics sn $ runStreamTxn db $
    stillValid >>= \case
      True -> do
        (msgsProcessed, w) <- action
        return (msgsProcessed, w, Just True)
      False -> do
        tid <- liftIO myThreadId
        logInfo (showText tid <> " lease no longer valid.")
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

outputStreamAndTopic ::
  (HasJobConfig m, Message c, Monad m) =>
  StepName ->
  StreamStep b (Stream c) ->
  m (Stream c, Topic)
outputStreamAndTopic stepName step = do
  jc <- getJobConfig
  let streamTopic =
        makeTopic
          (jobConfigSS jc)
          stepName
          (getStepNumPartitions jc (getStepConfig step))
  let stream =
        ( if stepProducesWatermark step
            then setStreamWatermarkByTopic streamTopic
            else id
        )
          $ streamFromTopic streamTopic stepName
  return (fmap fromMessage stream, streamTopic)

instance MonadStream LeaseBasedStreamWorker where
  run
    sn
    s@WriteOnlyProcessor
      { writeOnlyProduce,
        writeOnlyWatermarkBy,
        writeOnlyStreamStepConfig
      } = do
      (outStream, outTopic) <- outputStreamAndTopic sn s
      (cfg@JobConfig {jobConfigDB}, taskReg) <- Reader.ask
      metrics <- registerStepMetrics sn
      let job _stillValid _release =
            doForSeconds (leaseDuration cfg)
              $ void
              $ throttleByErrors metrics sn
              $ runStreamTxn jobConfigDB
              $ runCustomWatermark writeOnlyWatermarkBy
              $ produceStep (getMsgsPerBatch cfg writeOnlyStreamStepConfig) outTopic writeOnlyProduce
      liftIO
        $ runStreamTxn jobConfigDB
        $ addTask taskReg (TaskName sn) (leaseDuration cfg) job
      return outStream
  run
    sn
    s@StreamProcessor
      { streamProcessorInStream,
        streamProcessorWatermarkBy,
        streamProcessorProcessBatch,
        streamProcessorStreamStepConfig
      } = do
      (outStream, outTopic) <- outputStreamAndTopic sn s
      (cfg@JobConfig {jobConfigDB}, _) <- Reader.ask
      metrics <- registerStepMetrics sn
      let checkpointSS =
            streamConsumerCheckpointSS
              (jobConfigSS cfg)
              streamProcessorInStream
              sn
      let job pid _stillValid _release =
            -- NOTE: we must use a case here to do the pattern match to work around a
            -- GHC limitation. In 8.6, let gives a very confusing error message about
            -- variables escaping their scopes. In 8.8, it gives a more helpful
            -- "my brain just exploded" message that says to use a case statement.
            case streamProcessorInStream of
              Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
                bracket
                  (runStreamTxn jobConfigDB $ setUpState cfg sn pid checkpointSS)
                  destroyState
                  \state ->
                    doForSeconds (leaseDuration cfg)
                      $ void
                      $ throttleByErrors metrics sn
                      $ runStreamTxn jobConfigDB
                      $ runCustomWatermark streamProcessorWatermarkBy
                      $ pipeStep
                        cfg
                        streamProcessorStreamStepConfig
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
          $ runStreamTxn jobConfigDB
          $ addTask taskReg taskName (leaseDuration cfg) (job pid)
      return outStream
  run sn s@(Stream2Processor inl inr watermarker ls rs stepCfg) = do
    cfg@JobConfig {jobConfigDB} <- getJobConfig
    (outStream, outTopic) <- outputStreamAndTopic sn s
    metrics <- registerStepMetrics sn
    let lname = sn <> "0"
    let rname = sn <> "1"
    let checkpointSSL =
          streamConsumerCheckpointSS
            (jobConfigSS cfg)
            inl
            lname
    let checkpointSSR =
          streamConsumerCheckpointSS
            (jobConfigSS cfg)
            inr
            rname
    let ljob pid _stillValid _release =
          case inl of
            Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
              bracket
                (runStreamTxn jobConfigDB $ setUpState cfg lname pid checkpointSSL)
                destroyState
                \state ->
                  doForSeconds (leaseDuration cfg)
                    $ void
                    $ throttleByErrors metrics sn
                    $ runStreamTxn jobConfigDB
                    $ runCustomWatermark watermarker
                    $ pipeStep
                      cfg
                      stepCfg
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
              bracket
                (runStreamTxn jobConfigDB $ setUpState cfg rname pid checkpointSSR)
                destroyState
                \state ->
                  doForSeconds (leaseDuration cfg)
                    $ void
                    $ throttleByErrors metrics sn
                    $ runStreamTxn jobConfigDB
                    $ runCustomWatermark watermarker
                    $ pipeStep
                      cfg
                      stepCfg
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
        $ runStreamTxn jobConfigDB
        $ addTask taskReg lTaskName (leaseDuration cfg) (ljob pid)
    forEachPartition inr $ \pid -> do
      let rTaskName = TaskName $ BS8.pack (show rname ++ "_" ++ show pid)
      taskReg <- taskRegistry
      liftIO
        $ runStreamTxn jobConfigDB
        $ addTask taskReg rTaskName (leaseDuration cfg) (rjob pid)
    return outStream
  run
    sn
    step@TableProcessor
      { tableProcessorGroupedBy,
        tableProcessorAggregation,
        tableProcessorWatermarkBy,
        tableProcessorStreamStepConfig
      } = do
      cfg@JobConfig {jobConfigDB} <- getJobConfig
      let table =
            getAggrTable
              cfg
              sn
              (getStepNumPartitions cfg (getStepConfig step))
      let (GroupedBy inStream _) = tableProcessorGroupedBy
      metrics <- registerStepMetrics sn
      let checkpointSS =
            streamConsumerCheckpointSS
              (jobConfigSS cfg)
              inStream
              sn
      let job pid _stillValid _release =
            case inStream of
              Stream streamReadAndCheckpoint _ _ _ _ setUpState destroyState ->
                bracket
                  (runStreamTxn jobConfigDB $ setUpState cfg sn pid checkpointSS)
                  destroyState
                  \state ->
                    doForSeconds (leaseDuration cfg)
                      $ void
                      $ throttleByErrors metrics sn
                      $ runStreamTxn jobConfigDB
                      $ runCustomWatermark tableProcessorWatermarkBy
                      $ aggregateStep
                        cfg
                        tableProcessorStreamStepConfig
                        sn
                        tableProcessorGroupedBy
                        streamReadAndCheckpoint
                        state
                        tableProcessorAggregation
                        table
                        metrics
                        pid
      forEachPartition inStream $ \pid -> do
        taskReg <- taskRegistry
        let taskName = TaskName $ BS8.pack (show sn ++ "_" ++ show pid)
        liftIO
          $ runStreamTxn jobConfigDB
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

-- | Creates a reference to an existing aggregation table from outside of a
-- 'MonadStream' action. This is used in order to read from the aggregation
-- table from outside of the stream processing system.
--
-- NOTE: You must ensure that the number of partitions passed to this function
-- matches the number of partitions set on the 'StreamStep' that produces this
-- table (as set with 'withOutputPartitions'), or the default number of
-- partitions in the 'JobConfig' ('defaultNumPartitions') if the number
-- wasn't set on the 'StreamStep'. Failure to do so will result in 'AT.get'
-- returning incorrect results.
getAggrTable ::
  JobConfig ->
  StepName ->
  -- | Number of partitions. Controls max number of concurrent
  -- writers to the table.
  Word8 ->
  AT.AggrTable k v
getAggrTable sc sn n = AT.AggrTable {..}
  where
    aggrTableSS =
      FDB.extend (jobConfigSS sc) [C.topics, FDB.Bytes sn, C.aggrTable]
    aggrTableNumPartitions = n

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
            Error (MaxRetriesExceeded (CError TransactionTimedOut)) -> do
              logWarn (showText sn <> " timed out. If this keeps happening, try reducing the batch size.")
              incrTimeouts metrics
              threadDelay 15000
            CError TransactionTimedOut -> do
              logWarn (showText sn <> " timed out. If this keeps happening, try reducing the batch size.")
              incrTimeouts metrics
              threadDelay 15000
            e -> throw e
        ),
      Handler
        ( \case
            Error (MaxRetriesExceeded (CError NotCommitted)) -> do
              incrConflicts metrics
              logWarn (showText sn <> " conflicted with another transaction. If this happens frequently, it may be a bug in fdb-streaming.")
              threadDelay 15000
            e -> throw e
        ),
      Handler
        ( \(e :: SomeException) -> do
            tid <- myThreadId
            logError (showText sn <> " on thread " <> showText tid <> " caught " <> showText e)
        )
    ]
    $ do
      threadDelay 150
      t1 <- getTime Monotonic
      (numConsumed, _) <- x
      t2 <- getTime Monotonic
      let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
      recordBatchLatency metrics timeMillis
      when (numConsumed == 0) (threadDelay 1000000)

produceStep ::
  Message a =>
  Word16 ->
  Topic ->
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
  StreamStepConfig ->
  Stream a ->
  (StreamReadAndCheckpoint a state) ->
  state ->
  Topic ->
  StepName ->
  (Seq (Maybe (Versionstamp 'Complete), a) -> Transaction (Seq b)) ->
  Maybe StreamEdgeMetrics ->
  PartitionId ->
  Transaction (Int, Seq b)
pipeStep
  jobCfg
  stepCfg
  inStream
  readAndCheckpoint
  state
  outCfg
  sn
  transformBatch
  metrics
  pid = do
    let checkpointSS = streamConsumerCheckpointSS (jobConfigSS jobCfg) inStream sn
    inMsgs <-
      readAndCheckpoint
        jobCfg
        sn
        pid
        checkpointSS
        (getMsgsPerBatch jobCfg stepCfg)
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
  Topic ->
  -- | Index of the stream being consumed. 0 for left side of
  -- join, 1 for right. This will be refactored to support
  -- n-way joins in the future.
  Int ->
  (a1 -> c) ->
  (a1 -> a2 -> b) ->
  Seq (Maybe (Versionstamp 'Complete), a1) ->
  Transaction (Seq b)
oneToOneJoinStep outputTopic streamJoinIx pl combiner msgs = do
  -- Read from one of the two join streams, and for each
  -- message read, compute the join key. Using the join key, look in the
  -- join table to see if the partner is already there. If so, write the tuple
  -- downstream. If not, write the one message we do have to the join table.
  -- TODO: think of a way to garbage collect items that never get joined.
  let sn = "1:1"
  let joinSS = topicCustomMetadataSS outputTopic
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
  StreamStepConfig ->
  StepName ->
  GroupedBy k v ->
  StreamReadAndCheckpoint v state ->
  state ->
  (v -> aggr) ->
  AT.AggrTable k aggr ->
  Maybe StreamEdgeMetrics ->
  PartitionId ->
  FDB.Transaction (Int, Seq (k, aggr))
aggregateStep
  jobCfg
  stepCfg
  sn
  (GroupedBy inStream toKeys)
  streamReadAndCheckpoint
  state
  toAggr
  table
  metrics
  pid = do
    let checkpointSS = streamConsumerCheckpointSS (jobConfigSS jobCfg) inStream sn
    msgs <-
      fmap snd
        <$> streamReadAndCheckpoint
          jobCfg
          sn
          pid
          checkpointSS
          (getMsgsPerBatch jobCfg stepCfg)
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
        logError (showText ident <> " on thread " <> showText tid <> " caught " <> showText e)
    ]

-- | Runs a stream processing job forever. Blocks indefinitely.
runJob :: JobConfig -> (forall m. MonadStream m => m a) -> IO ()
runJob
  cfg@JobConfig {jobConfigDB, numStreamThreads, numPeriodicJobThreads, logLevel}
  topology = withGlobalLogging (LogConfig Nothing True) $ do
    setLogLevel logLevel
    logInfo "Starting main loop"
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

instance HasJobConfig PureStream where
  getJobConfig = Reader.ask

instance MonadStream PureStream where
  run sn s@WriteOnlyProcessor {} = makeStream sn sn s
  run sn s@StreamProcessor {} = makeStream sn sn s
  run sn s@Stream2Processor {} = makeStream sn sn s
  run sn s@TableProcessor {} = do
    cfg <- getJobConfig
    let table =
          getAggrTable
            cfg
            sn
            (getStepNumPartitions cfg (getStepConfig s))
    return table

-- | Extracts the output of a MonadStream topology without side effects.
-- This can be used to return handles to any streams and tables you want
-- to read from outside of the stream processing paradigm.
runPure :: JobConfig -> (forall m. MonadStream m => m a) -> a
runPure cfg x = Reader.runReader (unPureStream x) cfg

-- | If the step has been configured to use a custom number of partitions,
-- return it, otherwise return the job default.
getStepNumPartitions :: JobConfig -> StreamStepConfig -> Word8
getStepNumPartitions JobConfig {defaultNumPartitions} StreamStepConfig {stepOutputPartitions} =
  fromMaybe defaultNumPartitions stepOutputPartitions

-- | If the step has been configured to use a custom batch size, return it,
-- otherwise return the job default.
getMsgsPerBatch :: JobConfig -> StreamStepConfig -> Word16
getMsgsPerBatch JobConfig {msgsPerBatch} StreamStepConfig {stepBatchSize} =
  fromMaybe msgsPerBatch stepBatchSize
