{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ConstrainedClassMethods #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

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
--   second for the 'Topic' data structure, and perhaps millions for the 'AggrTable' structure.
--   See <https://apple.github.io/foundationdb/performance.html FoundationDB's docs>
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
-- >   -- benefit of avoiding the storage of uninteresting intermediate data in
-- >   -- FoundationDB, but
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
    listTopics,

    -- * Streams
    Stream,
    StreamPersisted (External, FDB),
    streamTopic,
    maybeStreamTopic,
    isStreamWatermarked,
    getStreamWatermark,
    StreamStep,
    Message (..),

    -- * Creating streams
    existing,
    existingWatermarked,
    atLeastOnce,
    pipe,
    oneToOneJoin,
    oneToManyJoin,

    -- * Indexing stream contents
    indexBy,
    Ix.Index,
    eventualIndexBy,

    -- * Tables
    AT.AggrTable,
    groupBy,
    GroupedBy (..),
    aggregate,
    benignIO,

    -- * Sinks
    Sink(..),

    -- * More complex usage
    run,
    watermarkBy,
    withBatchSize,
    withOutputPartitions,
    pipe',
    oneToOneJoin',
    oneToManyJoin',
    aggregate',

    -- * Advanced Usage
    topicWatermarkSS,
    WatermarkBy,
    streamFromTopic,
    getAggrTable,
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
import Control.Monad.Reader (MonadReader, ReaderT)
import qualified Control.Monad.Reader as Reader
import Control.Monad.State.Strict (MonadState, StateT)
import qualified Control.Monad.State.Strict as State
import qualified Data.ByteString.Char8 as BS8
import Data.Foldable (for_, toList)
import Data.Maybe (fromJust, fromMaybe, isJust)
import Data.Sequence (Seq ())
import qualified Data.Sequence as Seq
import Data.Text.Encoding (decodeUtf8)
import Data.Traversable (for)
import Data.Witherable.Class (catMaybes, witherM)
import Data.Word (Word16, Word64, Word8)
import qualified FDBStreaming.AggrTable as AT
import qualified FDBStreaming.Index as Ix
import FDBStreaming.JobConfig (JobConfig (JobConfig, defaultChunkSizeBytes, defaultNumPartitions, jobConfigDB, jobConfigSS, leaseDuration, logLevel, msgsPerBatch, numPeriodicJobThreads, numStreamThreads, streamMetricsStore, tasksToCleanUp), JobSubspace)
import FDBStreaming.Joins
  ( OneToOneJoinSS,
    delete1to1JoinData,
    get1to1JoinData,
    oneToOneJoinSS,
    write1to1JoinData,
  )
import qualified FDBStreaming.Joins.OneToMany as OTM
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.Stream.Internal
  ( Stream,
    Stream' (Stream'),
    StreamName,
    StreamPersisted (External, FDB),
    StreamReadAndCheckpoint,
    getStream',
    getStreamWatermark,
    isStreamWatermarked,
    maybeStreamTopic,
    putStream',
    setStreamWatermarkByTopic,
    streamConsumerCheckpointSS,
    streamFromTopic,
    streamMinReaderPartitions,
    streamTopic,
    streamWatermarkSS,
  )
import FDBStreaming.StreamStep.Internal
  ( BatchProcessor (IOBatchProcessor,
                    ioBatchInStream,
                    ioProcessBatch,
                    ioBatchProcessorName,
                    ioBatchNumPartitions,
                    IBatchProcessor,
                    iBatchInStream,
                    iProcessBatch,
                    iBatchNumPartitions,
                    iBatchProcessorName,
                    OBatchProcessor,
                    oProcessBatch,
                    oBatchNumPartitions,
                    oBatchProcessorName,
                    BatchProcessor,
                    processBatch),
    numBatchPartitions,
    getBatchProcessorName,
    --tableProcessorGroupedBy,
    --tableProcessorAggregation,
    --tableProcessorWatermarkBy,
    --tableProcessorTriggerBy,
    --tableProcessorStreamStepConfig,

    GroupedBy (GroupedBy),
    Indexer (Indexer),
    StepName,
    StreamStep
      ( IndexedStreamProcessor,
        StreamProcessor,
        TableProcessor, SinkProcessor, sinkProcessorWatermarkBy, sinkProcessorBatchProcessors, sinkProcessorStreamStepConfig
      ),
    StreamStepConfig (StreamStepConfig),
    batchProcessorHasInput,
    batchProcessorInputTopic,
    defaultStreamStepConfig,
    getStepConfig,
    groupedByInStream,
    indexBy,
    indexedStreamProcessorIndexBy,
    indexedStreamProcessorIndexName,
    indexedStreamProcessorInner,
    isBatchProcessorInputWatermarked,
    isStepDefaultWatermarked,
    stepBatchSize,
    stepOutputPartitions,
    stepProducesWatermark,
    streamProcessorBatchProcessors,
    streamProcessorStreamStepConfig,
    streamProcessorWatermarkBy,
    watermarkBy,
    withBatchSize,
    withOutputPartitions, ProcessBatchStats (ProcessBatchStats, processBatchStatsNumProcessed), stepBatchProcessors, Sink (Sink), StructureBeingIndexed
  )
import FDBStreaming.TaskLease (TaskName (TaskName), secondsSinceEpoch)
import FDBStreaming.TaskRegistry as TaskRegistry
  ( TaskRegistry,
    addTask,
    removeTask,
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
    writeTopic,
  )
import qualified FDBStreaming.Topic as Topic
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Util (logErrors)
import FDBStreaming.Watermark
  ( WatermarkBy (CustomWatermark, DefaultWatermark, NoWatermark),
    WatermarkSS,
    getWatermark,
    setWatermark,
    topicWatermarkSS,
  )
import FoundationDB (Transaction, await, withSnapshot)
import qualified FoundationDB as FDB
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
import Safe.Foldable (minimumMay, minimumNote)
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
    { -- NOTE: actually all stream transactions are idempotent, but retries are
      -- set to zero, so it's moot.
      FDB.idempotent = False,
      FDB.snapshotReads = False,
      -- No point in retrying since we are running in a loop
      -- anyway.
      maxRetries = 0,
      timeout = 5000,
      FDB.getConflictingKeys = False
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

-- | Returns the subspace in which the given stream's watermarks are stored.
watermarkSS :: JobSubspace -> Stream t a -> Maybe WatermarkSS
watermarkSS jobSS stream = case streamWatermarkSS stream of
  Nothing -> Nothing
  Just wmSS -> Just (wmSS jobSS)

-- | Registers an IO transformation to perform on each message if/when the
-- stream is consumed downstream. Return 'Nothing' to filter the stream. Side
-- effects here should be benign and relatively cheap -- they could be run many
-- times for the same input.
benignIO :: (a -> IO (Maybe b)) -> Stream t a -> Stream t b
benignIO g stream = case getStream' stream of
  (Stream' rc np wmSS streamName setUp destroy) -> putStream' stream
    $ build
    $ \cfg rn pid ss n state -> do
      xs <- rc cfg rn pid ss n state
      -- NOTE: functions like this must always be run after checkpointing, or
      -- we could filter out the last message we see and make no progress forever.
      flip witherM xs $ \(c, x) -> fmap (c,) <$> liftIO (g x)
    where
      build x = Stream' x np wmSS streamName setUp destroy

-- The reason we are using a tagless final interface like this in addition to
-- the record types, when it seems like we might only need one, is because we
-- want the benefits of both. The tagless final interface allows us to interpret
-- the DAG in multiple ways, including checking for errors, forward
-- compatibility checks, running with different strategies, etc. The record
-- interface allows us to use a builder style to modify our stream processors
-- "at a distance", with functions like 'watermarkBy'.

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
-- transaction (and making this StateT of Transaction, not of IO), but I hit
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
registerDefaultWatermarker cfg@JobConfig {jobConfigDB = db} taskReg x = do
  let job = snd $ State.execState (runDefaultWatermarker x) (cfg, return ())
  runStreamTxn db $ addTask taskReg "dfltWM" 1 \_ _ -> job

instance HasJobConfig DefaultWatermarker where
  getJobConfig = State.gets fst

-- | Append another watermark operation to the sequence of watermarking actions
-- that we intend to run when default watermarking.
watermarkNext :: Transaction () -> DefaultWatermarker ()
watermarkNext t = do
  JobConfig {jobConfigDB} <- getJobConfig
  State.modify \(c, t') -> (c, t' >> runStreamTxn jobConfigDB t)

-- | If it's possible to default watermark a StreamStep's output, return the
-- topics and the names of the batch processors reading from them, so that we
-- can watermark.
defaultWatermarkUpstreamTopics ::
  StepName ->
  StreamStep outMsg runResult ->
  Maybe [(Topic, StepName)]
defaultWatermarkUpstreamTopics sn streamStep =
  -- This checks two of the conditions that must be satisfied to apply a default
  -- watermark to a stream. Specifically, conditions 3 and 4:
  -- 3. Does the input have a watermark subspace?
  -- 4. Is the input a stream inside FoundationDB? If not, we can't get the
  --    checkpoint for this reader that corresponds to a point in the
  --    watermark function.
  let withInput = filter batchProcessorHasInput (stepBatchProcessors streamStep)
      topics = map batchProcessorInputTopic withInput
   in if all isBatchProcessorInputWatermarked withInput && all isJust topics
        then
          Just $
            zip
              (map fromJust topics)
              [sn <> BS8.pack (show i) | i <- [(0 :: Int) ..]]
        else Nothing

instance MonadStream DefaultWatermarker where
  run sn step@IndexedStreamProcessor{} =
    runIndexedStreamProcessorPure sn step
  run sn step@StreamProcessor {} = do
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
           defaultWatermarkUpstreamTopics sn step
         ) of
      (True, Just wmSS, Just xs) ->
        watermarkNext (defaultWatermark xs wmSS)
      _ -> return ()
    return output
  -- Must do this messy nested pattern match because pattern matching on
  -- existentials elsewhere can cause really confusing spurious type errors.
  -- https://gitlab.haskell.org/ghc/ghc/issues/15991
  run sn step@(TableProcessor (GroupedBy inStream _) _ _ _ _) = do
    cfg <- getJobConfig
    let table =
          getAggrTable
            cfg
            sn
            (getStepNumPartitions cfg (getStepConfig step))
    case ( isStepDefaultWatermarked step,
           AT.aggrTableWatermarkSS table,
           maybeStreamTopic inStream,
           streamWatermarkSS inStream
         ) of
      (True, wmSS, Just inTopic, Just _inWMSS) ->
        watermarkNext $
          defaultWatermark
            [(inTopic, sn)]
            wmSS
      _ -> return ()
    return table
  run sn step@SinkProcessor {} = do
    cfg <- getJobConfig
    case (isStepDefaultWatermarked step,
          sinkWatermarkSubspace cfg sn,
           defaultWatermarkUpstreamTopics sn step) of
      (True, wmSS, Just xs) -> do
        watermarkNext (defaultWatermark xs wmSS)
        return (Sink wmSS)
      (_, wmSS, _) -> return (Sink wmSS)

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
-- NOTE: since introducing chunking and the new 'Checkpoint' type, the watermark
-- can be slightly wrong, in the case where a reader is checkpointed halfway
-- through a chunk. For example, if vn are versionstamp keys, and bn are msgs in
-- a chunk, then our checkpoint might be v2,b2, but the root watermark is always
-- computed in terms of the last msg in the chunk, b3. To fix this, we subtract
-- 1 from the versionstamp, so that our watermark is guaranteed to be pointing
-- at the last completely-read chunk.
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
        ( parent,
          minimumNote
            "No checkpoints found for topic. Does it have zero partitions?"
            ckpts
        )
  minCheckpoints <- for minCheckpointsF await
  parentWMsF <- for minCheckpoints \(parent, Topic.Checkpoint vs _) ->
    getWatermark (topicWatermarkSS parent) $ transactionV $ conservativeVS vs
  parentsWMs <- for parentWMsF await
  let minParentWM = minimumMay (catMaybes parentsWMs)
  case minParentWM of
    Nothing -> return ()
    Just newWM -> setWatermark wmSS newWM
  where
    conservativeVS :: Versionstamp 'Complete -> Versionstamp 'Complete
    conservativeVS vs@(CompleteVersionstamp (TransactionVersionstamp 0 _) _) = vs
    conservativeVS (CompleteVersionstamp (TransactionVersionstamp v _) _) =
      CompleteVersionstamp (TransactionVersionstamp (v -1) maxBound) maxBound
    transactionV :: Versionstamp 'Complete -> Word64
    transactionV (CompleteVersionstamp (TransactionVersionstamp v _) _) = v

-- | Read messages from an existing Stream. You can use this to read from a
-- separate stream job that was defined at a distance -- in another codebase,
-- scope, etc.
--
-- If you want events downstream of this to have watermarks, set
-- 'isWatermarked' to @True@ on the result if you know that the existing Stream
-- has a watermark; this function doesn't have enough information to
-- determine that itself.
existing :: (Message a, MonadStream m) => Topic -> m (Stream 'FDB a)
existing tc =
  return
    $ fmap fromMessage
    $ streamFromTopic tc
    $ Topic.topicName tc

-- | Read messages from an existing stream which is known to be watermarked.
-- If the existing stream is not watermarked, watermarks will not be propagated
-- downstream from this stream.
existingWatermarked :: (Message a, MonadStream m) => Topic -> m (Stream 'FDB a)
existingWatermarked tc =
  fmap setStreamWatermarkByTopic (existing tc)

-- | Produce a side effect at least once for each message in the stream.
atLeastOnce :: (MonadStream m) => StepName -> Stream t a -> (a -> IO ()) -> m Sink
atLeastOnce sn input f =
  run sn $
    SinkProcessor
      { sinkProcessorWatermarkBy = DefaultWatermark,
        sinkProcessorBatchProcessors =
          [ IOBatchProcessor
              input
              (\_cfg _ss _pid xs -> do
                units <- mapM (liftIO . f . snd) xs
                return (ProcessBatchStats (Just $ length xs), units))
              (fromIntegral $ streamMinReaderPartitions input)
              "alo"
          ],
        sinkProcessorStreamStepConfig = defaultStreamStepConfig
      }

-- | Transforms the contents of a stream, outputting the results to a new Stream
-- stored in FoundationDB. Return 'Nothing' to filter the stream.
--
-- While this function is versatile, it should be used sparingly. Each
-- intermediate 'Stream' stored in FoundationDB has a significant cost in storage and
-- performance. Prefer to use 'fmap' and 'benignIO' on 'Stream's when possible,
-- as these functions register transformations that are applied lazily when the
-- stream is processed by downstream 'StreamStep's, which avoids persisting
-- uninteresting intermediate results to FoundationDB.
pipe ::
  (Message b, MonadStream m) =>
  StepName ->
  Stream t a ->
  (a -> IO (Maybe b)) ->
  m (Stream 'FDB b)
pipe sn input f =
  run sn $
    pipe' input f

-- | Like 'pipe', but returns a 'StreamStep' operation that can be further
-- customized before calling 'run' on it.
pipe' ::
  Message b =>
  Stream t a ->
  (a -> IO (Maybe b)) ->
  StreamStep b (Stream 'FDB b)
pipe' input f =
  StreamProcessor
    { streamProcessorWatermarkBy =
        if isStreamWatermarked input
          then DefaultWatermark
          else NoWatermark,
      streamProcessorBatchProcessors =
        [ IOBatchProcessor
            input
            (\_cfg _ss _pid xs -> do
              results <- catMaybes <$> mapM (liftIO . f . snd) xs
              return (ProcessBatchStats (Just $ length xs), results))
            (fromIntegral $ streamMinReaderPartitions input)
            "p"
        ],
      streamProcessorStreamStepConfig = defaultStreamStepConfig
    }

-- | Index the contents of the topic underlying the given input Stream. Has no
-- effect if the stream is not persisted to FoundationDB. This is an eventually
-- consistent indexing operation; it runs as a separate stage of the pipeline,
-- and is thus more suitable for more resource-intensive indexing operations.
--
-- Returns an 'Ix.Index' and a 'Sink' object representing this step. The sink
-- can be used to query the watermark of this step, which you can use to
-- determine how far behind real time the index is.
eventualIndexBy ::
  (AT.TableKey k, MonadStream m) =>
  StepName ->
  (a -> [k]) ->
  Stream 'FDB a ->
  m (Ix.Index k, Sink)
eventualIndexBy sn f stream = do
  let topic = streamTopic stream
  let ix = Ix.namedIndex topic sn
  let ixer _cfg _ss _pid batch = do
        for_ batch $ \case
          (Just coord, x) -> for_ (f x) \k -> do
            Ix.indexCommitted ix k coord
          (Nothing, _) -> return ()
        return (ProcessBatchStats (Just (length batch)), mempty)
  sink <- run sn $
    SinkProcessor
      { sinkProcessorWatermarkBy = DefaultWatermark,
        sinkProcessorBatchProcessors =
          [IOBatchProcessor
            stream
            ixer
            (fromIntegral $ streamMinReaderPartitions stream)
            "eix"],
        sinkProcessorStreamStepConfig = defaultStreamStepConfig
      }
  return (ix, sink)

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
  Stream t1 a ->
  Stream t2 b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  m (Stream 'FDB d)
oneToOneJoin sn in1 in2 p1 p2 j =
  run sn $
    oneToOneJoin' in1 in2 p1 p2 j

-- | Like 'oneToOneJoin', but returns a 'StreamStep', which can be further
-- customized before being passed to 'run'.
oneToOneJoin' ::
  (Message a, Message b, Message c, Message d) =>
  Stream t1 a ->
  Stream t2 b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  StreamStep d (Stream 'FDB d)
oneToOneJoin' inl inr pl pr j =
  let lstep = \_cfg workSS _pid -> oneToOneJoinStep (oneToOneJoinSS workSS) 0 pl j
      rstep = \_cfg workSS _pid -> oneToOneJoinStep (oneToOneJoinSS workSS) 1 pr (flip j)
   in StreamProcessor
        ( if isStreamWatermarked inl && isStreamWatermarked inr
            then DefaultWatermark
            else NoWatermark
        )
        [ IOBatchProcessor
           inl
           lstep
           (fromIntegral $ streamMinReaderPartitions inl)
           "1l",
          IOBatchProcessor
            inr
            rstep
            (fromIntegral $ streamMinReaderPartitions inr)
            "1r"
        ]
        defaultStreamStepConfig

-- Streaming one-to-many join. Each message in the left stream may be joined
-- to arbtrarily many messages in the right stream. However, each message in
-- the right stream may only be assigned to one message in the left stream.
-- In other words, the keying function for the left stream must be injective,
-- but the right keying function need not be injective.
oneToManyJoin ::
  (Message a, Message b, Message c, Message d, MonadStream m) =>
  StepName ->
  Stream t1 a ->
  Stream t2 b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  m (Stream 'FDB d)
oneToManyJoin sn inl inr pl pr j =
  run sn $ oneToManyJoin' inl inr pl pr j

-- Like 'oneToManyJoin', but returns a 'StreamStep', which can be further
-- customized before being passed to 'run'.
oneToManyJoin' ::
  (Message a, Message b, Message c, Message d) =>
  Stream t1 a ->
  Stream t2 b ->
  (a -> c) ->
  (b -> c) ->
  (a -> b -> d) ->
  StreamStep d (Stream 'FDB d)
oneToManyJoin' inl inr pl pr j =
  let watermarker =
        if isStreamWatermarked inl && isStreamWatermarked inr
          then DefaultWatermark
          else NoWatermark
      joinFn = (\l r -> return (j l r))
      lstep =
        IBatchProcessor
          { iBatchInStream = inl,
            iProcessBatch = \_cfg workSS pid lmsgs -> do
                             OTM.lMessageJob (OTM.oneToManyJoinSS workSS)
                                             pid
                                             pl
                                             (fmap snd lmsgs)
                             return (ProcessBatchStats (Just (length lmsgs))),
            iBatchNumPartitions = fromIntegral $ streamMinReaderPartitions inl,
            iBatchProcessorName = "otml"
          }
      rstep =
        IOBatchProcessor
          { ioBatchInStream = inr,
            ioProcessBatch = \_cfg workSS _pid rmsgs -> do
                             results <- OTM.rMessageJob
                                          (OTM.oneToManyJoinSS workSS)
                                          pr
                                          (fmap snd rmsgs)
                                          joinFn
                             return ( ProcessBatchStats (Just (length rmsgs))
                                    , results),
            ioBatchNumPartitions = fromIntegral $ streamMinReaderPartitions inr,
            ioBatchProcessorName = "otmr"
          }
      flushBacklog =
        OBatchProcessor
          { oProcessBatch = \cfg workSS pid -> do
                             results <- OTM.flushBacklogJob
                                          (OTM.oneToManyJoinSS workSS)
                                          pid
                                          joinFn
                                          (fromIntegral $ msgsPerBatch cfg)
                             -- Usually we want to return stats about the length
                             -- of the input to the batch processor, but in this
                             -- case, output is all we have (see flushBacklogJob)
                             -- for details.
                             return ( ProcessBatchStats (Just (length results))
                                    , results),
            oBatchNumPartitions = fromIntegral $ streamMinReaderPartitions inl,
            oBatchProcessorName = "otmf"
          }
   in StreamProcessor
        { streamProcessorWatermarkBy = watermarker,
          streamProcessorBatchProcessors =
            [ lstep,
              rstep,
              flushBacklog
            ],
          streamProcessorStreamStepConfig = defaultStreamStepConfig
        }

-- | Assigns each element of a stream to zero or more buckets. The contents of
-- these buckets can be monoidally reduced by 'aggregate' and 'aggregate''.
groupBy ::
  (v -> [k]) ->
  Stream t v ->
  GroupedBy k v
groupBy k t = GroupedBy t k

-- | Given a grouped stream created by 'groupBy', convert all values in each
-- bucket into a monoidal value, then monoidally combine them. The result is
-- an 'AT.AggrTable' data structure stored in FoundationDB.
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
  m (Stream 'FDB b)
makeStream stepName streamName step = do
  jc <- getJobConfig
  let topic =
        makeTopic
          (jobConfigSS jc)
          stepName
          (getStepNumPartitions jc (getStepConfig step))
          (defaultChunkSizeBytes jc)
  return
    $ fmap fromMessage
    $ ( if stepProducesWatermark step
          then setStreamWatermarkByTopic
          else id
      )
    $ streamFromTopic topic streamName

forEachPartition :: Applicative m => Stream t a -> (PartitionId -> m ()) -> m ()
forEachPartition s = for_ [0 .. fromIntegral $ streamMinReaderPartitions s - 1]

forEachPartition' :: (Applicative m, Integral a) => a -> (PartitionId -> m ()) -> m ()
forEachPartition' x = for_ [0 .. fromIntegral x - 1]

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
taskRegistry = Reader.asks snd

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

-- | Given the output messages of a O or IO batch processor, runs the user's
-- custom watermark function on the last message in the batch, then sets the
-- watermark to the output of the function.
--
-- If the batch is empty or the user has not specified a custom watermark
-- function, does nothing.
runCustomWatermark :: WatermarkSS -> WatermarkBy a -> Seq a -> Transaction ()
runCustomWatermark wmSS (CustomWatermark f) _outputMsgs@(_ Seq.:|> x) =
  f x >>= setWatermark wmSS
runCustomWatermark _ _ _ = return ()

runIndexers ::
  [Indexer outMsg] ->
  Topic ->
  Seq (outMsg, Topic.CoordinateUncommitted) ->
  Transaction ()
runIndexers ixers outTopic outMsgsWithCoords = do
  for_ ixers \(Indexer ixNm ixer) -> do
    let ix = Ix.namedIndex outTopic ixNm
    for_ outMsgsWithCoords \(msg, coord) -> do
      let ks = ixer msg
      for_ ks \k -> Ix.index ix k coord

outputStreamAndTopic ::
  (HasJobConfig m, Message c, Monad m) =>
  StepName ->
  StreamStep b (Stream 'FDB c) ->
  m (Stream 'FDB c, Topic)
outputStreamAndTopic stepName step = do
  jc <- getJobConfig
  let topic =
        makeTopic
          (jobConfigSS jc)
          stepName
          (getStepNumPartitions jc (getStepConfig step))
          (defaultChunkSizeBytes jc)
  let stream =
        ( if stepProducesWatermark step
            then setStreamWatermarkByTopic
            else id
        )
          $ streamFromTopic topic stepName
  return (fmap fromMessage stream, topic)

-- | Returns Nothing if the step doesn't produce a topic (e.g., it's a
-- TableProcessor).
outputTopic ::
  (HasJobConfig m, Monad m, StructureBeingIndexed runResult ~ Stream 'FDB b) =>
  StepName ->
  StreamStep b runResult ->
  m Topic
outputTopic stepName IndexedStreamProcessor {indexedStreamProcessorInner} =
  outputTopic stepName indexedStreamProcessorInner
outputTopic stepName step@StreamProcessor {} = do
  jc <- getJobConfig
  return
    $ makeTopic
      (jobConfigSS jc)
      stepName
      (getStepNumPartitions jc (getStepConfig step))
      (defaultChunkSizeBytes jc)
-- I thought that GHC would know this is impossible, but I get a non-exhaustive
-- pattern warning.
outputTopic _ _ = error "impossible case in outputTopic"

instance MonadStream LeaseBasedStreamWorker where
  run sn step@IndexedStreamProcessor {} =
    runIndexedStreamProcessorLeaseBased sn step
  run sn step@StreamProcessor {} =
    runStreamProcessorLeaseBased sn step []
  run
    sn
    step@( TableProcessor
             tableProcessorGroupedBy@(GroupedBy inStream _)
             tableProcessorAggregation
             tableProcessorWatermarkBy
             _
             tableProcessorStreamStepConfig
           ) =
      do
        cfg@JobConfig {jobConfigDB} <- getJobConfig
        let table =
              getAggrTable
                cfg
                sn
                (getStepNumPartitions cfg (getStepConfig step))
        metrics <- registerStepMetrics sn
        let checkpointSS =
              streamConsumerCheckpointSS
                (jobConfigSS cfg)
                inStream
                sn
        let job pid _stillValid _release =
              case getStream' inStream of
                Stream' streamReadAndCheckpoint _ _ _ setUpState destroyState ->
                  bracket
                    (runStreamTxn jobConfigDB $ setUpState cfg sn pid checkpointSS)
                    destroyState
                    \state ->
                      doForSeconds (leaseDuration cfg)
                        $ throttleByErrors metrics sn
                        $ runStreamTxn jobConfigDB do
                          (stats, aggs) <- aggregateStep
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
                          runCustomWatermark (AT.aggrTableWatermarkSS table)
                                             tableProcessorWatermarkBy
                                             aggs
                          return stats
        forEachPartition inStream $ \pid -> do
          taskReg <- taskRegistry
          let taskName = TaskName $ BS8.pack (show sn ++ "_tbl_" ++ show pid)
          liftIO
            $ runStreamTxn jobConfigDB
            $ addTask taskReg taskName (leaseDuration cfg) (job pid)
        return table
  run sn step@SinkProcessor {} =
    runSinkProcessorLeaseBased sn step

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

-- | Subspace storing any state needed to maintain a SinkProcessor StreamStep.
sinkSubspace ::
  JobConfig ->
  StepName ->
  FDB.Subspace
sinkSubspace cfg sn = FDB.extend (jobConfigSS cfg) [C.topics, FDB.Bytes sn, C.sink]

-- | Subspace storing watermark information for a sink (a stream step that
-- produces no output).
sinkWatermarkSubspace ::
  JobConfig ->
  StepName ->
  FDB.Subspace
sinkWatermarkSubspace cfg sn = FDB.extend (sinkSubspace cfg sn) [FDB.Bytes "wm"]

continuousTaskRegSS :: JobConfig -> FDB.Subspace
continuousTaskRegSS cfg = FDB.extend (jobConfigSS cfg) [FDB.Bytes "leasec"]

periodicTaskRegSS :: JobConfig -> FDB.Subspace
periodicTaskRegSS cfg = FDB.extend (jobConfigSS cfg) [FDB.Bytes "leasep"]

-- | The core loop body for every stream job. Throttles the job based on any
-- errors that occur, records timing metrics.
throttleByErrors ::
  Maybe StreamEdgeMetrics ->
  StepName ->
  IO ProcessBatchStats ->
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
            Error (MaxRetriesExceeded (CError (NotCommitted range))) -> do
              incrConflicts metrics
              logWarn (showText sn <> " conflicted with another transaction. If this happens frequently, it may be a bug in fdb-streaming. If this step is a join, occasional conflicts are expected and shouldn't impact performance. Conflict range: " <> showText range)
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
      ProcessBatchStats{processBatchStatsNumProcessed} <- x
      t2 <- getTime Monotonic
      let timeMillis = (`div` 1000000) $ toNanoSecs $ diffTimeSpec t2 t1
      recordBatchLatency metrics timeMillis
      case processBatchStatsNumProcessed of
        Just 0 -> threadDelay 1000000
        _      -> return ()

-- | Utility function to record metrics for an input batch of messages.
-- Returns the sequence of input messages unchanged.
recordInputBatchMetrics ::
  MonadIO m =>
  Maybe StreamEdgeMetrics ->
  Seq (Maybe Topic.Coordinate, a) ->
  m ()
recordInputBatchMetrics metrics inMsgs = do
  liftIO $ when (Seq.null inMsgs) (incrEmptyBatchCount metrics)
  liftIO $ recordMsgsPerBatch metrics (Seq.length inMsgs)

-- | Utility function to write output messages to a random partition of
-- the given topic. Returns the uncommitted coordinates the messages will be
-- written to once the transaction commits. These coordinates can be used to
-- atomically write secondary indices.
writeToRandomPartition ::
  Message b =>
  Topic ->
  Seq b ->
  Transaction (Seq Topic.CoordinateUncommitted)
writeToRandomPartition outTopic outputMsgs = do
  let outMsgs = fmap toMessage outputMsgs
  p' <- liftIO $ randPartition outTopic
  coords <- writeTopic outTopic p' (toList outMsgs)
  return (Seq.fromList coords)

oneToOneJoinStep ::
  forall a1 a2 b c.
  (Message a1, Message a2, Message c) =>
  OneToOneJoinSS ->
  -- | Index of the stream being consumed. 0 for left side of
  -- join, 1 for right. This will be refactored to support
  -- n-way joins in the future.
  Int ->
  (a1 -> c) ->
  (a1 -> a2 -> b) ->
  Seq (Maybe Topic.Coordinate, a1) ->
  Transaction (ProcessBatchStats, Seq b)
oneToOneJoinStep joinSS streamJoinIx pl combiner ckptmsgs = do
  -- Read from one of the two join streams, and for each
  -- message read, compute the join key. Using the join key, look in the
  -- join table to see if the partner is already there. If so, write the tuple
  -- downstream. If not, write the one message we do have to the join table.
  -- TODO: think of a way to garbage collect items that never get joined.
  let otherIx = if streamJoinIx == 0 then 1 else 0
  joinFutures <-
    for ckptmsgs \(_, msg) -> do
      let k = pl msg
      -- TODO: this algorithm can cause conflicts if both sides of the join are
      -- processed at exactly the same time. We originally used a snapshot read
      -- here, but that was incorrect -- if both messages for a key are
      -- processed simultaneously, they miss each other and get stuck forever.
      -- It's possible that conflicts aren't a big problem -- perhaps it will reach an
      -- equilibrium where one side of the join is a little behind the other.
      -- The other alternative is to add a partitionByKey operation before the
      -- join step. It would hash the key of each message, modulate that hash
      -- and use it to choose which partition to write the message downstream
      -- to. This would ensure that all messages that will be joined together
      -- are thrown in the same partition. However, that could easily be slower
      -- than occasional conflicts -- we'd need to write every message to FDB
      -- again. OTOH, there is a lot of opportunity for avoiding a lot of writes
      -- in joins if we do that -- we could even conceivably hold join state in
      -- the memory of the join processor, or send messages by some other
      -- mechanism than FDB.
      joinF <- get1to1JoinData joinSS otherIx k
      return (k, msg, joinF)
  joinData <- for joinFutures \(k, msg, joinF) -> do
    d <- await joinF
    return (k, msg, d)
  results <- catMaybes <$> for joinData \(k, lmsg, d) -> do
    case d of
      Just (rmsg :: a2) -> do
        delete1to1JoinData joinSS k
        return $ Just $ combiner lmsg rmsg
      Nothing -> do
        write1to1JoinData joinSS k streamJoinIx (lmsg :: a1)
        return Nothing
  return (ProcessBatchStats (Just (length ckptmsgs)), results)

-- TODO: replace this helper function with a BatchProcessor in the
-- TableProcessor constructor
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
  FDB.Transaction (ProcessBatchStats, Seq (k, aggr))
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
    -- TODO: consolidate these metrics (probably in throttleByErrors, which
    -- should be renamed)
    liftIO $ when (Seq.null msgs) (incrEmptyBatchCount metrics)
    liftIO $ recordMsgsPerBatch metrics (Seq.length msgs)
    let kvs = Seq.fromList [(k, toAggr v) | v <- toList msgs, k <- toKeys v]
    AT.mappendBatch table pid kvs
    return (ProcessBatchStats (Just (length msgs)), kvs)

-- | Runs a stream processing job forever. Blocks indefinitely.
runJob :: JobConfig -> (forall m. MonadStream m => m a) -> IO ()
runJob
  cfg@JobConfig {jobConfigDB, numStreamThreads, numPeriodicJobThreads, logLevel
                , tasksToCleanUp}
  topology = withGlobalLogging (LogConfig Nothing True) $ do
    setLogLevel logLevel
    logInfo "Starting main loop"
    -- Run threads for continuously-running stream steps
    (_pureResult, continuousTaskReg) <- registerContinuousLeases cfg topology
    FDB.runTransaction jobConfigDB
      $ for_ tasksToCleanUp $ \t -> removeTask continuousTaskReg (TaskName t)
    continuousThreads <-
      replicateM numStreamThreads
        $ async
        $ forever
        $ logErrors "Continuous job"
        $ runRandomTask jobConfigDB continuousTaskReg >>= \case
          False -> threadDelay (leaseDuration cfg * 1000000 `div` 2)
          True -> return ()
    -- Run threads for periodic jobs (watermarking, cleanup, etc)
    periodicTaskReg <- TaskRegistry.empty (periodicTaskRegSS cfg)
    FDB.runTransaction jobConfigDB
      $ for_ tasksToCleanUp $ \t -> removeTask periodicTaskReg (TaskName t)
    registerDefaultWatermarker cfg periodicTaskReg topology
    periodicThreads <-
      replicateM numPeriodicJobThreads
        $ async
        $ forever
        $ logErrors "Periodic job"
        $ runRandomTask jobConfigDB periodicTaskReg >>= \case
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
  run sn step@IndexedStreamProcessor {} =
    runIndexedStreamProcessorPure sn step
  run sn s@StreamProcessor {} = makeStream sn sn s
  run sn s@TableProcessor {} = do
    cfg <- getJobConfig
    let table =
          getAggrTable
            cfg
            sn
            (getStepNumPartitions cfg (getStepConfig s))
    return table
  run sn SinkProcessor {} = do
    cfg <- getJobConfig
    return (Sink (sinkWatermarkSubspace cfg sn))

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

newtype ListTopics a = ListTopics {unListTopics :: ReaderT JobConfig (StateT [Topic] Identity) a}
  deriving (Functor, Applicative, Monad, MonadReader JobConfig, MonadState [Topic])

instance HasJobConfig ListTopics where
  getJobConfig = Reader.ask

instance MonadStream ListTopics where
  run sn step@IndexedStreamProcessor {} = do
    t <- outputTopic sn step
    State.modify (t :)
    runIndexedStreamProcessorPure sn step
  run sn step@StreamProcessor {} = do
    t <- outputTopic sn step
    State.modify (t :)
    makeStream sn sn step
  run sn step@TableProcessor {} = do
    cfg <- getJobConfig
    let table =
          getAggrTable
            cfg
            sn
            (getStepNumPartitions cfg (getStepConfig step))
    return table
  run sn SinkProcessor {} = do
    cfg <- getJobConfig
    return (Sink (sinkWatermarkSubspace cfg sn))

-- | List all topics written to by the given stream topology.
listTopics :: JobConfig -> (forall m. MonadStream m => m a) -> [Topic]
listTopics cfg f =
  State.execState (Reader.runReaderT (unListTopics f) cfg) []

-- | Run function for 'IndexedStreamProcessor'. If the input is not
-- an 'IndexedStreamProcessor', fails.
--
-- Internally, this function collects all the indexers the user has set for
-- this stream step into a list, then runs the step by calling
-- 'runStreamProcessorLeaseBased' on the step and its indexers. The purpose of
-- the function is to "forget" the type information that is remembered
-- by the 'IndexedStreamProcessor' constructor.
runIndexedStreamProcessorLeaseBased :: StepName
                                    -> StreamStep outMsg runResult
                                    -> LeaseBasedStreamWorker runResult
runIndexedStreamProcessorLeaseBased
  sn
  step@IndexedStreamProcessor {} = go step []

    where
      go :: StreamStep outMsg runResult -> [Indexer outMsg] -> LeaseBasedStreamWorker runResult
      go
        step'@IndexedStreamProcessor { indexedStreamProcessorInner
                                     , indexedStreamProcessorIndexName
                                     , indexedStreamProcessorIndexBy
                                     }
        indexers = do
          result <- go indexedStreamProcessorInner
                       (Indexer
                          indexedStreamProcessorIndexName
                          indexedStreamProcessorIndexBy
                        : indexers)
          topic <- outputTopic sn step'
          return (Ix.namedIndex topic indexedStreamProcessorIndexName, result)
      go step'@StreamProcessor {} indexers =
        runStreamProcessorLeaseBased sn step' indexers
      go TableProcessor{} _ = error "impossible case 2"
      go SinkProcessor{} _ = error "impossible case 3"
runIndexedStreamProcessorLeaseBased _ _ = error "runIndexedStreamProcessorLeaseBased called on wrong constructor"

-- | run the indexed stream processor in monads where we don't need indexing
-- to actually be run, such as when we are just trying to analyze the DAG.
-- This means every MonadStream except for 'LeasebasedStreamWorker'.
runIndexedStreamProcessorPure :: (HasJobConfig m, MonadStream m)
                              => StepName
                              -> StreamStep outMsg runResult
                              -> m runResult
runIndexedStreamProcessorPure
  sn
  step@IndexedStreamProcessor {} = go step

    where
      go :: (HasJobConfig m, MonadStream m) => StreamStep outMsg runResult -> m runResult
      go
        step'@IndexedStreamProcessor { indexedStreamProcessorInner
                                     , indexedStreamProcessorIndexName
                                     } = do
          result <- go indexedStreamProcessorInner
          topic <- outputTopic sn step'
          return (Ix.namedIndex topic indexedStreamProcessorIndexName, result)
      go step'@StreamProcessor {} =
        run sn step'
      go TableProcessor{} = error "impossible case 2"
      go SinkProcessor{} = error "impossible case 3"
runIndexedStreamProcessorPure _ _ = error "runIndexedStreamProcessorPure called on wrong constructor"

runStreamProcessorLeaseBased :: StepName
                             -> StreamStep outMsg runResult
                             -> [Indexer outMsg]
                             -> LeaseBasedStreamWorker runResult
runStreamProcessorLeaseBased
  sn
  s@StreamProcessor
    { streamProcessorWatermarkBy,
      streamProcessorBatchProcessors,
      streamProcessorStreamStepConfig
    }
  indexers = do
    (outStream, outTopic) <- outputStreamAndTopic sn s
    (cfg@JobConfig {jobConfigDB, jobConfigSS, leaseDuration}, _) <- Reader.ask
    metrics <- registerStepMetrics sn
    let msgsPerBatch = getMsgsPerBatch cfg streamProcessorStreamStepConfig
    let workspaceSS =
          FDB.extend
            (topicCustomMetadataSS outTopic)
            [C.streamStepWorkspace]
    let processorsWStepNames =
          zipWith
            (\bp i ->
              (bp
              , sn <> BS8.pack (show i) <> "_" <> getBatchProcessorName bp))
            streamProcessorBatchProcessors
            [(0 :: Int) ..]
    for_ processorsWStepNames \(batchProcessor, sn_i) -> do
      let job pid _stillValid _release =
            case batchProcessor of
              IOBatchProcessor{ioBatchInStream, ioProcessBatch} ->
                -- NOTE: we must use a case here to do the pattern match to
                -- work around a GHC limitation. In 8.6, let gives a very
                -- confusing error message about variables escaping their
                -- scopes. In 8.8, it gives a more helpful "my brain just
                -- exploded" message that says to use a case statement.
                case getStream' ioBatchInStream of
                  (Stream' streamReadAndCheckpoint _ _ _ setUpState destroyState) -> do
                    -- NOTE: there is repetition of the bracket, doForSeconds,
                    -- and throttleByErrors, but the existentials make
                    -- DRYing it out difficult.
                    bracket
                      (runStreamTxn jobConfigDB $ setUpState cfg sn_i pid checkpointSS)
                      destroyState
                      $ \state -> doForSeconds leaseDuration
                      $ throttleByErrors metrics sn_i
                      $ runStreamTxn jobConfigDB
                      $ do
                        inMsgs <- streamReadAndCheckpoint
                                  cfg
                                  sn_i
                                  pid
                                  checkpointSS
                                  msgsPerBatch
                                  state
                        recordInputBatchMetrics metrics inMsgs
                        (stats, outputMsgs) <- ioProcessBatch cfg workspaceSS pid inMsgs
                        coordsUncommitted <- writeToRandomPartition outTopic outputMsgs
                        let outMsgsWithCoords = Seq.zip outputMsgs coordsUncommitted
                        runIndexers indexers outTopic outMsgsWithCoords
                        runCustomWatermark (topicWatermarkSS outTopic) streamProcessorWatermarkBy outputMsgs
                        return stats
                    where
                      -- TODO: DRY out
                      checkpointSS =
                        streamConsumerCheckpointSS
                          jobConfigSS
                          ioBatchInStream
                          sn_i
              IBatchProcessor{iBatchInStream, iProcessBatch} ->
                case getStream' iBatchInStream of
                  (Stream' streamReadAndCheckpoint _ _ _ setUpState destroyState) -> do
                    bracket
                      (runStreamTxn jobConfigDB $ setUpState cfg sn_i pid checkpointSS)
                      destroyState
                      $ \state -> doForSeconds leaseDuration
                      $ throttleByErrors metrics sn_i
                      $ runStreamTxn jobConfigDB
                      $ do
                        inMsgs <- streamReadAndCheckpoint
                                  cfg
                                  sn_i
                                  pid
                                  checkpointSS
                                  msgsPerBatch
                                  state
                        recordInputBatchMetrics metrics inMsgs
                        iProcessBatch cfg workspaceSS pid inMsgs
                    where
                      checkpointSS =
                        streamConsumerCheckpointSS
                          jobConfigSS
                          iBatchInStream
                          sn_i
              OBatchProcessor{oProcessBatch} ->
                doForSeconds leaseDuration
                        $ throttleByErrors metrics sn_i
                        $ runStreamTxn jobConfigDB do
                          (stats, outputMsgs) <- oProcessBatch cfg workspaceSS pid
                          coordsUncommitted <- writeToRandomPartition outTopic outputMsgs
                          runCustomWatermark (topicWatermarkSS outTopic) streamProcessorWatermarkBy outputMsgs
                          runIndexers indexers outTopic (Seq.zip outputMsgs coordsUncommitted)
                          return stats
              BatchProcessor{processBatch} ->
                doForSeconds leaseDuration
                        $ void
                        $ throttleByErrors metrics sn_i
                        $ runStreamTxn jobConfigDB
                        $ processBatch cfg workspaceSS pid

      forEachPartition' (numBatchPartitions batchProcessor) $ \pid -> do
        let taskName = mkTaskName sn_i pid
        taskReg <- taskRegistry
        liftIO
          $ runStreamTxn jobConfigDB
          $ addTask taskReg taskName leaseDuration (job pid)
    return outStream

-- It's not worth making this case impossible because this function is internal
-- to this namespace and used merely to DRY out a few cases.
runStreamProcessorLeaseBased
  _ _ _ = error "runStreamProcessorLeaseBased called on non-StreamProcessor"

runSinkProcessorLeaseBased :: StepName
                           -> StreamStep () Sink
                           -> LeaseBasedStreamWorker Sink
runSinkProcessorLeaseBased
  sn
  SinkProcessor { sinkProcessorWatermarkBy
                , sinkProcessorBatchProcessors
                , sinkProcessorStreamStepConfig
                } = do
    (cfg@JobConfig {jobConfigDB, jobConfigSS, leaseDuration}, _) <- Reader.ask
    metrics <- registerStepMetrics sn
    let msgsPerBatch = getMsgsPerBatch cfg sinkProcessorStreamStepConfig
    let workspaceSS =
          FDB.extend
            (sinkSubspace cfg sn)
            [C.streamStepWorkspace]
    let wmSS = sinkWatermarkSubspace cfg sn
    let processorsWStepNames =
          zipWith
            (\bp i ->
              (bp
              , sn <> BS8.pack (show i) <> "_" <> getBatchProcessorName bp))
            sinkProcessorBatchProcessors
            [(0 :: Int) ..]
    --TODO: most of this is copy/pasted from runStreamProcessorLeaseBased, but
    -- I don't yet see a way to avoid the repetition, because there are enough
    -- differences that it's somewhat tricky to DRY out.
    for_ processorsWStepNames \(batchProcessor, sn_i) -> do
      let job pid _stillValid _release =
            case batchProcessor of
              IOBatchProcessor{ioBatchInStream, ioProcessBatch} ->
                -- NOTE: we must use a case here to do the pattern match to
                -- work around a GHC limitation. In 8.6, let gives a very
                -- confusing error message about variables escaping their
                -- scopes. In 8.8, it gives a more helpful "my brain just
                -- exploded" message that says to use a case statement.
                case getStream' ioBatchInStream of
                  (Stream' streamReadAndCheckpoint _ _ _ setUpState destroyState) -> do
                    -- NOTE: there is repetition of the bracket, doForSeconds,
                    -- and throttleByErrors, but the existentials make
                    -- DRYing it out difficult.
                    bracket
                      (runStreamTxn jobConfigDB $ setUpState cfg sn_i pid checkpointSS)
                      destroyState
                      $ \state -> doForSeconds leaseDuration
                      $ throttleByErrors metrics sn_i
                      $ runStreamTxn jobConfigDB
                      $ do
                        inMsgs <- streamReadAndCheckpoint
                                  cfg
                                  sn_i
                                  pid
                                  checkpointSS
                                  msgsPerBatch
                                  state
                        recordInputBatchMetrics metrics inMsgs
                        (stats, outputMsgs) <- ioProcessBatch cfg workspaceSS pid inMsgs
                        -- custom watermark support might not actually be needed
                        -- for sink steps -- the custom watermark is always going
                        -- to be given () as input, so it's not really useful.
                        runCustomWatermark wmSS sinkProcessorWatermarkBy outputMsgs
                        return stats
                    where
                      -- TODO: DRY out
                      checkpointSS =
                        streamConsumerCheckpointSS
                          jobConfigSS
                          ioBatchInStream
                          sn_i
              -- Crude way to reduce needless repetition. Users can't create
              -- sink steps themselves, so these error should be impossible
              -- cases.
              IBatchProcessor{} ->
                error "impossible: sink steps don't support IBatchProcessor"
              OBatchProcessor{} ->
                error "impossible: sink steps don't support OBatchProcessor"
              BatchProcessor{} ->
                error "impossible: sink steps don't support BatchProcessor"
      forEachPartition' (numBatchPartitions batchProcessor) $ \pid -> do
        let taskName = mkTaskName sn_i pid
        taskReg <- taskRegistry
        liftIO
          $ runStreamTxn jobConfigDB
          $ addTask taskReg taskName leaseDuration (job pid)
    return (Sink (sinkWatermarkSubspace cfg sn))
