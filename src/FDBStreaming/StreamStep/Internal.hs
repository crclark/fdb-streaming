{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -fno-warn-missing-export-lists #-}

-- Internal module; export everything!

module FDBStreaming.StreamStep.Internal where

import Data.ByteString (ByteString)
import Data.Sequence (Seq)
import Data.Word (Word16, Word8)
import qualified FDBStreaming.AggrTable as AT
import FDBStreaming.Index (Index, IndexName)
import FDBStreaming.JobConfig (JobSubspace)
import FDBStreaming.Message (Message)
import FDBStreaming.Stream.Internal (Stream, StreamPersisted (FDB), maybeStreamTopic, streamWatermarkSS)
import FDBStreaming.Topic (Coordinate, Topic, PartitionId)
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkBy (CustomWatermark),
    WatermarkSS,
    isDefaultWatermark,
    producesWatermark,
  )
import FoundationDB (Transaction)
import FoundationDB.Layer.Subspace (Subspace)

-- | The name of a step in the pipeline. A step can be thought of as a function
-- that consumes a stream and writes to a stream, a table, or a side effect.
type StepName = ByteString

data GroupedBy k v
  = -- TODO: we could improve type safety by parametrizing by t, but not sure if
    -- it would cause problems. Seems like GroupedBy shouldn't care about where
    -- the stream is.
    -- NOTE: do not export this constructor to users; it's too easy to hit
    -- GHC bug: https://gitlab.haskell.org/ghc/ghc/issues/15991
    forall t.
    GroupedBy
      { groupedByInStream :: Stream t v,
        groupedByFunction :: v -> [k]
      }

data StreamStepConfig
  = StreamStepConfig
      { -- | Specifies how many partitions the output stream or table should have.
        -- This determines how many threads downstream steps will need in order to
        -- consume the output. This overrides the default value specified in
        -- 'JobConfig'.
        stepOutputPartitions :: Maybe Word8,
        stepBatchSize :: Maybe Word16
      }
  deriving (Show, Eq)

defaultStreamStepConfig :: StreamStepConfig
defaultStreamStepConfig = StreamStepConfig Nothing Nothing

-- TODO: instead of the Maybe in batchInStream, we should really have two
-- constructors for BatchProcessor.

-- | Contains all the information we need to pull data out of a stream and
-- transform it. Uses an existential so that we can more easily implement
-- joins and other operations that pull from multiple streams. This type is an
-- internal detail of StreamStep, and not very useful on its own.
--
-- If the input stream is a topic with n partitions, up to n instances of this
-- processor will be running on n threads simultaneously.
data BatchProcessor b
  = -- NOTE: do not export this constructor to users; it's too easy to hit
    -- GHC bug: https://gitlab.haskell.org/ghc/ghc/issues/15991
    forall a r.
    IOBatchProcessor
      { -- | The input stream that this processor reads from.
        ioBatchInStream :: Stream r a,
        -- | A batch processing function takes a batch of messages as input, along
        -- with a subspace in which it can store any state it needs to persist to
        -- operate or communicate. This subspace is shared among all batch
        -- processors in the same 'StreamStep', which allows heterogeneous workers
        -- within a step to communicate. For example, this is used to store
        -- temporary data for joins -- one BatchProcessor is responsible for the
        -- left side of the join, the other is responsible for the right side.
        ioProcessBatch :: Subspace
                       -> PartitionId
                       -> Seq (Maybe Coordinate, a)
                       -> Transaction (Seq b),
        -- | Number of distinct partitions (threads) that this processor should
        -- run on concurrently.
        ioBatchNumPartitions :: Word8,
        -- | Name of this processor. Must be unique within a StreamStep. This
        -- name will be appended to the task name in the TaskRegistry, easing
        -- debugging of the task assignment system.
        ioBatchProcessorName :: ByteString
      }
    -- | A batch processing function that reads input but has no output.
    | forall a r. IBatchProcessor
        {
          iBatchInStream :: Stream r a,
          iProcessBatch :: Subspace
                        -> PartitionId
                        -> Seq (Maybe Coordinate, a)
                        -> Transaction (),
          iBatchNumPartitions :: Word8,
          iBatchProcessorName :: ByteString
        }
    -- | A batch processing function that does not read from an input stream but
    -- produces output. This sounds weird, but it can be useful in more complex
    -- use cases, such as streaming joins, where multiple IBatchProcessors
    -- receive input and store them in the step's private subspace until all of
    -- the data needed for the join has been received. Then an OBatchProcessor
    -- detects that all the data has been received, and flushes the data
    -- downstream. See the OneToMany join namespace for more details.
    | OBatchProcessor
        {
          oProcessBatch :: Subspace
                        -> PartitionId
                        -> Transaction (Seq b),
          oBatchNumPartitions :: Word8,
          oBatchProcessorName :: ByteString
        }
    -- | A batch processor that is run purely for side effects. No input stream,
    -- no output stream.
    | BatchProcessor
        {
          processBatch :: Subspace
                       -> PartitionId
                       -> Transaction (),
          batchNumPartitions :: Word8,
          batchProcessorName :: ByteString
        }

-- | Datatype describing the outputs of batchProcessorInputWatermarkSS.
data BatchProcessorInputWatermarkSS =
  BatchProcessorHasNoInput
  | BatchProcessorInputNotWatermarked
  | BatchProcessorInputWatermarkSS (JobSubspace -> WatermarkSS)

batchProcessorHasInput :: BatchProcessor a -> Bool
batchProcessorHasInput IOBatchProcessor{} = True
batchProcessorHasInput IBatchProcessor{} = True
batchProcessorHasInput _ = False

isBatchProcessorInputWatermarked :: BatchProcessor a -> Bool
isBatchProcessorInputWatermarked x = case batchProcessorInputWatermarkSS x of
  BatchProcessorInputWatermarkSS _ -> True
  _ -> False

-- | Returns the watermarkSS function from the underlying stream of the given
-- batch processor. There are three cases: the batch processor has no input,
-- or the batch procesor input is not watermarked, or the input is watermarked,
-- in which case we return the function that creates the WatermarkSS for that
-- input.
batchProcessorInputWatermarkSS
  :: BatchProcessor b
  -> BatchProcessorInputWatermarkSS
batchProcessorInputWatermarkSS IOBatchProcessor{ioBatchInStream} =
  maybe
    BatchProcessorInputNotWatermarked
    BatchProcessorInputWatermarkSS (streamWatermarkSS ioBatchInStream)
batchProcessorInputWatermarkSS IBatchProcessor{iBatchInStream} =
  maybe
    BatchProcessorInputNotWatermarked
    BatchProcessorInputWatermarkSS (streamWatermarkSS iBatchInStream)
batchProcessorInputWatermarkSS _ = BatchProcessorHasNoInput

-- | Returns a topic if this batch processor has an input stream, and that
-- input stream is persisted in FDB.
batchProcessorInputTopic :: BatchProcessor b -> Maybe Topic
batchProcessorInputTopic IOBatchProcessor{ioBatchInStream} =
  maybeStreamTopic ioBatchInStream
batchProcessorInputTopic IBatchProcessor{iBatchInStream} =
  maybeStreamTopic iBatchInStream
batchProcessorInputTopic _ = Nothing

numBatchPartitions :: BatchProcessor b -> Word8
numBatchPartitions IOBatchProcessor{ioBatchNumPartitions} = ioBatchNumPartitions
numBatchPartitions IBatchProcessor{iBatchNumPartitions} = iBatchNumPartitions
numBatchPartitions OBatchProcessor{oBatchNumPartitions} = oBatchNumPartitions
numBatchPartitions BatchProcessor{batchNumPartitions} = batchNumPartitions

getBatchProcessorName :: BatchProcessor b -> ByteString
getBatchProcessorName IOBatchProcessor{ioBatchProcessorName} = ioBatchProcessorName
getBatchProcessorName IBatchProcessor{iBatchProcessorName} = iBatchProcessorName
getBatchProcessorName OBatchProcessor{oBatchProcessorName} = oBatchProcessorName
getBatchProcessorName BatchProcessor{batchProcessorName} = batchProcessorName

data Indexer outMsg
  = forall k.
    AT.TableKey k =>
    Indexer
      { indexerIndexName :: IndexName,
        indexerIndexBy :: outMsg -> [k]
      }

-- TODO: What is a stream step, really? Just seems to be miscellaneous inputs to
-- 'run', gathered together so we can use a builder style on them.
-- Streams and AggrTables are well-defined, but stream steps are not.
-- TODO: need a splitter that can efficiently route messages to different
-- output streams.
data StreamStep outMsg runResult where
  IndexedStreamProcessor ::
    (Indexable c, Message b, AT.TableKey k) =>
    { indexedStreamProcessorInner :: StreamStep b c,
      indexedStreamProcessorIndexName :: IndexName,
      indexedStreamProcessorIndexBy :: b -> [k]
    } ->
    StreamStep b
      (Index k, c)
  StreamProcessor ::
    Message b =>
    { streamProcessorWatermarkBy :: WatermarkBy b,
      --TODO: we should probably move all the logic in the small wrappers in
      -- FDBStreaming.hs like 'runIndexers', 'writeToRandomPartition', etc into
      -- the batch processors, with a default value of StreamProcessor for 'pipe'. Then
      -- add a function to compose more behavior onto this function. There's no
      -- reason I can see that pipeStep and this callback both need to exist.
      -- Same with the other constructors.
      streamProcessorBatchProcessors :: [BatchProcessor b],
      -- | This is a hack to allow IndexedStreamProcessor to push its index
      -- function down into the run function for StreamProcessor. It should
      -- only be used by 'run' for IndexedStreamProcessor. Its job is to forget
      -- the index key type, while IndexedStreamProcessor's job is to remember
      -- it. TODO: is there a less redundant way to do this? We have a new
      -- GADT constructor, this existentially-quantified list, and a type class!
      streamProcessorIndexers :: [Indexer b],
      streamProcessorStreamStepConfig :: StreamStepConfig
    } ->
    StreamStep b
      (Stream 'FDB b)
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

class Indexable runResult where
  -- | When writing messages downstream, also index them by the given key
  -- function. This is immediately consistent with the write.
  indexBy ::
    (Message outMsg, AT.TableKey k) =>
    IndexName ->
    (outMsg -> [k]) ->
    StreamStep outMsg runResult ->
    StreamStep outMsg (Index k, runResult)

instance Indexable (Stream 'FDB outMsg) where
  indexBy ixnm f stp = IndexedStreamProcessor stp ixnm f

instance Indexable a => Indexable (Index k, a) where
  indexBy ixnm f stp = IndexedStreamProcessor stp ixnm f

stepWatermarkBy :: StreamStep outMsg runResult -> WatermarkBy outMsg
stepWatermarkBy IndexedStreamProcessor {..} = stepWatermarkBy indexedStreamProcessorInner
stepWatermarkBy StreamProcessor {..} = streamProcessorWatermarkBy
stepWatermarkBy TableProcessor {..} = tableProcessorWatermarkBy

stepProducesWatermark :: StreamStep outMsg runResult -> Bool
stepProducesWatermark = producesWatermark . stepWatermarkBy

isStepDefaultWatermarked :: StreamStep b r -> Bool
isStepDefaultWatermarked = isDefaultWatermark . stepWatermarkBy

-- NOTE: GHC bug prevents us from using record update syntax in this function,
-- which would make this much cleaner. https://gitlab.haskell.org/ghc/ghc/issues/2595
watermarkBy :: (b -> Transaction Watermark) -> StreamStep b r -> StreamStep b r
watermarkBy f (IndexedStreamProcessor inner nm ixby) =
  IndexedStreamProcessor (watermarkBy f inner) nm ixby
watermarkBy f (StreamProcessor _ ps ixers stepCfg) =
  StreamProcessor (CustomWatermark f) ps ixers stepCfg
watermarkBy f (TableProcessor g a _ trigger stepCfg) =
  TableProcessor g a (CustomWatermark f) trigger stepCfg

getStepConfig :: StreamStep b r -> StreamStepConfig
getStepConfig IndexedStreamProcessor {indexedStreamProcessorInner} =
  getStepConfig indexedStreamProcessorInner
getStepConfig StreamProcessor {streamProcessorStreamStepConfig} =
  streamProcessorStreamStepConfig
getStepConfig TableProcessor {tableProcessorStreamStepConfig} =
  tableProcessorStreamStepConfig

mapStepConfig :: (StreamStepConfig -> StreamStepConfig) -> StreamStep b r -> StreamStep b r
mapStepConfig f (IndexedStreamProcessor inner nm ixby) =
  IndexedStreamProcessor (mapStepConfig f inner) nm ixby
mapStepConfig f (StreamProcessor w p ixers c) = StreamProcessor w p ixers (f c)
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

consIndexer ::
  AT.TableKey k =>
  StreamStep outMsg runResult ->
  IndexName ->
  (outMsg -> [k]) ->
  StreamStep outMsg runResult
consIndexer (IndexedStreamProcessor inner ixnm' f') ixnm f =
  IndexedStreamProcessor (consIndexer inner ixnm f) ixnm' f'
consIndexer (StreamProcessor wm bps ixers conf) ixnm f =
  StreamProcessor wm bps (Indexer ixnm f : ixers) conf
-- TODO: make type more precise to indicate that this won't work on
-- table processors. Had difficulty doing so.
consIndexer s@TableProcessor {} _ _ = s

{- TODO
triggerBy :: ((Versionstamp 'Complete, v, [k]) -> Transaction [(k,aggr)])
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
triggerBy f tbp@(TableProcessor{}) = TriggeringTableProcessor tbp f
-}

data TriggerBy --TODO: decide what this needs to be
