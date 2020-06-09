{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}

-- Internal module; export everything!
{-# OPTIONS_GHC -fno-warn-missing-export-lists #-}

module FDBStreaming.StreamStep.Internal  where

import FDBStreaming.JobConfig (JobSubspace)
import FDBStreaming.Message(Message)
import FDBStreaming.Stream.Internal(Stream(streamWatermarkSS, streamTopic))
import FDBStreaming.Index (Index, IndexName)
import FDBStreaming.Topic (Topic, Checkpoint)
import qualified FDBStreaming.AggrTable as AT
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkBy(CustomWatermark),
    WatermarkSS,
    producesWatermark,
    isDefaultWatermark
  )

import Data.ByteString (ByteString)
import Data.Sequence (Seq)
import Data.Word (Word8, Word16)
import FoundationDB (Transaction)
import FoundationDB.Layer.Subspace (Subspace)

-- | The name of a step in the pipeline. A step can be thought of as a function
-- that consumes a stream and writes to a stream, a table, or a side effect.
type StepName = ByteString

data GroupedBy k v
  = GroupedBy
      { groupedByInStream :: Stream v,
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

-- | Contains all the information we need to pull data out of a stream and
-- transform it. Uses an existential so that we can more easily implement
-- joins and other operations that pull from multiple streams. This type is an
-- internal detail of StreamStep, and not very useful on its own.
--
-- If the input stream is a topic with n partitions, up to n instances of this
-- processor will be running on n threads simultaneously.
data BatchProcessor b =
  forall a. BatchProcessor {
    batchInStream :: Stream a,
    -- | A batch processing function takes a batch of messages as input, along
    -- with a subspace in which it can store any state it needs to persist to
    -- operate or communicate. This subspace is shared among all batch
    -- processors in the same 'StreamStep', which allows heterogeneous workers
    -- within a step to communicate. For example, this is used to store
    -- temporary data for joins -- one BatchProcessor is responsible for the
    -- left side of the join, the other is responsible for the right side.
    processBatch :: Subspace -> Seq (Maybe Checkpoint, a) -> Transaction (Seq b)
  }

-- | Returns the watermarkSS function from the underlying stream.
batchProcessorInputWatermarkSS :: BatchProcessor b -> Maybe (JobSubspace -> WatermarkSS)
batchProcessorInputWatermarkSS (BatchProcessor i _) = streamWatermarkSS i

-- | Returns a topic if the input stream to this batch processor is persisted
-- inside FoundationDB as a 'Topic'.
batchProcessorInputTopic :: BatchProcessor b -> Maybe Topic
batchProcessorInputTopic (BatchProcessor i _) = streamTopic i

data Indexer outMsg =
  forall k. AT.TableKey k => Indexer {
    indexerIndexName :: IndexName,
    indexerIndexBy :: (outMsg -> [k])
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
      indexedStreamProcessorIndexBy :: (b -> [k])
    } -> StreamStep b (Index k, c)
  StreamProcessor ::
    Message b =>
    { streamProcessorWatermarkBy :: WatermarkBy b,
      --TODO: we should probably move all the logic in 'pipeStep' into
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

class Indexable runResult where
  indexBy :: (Message outMsg, AT.TableKey k)
          => IndexName
          -> (outMsg -> [k])
          -> StreamStep outMsg runResult
          -> StreamStep outMsg (Index k, runResult)

instance Indexable (Stream outMsg) where
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

consIndexer :: AT.TableKey k
            => StreamStep outMsg runResult
            -> IndexName
            -> (outMsg -> [k])
            -> StreamStep outMsg runResult
consIndexer (IndexedStreamProcessor inner ixnm' f') ixnm f =
  IndexedStreamProcessor (consIndexer inner ixnm f) ixnm' f'
consIndexer (StreamProcessor wm bps ixers conf) ixnm f =
  StreamProcessor wm bps (Indexer ixnm f : ixers) conf
-- TODO: make type more precise to indicate that this won't work on
-- table processors. Had difficulty doing so.
consIndexer s@TableProcessor{} _ _ = s

{- TODO
triggerBy :: ((Versionstamp 'Complete, v, [k]) -> Transaction [(k,aggr)])
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
          -> StreamStep v (k, aggr) (AT.AggrTable k aggr)
triggerBy f tbp@(TableProcessor{}) = TriggeringTableProcessor tbp f
-}

data TriggerBy --TODO: decide what this needs to be
