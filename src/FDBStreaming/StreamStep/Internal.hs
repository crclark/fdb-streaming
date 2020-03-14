{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NamedFieldPuns #-}

-- Internal module; export everything!
{-# OPTIONS_GHC -fno-warn-missing-export-lists #-}

module FDBStreaming.StreamStep.Internal  where

import FDBStreaming.Message(Message)
import FDBStreaming.Stream.Internal(Stream)
import FDBStreaming.Topic (Topic)
import qualified FDBStreaming.AggrTable as AT
import FDBStreaming.Watermark (Watermark)

import Data.ByteString (ByteString)
import Data.Sequence (Seq)
import Data.Word (Word8, Word16)
import FoundationDB (Transaction)
import FoundationDB.Versionstamp
  ( Versionstamp,
    VersionstampCompleteness (Complete),
  )

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
  -- TODO: we don't really need this case. oneToOneJoin could just internally
  -- create two StreamProcessors and the user would never know the difference.
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