{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Utilities for pushing data into a stream from outside the stream
-- processing pipeline. This can to be used to insert incoming data from
-- HTTP requests, for example, or do simple things like generate test data and
-- insert it directly into a stream processing pipeline.
--
-- If you are trying to move data from another database into FDBStreaming, it
-- may be easier to write a wrapper using 'FDBStreaming.Stream.customStream',
-- which allows you stream data from an external database in a pulling fashion.
--
-- In order to make inserts from outside the system idempotent (which is
-- necessary to avoid duplicated writes if we receive 'CommitUnknownResult'
-- errors), this module's interface uses 'BatchWriter's, which allow you to
-- specify an idempotencyKey for each item you insert.
module FDBStreaming.Push
  ( PushStreamConfig (..),
    runPushStream,
    runPushStream',
  )
where

import Control.Monad (replicateM, void)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (for_)
import Data.Maybe (fromMaybe, isJust)
import Data.Word (Word8)
import qualified FDBStreaming.JobConfig as JC
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.Stream (Stream, StreamName, StreamPersisted (FDB))
import FDBStreaming.Stream.Internal (setStreamWatermarkByTopic, streamFromTopic)
import FDBStreaming.Topic (makeTopic, randPartition, topicCustomMetadataSS, writeTopic)
import FDBStreaming.Util.BatchWriter (BatchWriter, BatchWriterConfig, batchWriter, defaultBatchWriterConfig)
import FDBStreaming.Watermark (Watermark, setWatermark, topicWatermarkSS)
import FoundationDB (Transaction)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB

-- | Specifies the configuration of a stream that will be populated by pushing
-- messages to it from outside the pipeline system. This is a subset of the
-- options available in 'StreamStepConfig'.
data PushStreamConfig inMsg
  = PushStreamConfig
      { -- | A watermarking function for the inputs to the stream.
        pushStreamWatermarkBy :: Maybe (inMsg -> Transaction Watermark),
        -- TODO: reuse StepConfig for options below?

        -- | Number of partitions in the Topic that the elements of the stream
        -- will be persisted to. If Nothing, the default in the 'JobConfig' will
        -- be used.
        pushStreamOutputPartitions :: Maybe Word8
      }

-- | A default configuration with no watermarking, and using the 'JobConfig'
-- default number of partitions.
defaultPushStreamConfig :: PushStreamConfig a
defaultPushStreamConfig = PushStreamConfig Nothing Nothing

-- | Set up a stream that messages can be pushed to from outside the pipeline.
-- Returns the stream and a set of BatchWriters for writing to it. The stream
-- can be passed into a 'MonadStream' action to serve as a root of a pipeline
-- DAG.
runPushStream ::
  Message inMsg =>
  -- | Job configuration. Must match the JobConfig of whatever
  -- pipeline you want to share the output stream with.
  JC.JobConfig ->
  StreamName ->
  PushStreamConfig inMsg ->
  -- | Config for batch writers
  BatchWriterConfig ->
  -- | Number of batch writers to create
  Word ->
  IO (Stream 'FDB inMsg, [BatchWriter inMsg])
runPushStream jc sn ps bwc bn = do
  let numPartitions =
        fromMaybe
          (JC.defaultNumPartitions jc)
          (pushStreamOutputPartitions ps)
  let topic = makeTopic (JC.jobConfigSS jc) sn numPartitions (JC.defaultChunkSizeBytes jc)
  let stream =
        ( if isJust (pushStreamWatermarkBy ps)
            then setStreamWatermarkByTopic
            else id
        )
          $ streamFromTopic topic sn
  let writeBatch xs = do
        for_ (pushStreamWatermarkBy ps) $ \wf -> case xs of
          (x : _) -> do
            w <- wf x
            setWatermark (topicWatermarkSS topic) w
          _ -> return ()
        pid <- liftIO $ randPartition topic
        void $ writeTopic topic pid (fmap toMessage xs)
  let bwSS = FDB.extend (topicCustomMetadataSS topic) [FDB.Bytes "pbw"]
  writers <- replicateM (fromIntegral bn) $ batchWriter bwc (JC.jobConfigDB jc) bwSS writeBatch
  return (fmap fromMessage stream, writers)

-- | Simplified version of 'runPushStream' using basic default configuration.
runPushStream' ::
  Message inMsg =>
  JC.JobConfig ->
  StreamName ->
  IO (Stream 'FDB inMsg, BatchWriter inMsg)
runPushStream' jc sn =
  fmap head <$> runPushStream jc sn defaultPushStreamConfig defaultBatchWriterConfig 1
-- TODO: pushAggrTable interface.
