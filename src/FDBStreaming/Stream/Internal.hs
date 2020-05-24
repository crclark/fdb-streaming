{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.Stream.Internal
  ( Stream (..),
    StreamName,
    StreamReadAndCheckpoint,
    isStreamWatermarked,
    streamConsumerCheckpointSS,
    getStreamWatermark,
    customStream,
    setStreamWatermarkByTopic,
    streamFromTopic
  )
where

import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.Maybe (fromJust, isJust)
import Data.Sequence (Seq ())
import Data.Witherable.Class (Filterable, mapMaybe)
import Data.Word (Word16)
import FDBStreaming.JobConfig (JobConfig (jobConfigSS), JobSubspace)
import FDBStreaming.Topic
  ( PartitionId,
    ReaderName,
    Topic
  )
import qualified FDBStreaming.Topic as Topic
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkSS,
    getCurrentWatermark,
    setWatermark,
    topicWatermarkSS
  )
import FoundationDB as FDB
  ( Transaction,
    await,
  )
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import Safe.Foldable (maximumMay)

-- TODO: add a MonadStream implementation that checks that all stream names are
-- unique.
type StreamName = ByteString

-- | Readable alias for the function that transactionally reads from a stream
-- and checkpoints what it has read.
type StreamReadAndCheckpoint a state =
  JobConfig ->
  ReaderName ->
  PartitionId ->
  FDB.Subspace ->
  Word16 ->
  state ->
  Transaction (Seq (Maybe Topic.Checkpoint, a))

-- TODO: say we have a topology like
--
-- k <- kafkaInput
-- foos <- filter isFoo k
-- bars <- filter isBar k
-- bazs <- filter isBaz k
--
-- In this case, it would be nice to only have one process reading Kafka for
-- all of those three branches of the topology. The reader could write to all
-- three. We need a new step type that runs multiple downstream steps in
-- lockstep, in a single transaction. Problem: it's not clear to either us
-- (if we perform the optimization automatically) or the user (doing it manually)
-- how many downstream steps can be merged in this way without hitting
-- scalability problems.

-- TODO: I wanted to optimize this by performing the read before starting the
-- transaction (so the read time doesn't count against the transaction time limit),
-- but doing so would add significant complexity to the implementation. We'd
-- need separate read and write checkpoints, and we'd need to use the delivery
-- functionality of the leases to ensure that a slow process doesn't read a batch,
-- go incommunicado for a long time, then come back and try to write, after we
-- decided it's dead. So for now, we'll just start the transaction, read, then
-- checkpoint. A workaround would be to do one-time setup in setupState. That
-- might be enough for all use cases. We shall see.

-- | Represents an unbounded stream of messages of type @a@.
data Stream a
  = forall state.
    Stream
      { -- | Given the name of the step consuming the stream, the
        -- partition id of the worker consuming the stream, a subspace
        -- for storing checkpoints, and a desired batch size n,
        -- transactionally read up to n messages and checkpoint the stream
        -- so that no calls with the same StepName will see the same message
        -- again. User-defined stream readers should return 'Nothing' for
        -- the Topic 'Checkpoint'; this value is used to persist watermarks for
        -- streams that are stored inside FoundationDB as 'Topic's.
        streamReadAndCheckpoint :: StreamReadAndCheckpoint a state,
        -- | The minimum number of threads that must concurrently read from
        -- the stream in order to maintain real-time throughput.
        streamMinReaderPartitions :: Integer,
        -- | Computes the subspace storing watermark data for this stream.
        -- If not 'Nothing', guarantees that this stream is watermarked in the
        -- given subspace.
        streamWatermarkSS :: Maybe (JobSubspace -> WatermarkSS),
        -- | Hacky unexported field to indicate that this stream is stored within
        -- FoundationDB. Several operations can only be performed on FoundationDB
        -- streams. Currently, only FoundationDB streams (and tables) can be
        -- watermarked.
        streamTopic :: Maybe Topic,
        -- | The unique name of this stream. This is used to persist checkpointing
        -- data in FoundationDB, so conflicts in these names are very, very bad.
        streamName :: StreamName,
        -- | Set up per-worker state needed by this stream reader. For example, this
        -- could be a connection to a database. A stream reader worker's lifecycle
        -- will call this once, then streamReadAndCheckpoint many times, then
        -- 'destroyState' once. This lifecycle will be repeated many times.
        -- Parameters passed to this function
        -- are the same as those passed to the batch read function.
        setUpState :: JobConfig -> ReaderName -> PartitionId -> FDB.Subspace -> Transaction state,
        -- | Called once to destroy a worker's state as set up by 'setUpState'. Not
        -- a transaction because we can't guarantee that we can run transactions if
        -- the worker dies abnormally.
        destroyState :: state -> IO ()
      }

defaultWmSS :: StreamName -> JobSubspace -> WatermarkSS
defaultWmSS sn ss = FDB.extend ss [C.topics, FDB.Bytes sn, C.customMeta, FDB.Bytes "wm"]

-- | Helper function for defining custom data sources for other external
-- databases.
customStream ::
  -- | Function that will be called to read a batch of records from the data
  -- source. Receives the configuration for the current job, the name and
  -- partition id of the stream processing step that is requesting the batch,
  -- a subspace you can use to store checkpoint information, and a requested
  -- batch size.
  ( JobConfig ->
    ReaderName ->
    PartitionId ->
    FDB.Subspace ->
    Word16 ->
    state ->
    Transaction (Seq a)
  ) ->
  -- | Minimum number of threads that must concurrently read from the stream
  -- in order to maintain real-time throughput.
  Integer ->
  -- | A watermark function. It's recommended that you make watermarking
  -- optional. If a given event cannot be watermarked (for example, if only
  -- some events have timestamps), this function should return Nothing for that
  -- event.
  Maybe (a -> Maybe Watermark) ->
  -- | The unique name of this stream. This must be provided by the user.
  StreamName ->
  -- | Called once when a worker is starting up, to set up any state the worker
  -- needs, such as a database connection. Parameters passed to this function
  -- are the same as those passed to the batch read function.
  (JobConfig -> ReaderName -> PartitionId -> FDB.Subspace -> Transaction state) ->
  -- | Called to destroy the worker state.
  (state -> IO ()) ->
  Stream a
customStream readBatch minThreads wmFn streamName setUp destroy =
  let stream = Stream
        { streamReadAndCheckpoint = \cfg rn pid ss n state -> do
            msgs <- readBatch cfg rn pid ss n state
            for_
              ( fmap
                  ($ jobConfigSS cfg)
                  (streamWatermarkSS stream)
              )
              $ \wmSS ->
                for_
                  (maximumMay (mapMaybe (fromJust wmFn) msgs))
                  $ \wm -> setWatermark wmSS wm
            return (fmap (Nothing,) msgs),
          streamMinReaderPartitions = minThreads,
          streamWatermarkSS = case wmFn of
            Nothing -> Nothing
            Just _ -> Just $ defaultWmSS streamName,
          streamTopic = Nothing,
          streamName = streamName,
          setUpState = setUp,
          destroyState = destroy
        }
   in stream

streamConsumerCheckpointSS ::
  JobSubspace ->
  Stream a ->
  ReaderName ->
  FDB.Subspace
streamConsumerCheckpointSS jobSS stream rn = case streamTopic stream of
  Nothing ->
    FDB.extend
      jobSS
      [ C.topics,
        FDB.Bytes (streamName stream),
        C.readers,
        FDB.Bytes rn
      ]
  Just topic -> Topic.readerSS topic rn

isStreamWatermarked :: Stream a -> Bool
isStreamWatermarked = isJust . streamWatermarkSS

-- | The functor instance for streams is lazy -- it will be computed by each
-- step that consumes a stream. If the function is very expensive, it may be
-- more efficient to use 'FDBStreaming.pipe' to write the result of the function
-- to FoundationDB once, then have all downstream steps read from that.
instance Functor Stream where
  fmap g Stream {..} =
    Stream
      { streamReadAndCheckpoint = \cfg rn pid ss n state ->
          fmap (fmap g) <$> streamReadAndCheckpoint cfg rn pid ss n state,
        ..
      }

-- | Same caveat applies as for the Functor instance. All downstream steps will
-- read all messages and run the filter logic.
instance Filterable Stream where
  mapMaybe g Stream {..} =
    Stream
      { streamReadAndCheckpoint = \cfg rdNm pid ss batchSize state ->
          mapMaybe (mapM g)
            <$> streamReadAndCheckpoint cfg rdNm pid ss batchSize state,
        ..
      }

-- | Returns the current watermark for the given stream, if it can be determined.
getStreamWatermark :: JobSubspace -> Stream a -> Transaction (Maybe Watermark)
getStreamWatermark jobSS stream = case streamWatermarkSS stream of
  Nothing -> return Nothing
  Just wmSS -> getCurrentWatermark (wmSS jobSS) >>= await

-- | sets the watermark subspace to this stream to the watermark subspace of
-- streamTopic. The
-- step writing to the topic must actually have a watermarking function (or
-- default watermark) set. Don't export this; too confusing for users.
setStreamWatermarkByTopic :: Stream a -> Stream a
setStreamWatermarkByTopic stream =
  case streamTopic stream of
    Just tc ->
      stream {streamWatermarkSS = Just $ \_ -> topicWatermarkSS tc}
    Nothing -> error "setStreamWatermarkByTopic: stream is not a topic"

-- | Create a stream from a topic. Assumes the topic is not watermarked. If it
-- is and you want to propagate watermarks downstream, the caller must call
-- 'setStreamWatermarkByTopic' on the result.
--
-- You only need this function if you are trying to access a topic that was
-- created by another pipeline, or created manually.
streamFromTopic :: Topic -> StreamName -> Stream ByteString
streamFromTopic tc streamName =
  Stream
    { streamReadAndCheckpoint = \_cfg rn pid _chkptSS n _state ->
        fmap (fmap (\(c,xs) -> (Just c, xs))) $ Topic.readNAndCheckpoint tc pid rn n,
      streamMinReaderPartitions = fromIntegral $ Topic.numPartitions tc,
      streamWatermarkSS = Nothing,
      streamTopic = Just tc,
      streamName = streamName,
      setUpState = \_ _ _ _ -> return (),
      destroyState = const $ return ()
    }
