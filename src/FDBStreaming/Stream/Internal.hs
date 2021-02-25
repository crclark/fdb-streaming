{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming.Stream.Internal
  ( Stream' (..),
    Stream (..),
    StreamPersisted (..),
    StreamName,
    StreamReadAndCheckpoint,
    isStreamWatermarked,
    streamConsumerCheckpointSS,
    getStreamWatermark,
    customStream,
    setStreamWatermarkByTopic,
    streamFromTopic,
    streamTopic,
    maybeStreamTopic,
    streamName,
    streamWatermarkSS,
    streamMinReaderPartitions,
    getStream',
    putStream',
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
    Topic,
  )
import qualified FDBStreaming.Topic as Topic
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkSS,
    getCurrentWatermark,
    setWatermark,
    topicWatermarkSS,
  )
import FoundationDB as FDB
  ( Transaction,
    await,
  )
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import Safe.Foldable (maximumMay)
import Data.Bifunctor (first)

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
  Transaction (Seq (Maybe Topic.Coordinate, a))

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

-- | Contains all the fields that both internal and external streams have in
-- common. This is an internal detail of the stream implementation.
data Stream' a
  = forall state.
    Stream'
      { -- | Given the name of the step consuming the stream, the
        -- partition id of the worker consuming the stream, a subspace
        -- for storing checkpoints, and a desired batch size n,
        -- transactionally read up to n messages and checkpoint the stream
        -- so that no calls with the same StepName will see the same message
        -- again. User-defined stream readers should return 'Nothing' for
        -- the Topic 'Checkpoint'; this value is used to persist watermarks for
        -- streams that are stored inside FoundationDB as 'Topic's.
        streamReadAndCheckpoint' :: StreamReadAndCheckpoint a state,
        -- | The minimum number of threads that must concurrently read from
        -- the stream in order to maintain real-time throughput.
        streamMinReaderPartitions' :: Integer,
        -- | Computes the subspace storing watermark data for this stream.
        -- If not 'Nothing', guarantees that this stream is watermarked in the
        -- given subspace.
        streamWatermarkSS' :: Maybe (JobSubspace -> WatermarkSS),
        -- | If this stream is the output of a stream step which is persisted to
        -- FoundationDB, then this contains the underlying Topic in which the
        -- stream is persisted. Streams that represent external data sources
        -- (such as Kafka topics) will have this set to 'Nothing'.
        streamName' :: StreamName,
        -- | Set up per-worker state needed by this stream reader. For example, this
        -- could be a connection to a database. A stream reader worker's lifecycle
        -- will call this once, then streamReadAndCheckpoint' many times, then
        -- 'destroyState'' once. This lifecycle will be repeated many times.
        -- Parameters passed to this function
        -- are the same as those passed to the batch read function.
        setUpState' :: JobConfig -> ReaderName -> PartitionId -> FDB.Subspace -> Transaction state,
        -- | Called once to destroy a worker's state as set up by 'setUpState''. Not
        -- a transaction because we can't guarantee that we can run transactions if
        -- the worker dies abnormally.
        destroyState' :: state -> IO ()
      }

-- | Denotes where the data in a stream is persisted. 'External' indicates that
-- the data is not stored in FoundationDB. In this case, it's either being
-- pushed or pulled from an external
-- source. 'FDB' indicates that it is stored in FoundationDB. Streams stored in
-- FoundationDB support additional operations, such as indexing, but may have
-- lower throughput.
data StreamPersisted = External | FDB

data Stream (t :: StreamPersisted) a where
  ExternalStream :: Stream' a -> Stream 'External a
  InternalStream :: Topic -> Stream' a -> Stream 'FDB a

streamTopic :: Stream 'FDB a -> Topic
streamTopic (InternalStream t _) = t

maybeStreamTopic :: Stream t a -> Maybe Topic
maybeStreamTopic (ExternalStream _) = Nothing
maybeStreamTopic (InternalStream t _) = Just t

streamName :: Stream t a -> StreamName
streamName = streamName' . getStream'

streamWatermarkSS :: Stream t a -> Maybe (JobSubspace -> WatermarkSS)
streamWatermarkSS = streamWatermarkSS' . getStream'

streamMinReaderPartitions :: Stream t a -> Integer
streamMinReaderPartitions = streamMinReaderPartitions' . getStream'

getStream' :: Stream t a -> Stream' a
getStream' (ExternalStream stream) = stream
getStream' (InternalStream _ stream) = stream

putStream' :: Stream t a -> Stream' b -> Stream t b
putStream' (ExternalStream _) stream = ExternalStream stream
putStream' (InternalStream t _) stream = InternalStream t stream

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
  Stream 'External a
customStream readBatch minThreads wmFn streamName' setUp destroy =
  let stream = Stream'
        { streamReadAndCheckpoint' = \cfg rn pid ss n state -> do
            msgs <- readBatch cfg rn pid ss n state
            for_
              ( fmap
                  ($ jobConfigSS cfg)
                  (streamWatermarkSS' stream)
              )
              $ \wmSS ->
                for_
                  (maximumMay (mapMaybe (fromJust wmFn) msgs))
                  $ \wm -> setWatermark wmSS wm
            return (fmap (Nothing,) msgs),
          streamMinReaderPartitions' = minThreads,
          streamWatermarkSS' = case wmFn of
            Nothing -> Nothing
            Just _ -> Just $ defaultWmSS streamName',
          streamName' = streamName',
          setUpState' = setUp,
          destroyState' = destroy
        }
   in ExternalStream stream

-- | Creates a subspace in which a consumer can checkpoint their progress.
streamConsumerCheckpointSS ::
  -- | Subspace containing the entire job.
  JobSubspace ->
  -- | input stream that we are checkpointing the consumption of.
  Stream t a ->
  -- | Name of the reader who is consuming from the input stream.
  ReaderName ->
  FDB.Subspace
streamConsumerCheckpointSS jobSS stream rn = case stream of
  ExternalStream stream' ->
    FDB.extend
      jobSS
      [ C.topics,
        FDB.Bytes (streamName' stream'),
        C.readers,
        FDB.Bytes rn
      ]
  InternalStream topic _ -> Topic.readerSS topic rn

isStreamWatermarked :: Stream t a -> Bool
isStreamWatermarked = isJust . streamWatermarkSS' . getStream'

-- | The functor instance for streams is lazy -- it will be computed by each
-- step that consumes a stream. If the function is very expensive, it may be
-- more efficient to use 'FDBStreaming.pipe' to write the result of the function
-- to FoundationDB once, then have all downstream steps read from that.
instance Functor Stream' where
  fmap g Stream' {..} =
    Stream'
      { streamReadAndCheckpoint' = \cfg rn pid ss n state ->
          fmap (fmap g) <$> streamReadAndCheckpoint' cfg rn pid ss n state,
        ..
      }

instance Functor (Stream t) where
  fmap f (ExternalStream s) = ExternalStream (fmap f s)
  fmap f (InternalStream t s) = InternalStream t (fmap f s)

-- | Same caveat applies as for the Functor instance. All downstream steps will
-- read all messages and run the filter logic.
instance Filterable Stream' where
  mapMaybe g Stream' {..} =
    Stream'
      { streamReadAndCheckpoint' = \cfg rdNm pid ss batchSize state ->
          mapMaybe (mapM g)
            <$> streamReadAndCheckpoint' cfg rdNm pid ss batchSize state,
        ..
      }

-- | Returns the current watermark for the given stream, if it can be determined.
getStreamWatermark :: JobSubspace -> Stream t a -> Transaction (Maybe Watermark)
getStreamWatermark jobSS stream = case streamWatermarkSS' (getStream' stream) of
  Nothing -> return Nothing
  Just wmSS -> getCurrentWatermark (wmSS jobSS) >>= await

-- | sets the watermark subspace to this stream to the watermark subspace of
-- streamTopic. The
-- step writing to the topic must actually have a watermarking function (or
-- default watermark) set. Don't export this; too confusing for users.
setStreamWatermarkByTopic :: Stream 'FDB a -> Stream 'FDB a
setStreamWatermarkByTopic (InternalStream tc stream) =
  InternalStream tc $ stream {streamWatermarkSS' = Just $ \_ -> topicWatermarkSS tc}

-- | Create a stream from a topic. Assumes the topic is not watermarked. If it
-- is and you want to propagate watermarks downstream, the caller must call
-- 'setStreamWatermarkByTopic' on the result.
--
-- You only need this function if you are trying to access a topic that was
-- created by another pipeline, or created manually.
streamFromTopic :: Topic -> StreamName -> Stream 'FDB ByteString
streamFromTopic tc streamName' = InternalStream tc $
  Stream'
    { streamReadAndCheckpoint' = \_cfg rn pid _chkptSS n _state ->
        fmap (first Just) <$> Topic.readNAndCheckpoint tc pid rn n,
      streamMinReaderPartitions' = fromIntegral $ Topic.numPartitions tc,
      streamWatermarkSS' = Nothing,
      streamName' = streamName',
      setUpState' = \_ _ _ _ -> return (),
      destroyState' = const $ return ()
    }
