{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

module FDBStreaming.Stream.Internal (
  Stream(..),
  StreamName,
  StreamReadAndCheckpoint,
  isStreamWatermarked,
  streamConsumerCheckpointSS,
  getStreamWatermark,
  customStream
) where

import Data.Maybe (fromJust, isJust)
import FDBStreaming.JobConfig (JobConfig(jobConfigSS), JobSubspace)
import FDBStreaming.Topic
  ( PartitionId,
    ReaderName,
    TopicConfig
  )
import qualified FDBStreaming.Topic as Topic
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Watermark
  ( Watermark,
    WatermarkSS,
    getCurrentWatermark,
    setWatermark
  )

import Data.ByteString (ByteString)
import FoundationDB as FDB
  ( Transaction,
    await,
  )
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import FoundationDB.Versionstamp
  (Versionstamp (),
    VersionstampCompleteness (Complete),
  )
import Data.Sequence (Seq ())
import Data.Foldable (for_)
import Data.Witherable (Filterable, catMaybes, mapMaybe)
import Data.Word (Word8)
import Safe.Foldable (maximumMay)

-- TODO: add a MonadStream implementation that checks that all stream names are
-- unique.
type StreamName = ByteString

-- | Readable alias for the function that transactionally reads from a stream
-- and checkpoints what it has read.
type StreamReadAndCheckpoint a state =
  JobConfig
  -> ReaderName
  -> PartitionId
  -> FDB.Subspace
  --TODO: Word16?
  -> Word8
  -> state
  -> Transaction (Seq (Maybe (Versionstamp 'Complete), a))

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

data Stream a =
  forall state.
  Stream
  { streamReadAndCheckpoint
      :: JobConfig
      -> ReaderName
      -> PartitionId
      -> FDB.Subspace
      --TODO: Word16?
      -> Word8
      -> state
      -> Transaction (Seq (Maybe (Versionstamp 'Complete), a))
    -- ^ Given the name of the step consuming the stream, the
    -- partition id of the worker consuming the stream, a subspace
    -- for storing checkpoints, and a desired batch size n,
    -- transactionally read up to n messages and checkpoint the stream
    -- so that no calls with the same StepName will see the same message
    -- again. User-defined stream readers should return 'Nothing' for
    -- the versionstamp; this value is used to persist watermarks for
    -- streams that are stored inside FoundationDB.
  , streamMinReaderPartitions :: Integer
    -- ^ The minimum number of threads that must concurrently read from
    -- the stream in order to maintain real-time throughput.
  , streamWatermarkSS :: Maybe (JobSubspace -> WatermarkSS)
    -- ^ Computes the subspace storing watermark data for this stream.
    -- If not 'Nothing', guarantees that this stream is watermarked in the
    -- given subspace.
  , streamTopicConfig :: Maybe TopicConfig
    -- ^ Hacky unexported field to indicate that this stream is stored within
    -- FoundationDB. Several operations can only be performed on FoundationDB
    -- streams. Currently, only FoundationDB streams (and tables) can be
    -- watermarked.
  , streamName :: StreamName
  -- ^ The unique name of this stream. This is used to persist checkpointing
  -- data in FoundationDB, so conflicts in these names are very, very bad.
  , setUpState :: (JobConfig -> ReaderName -> PartitionId -> FDB.Subspace -> Transaction state)
  -- ^ Set up per-worker state needed by this stream reader. For example, this
  -- could be a connection to a database. A stream reader worker's lifecycle
  -- will call this once, then streamReadAndCheckpoint many times, then
  -- 'destroyState' once. This lifecycle will be repeated many times.
  -- Parameters passed to this function
  -- are the same as those passed to the batch read function.
  , destroyState :: state -> IO ()
  -- ^ Called once to destroy a worker's state as set up by 'setUpState'. Not
  -- a transaction because we can't guarantee that we can run transactions if
  -- the worker dies abnormally.
  }

defaultWmSS :: StreamName -> JobSubspace -> WatermarkSS
defaultWmSS = undefined

-- | Helper function for defining custom data sources for other external
-- databases.
customStream
  :: (JobConfig
      -> ReaderName
      -> PartitionId
      -> FDB.Subspace
      -> Word8
      -> state
      -> Transaction (Seq a))
  -- ^ Function that will be called to read a batch of records from the data
  -- source. Receives the configuration for the current job, the name and
  -- partition id of the stream processing step that is requesting the batch,
  -- a subspace you can use to store checkpoint information, and a requested
  -- batch size.
  -> Integer
  -- ^ Minimum number of threads that must concurrently read from the stream
  -- in order to maintain real-time throughput.
  -> Maybe (a -> Maybe Watermark)
  -- ^ A watermark function. It's recommended that you make watermarking
  -- optional. If a given event cannot be watermarked (for example, if only
  -- some events have timestamps), this function should return Nothing for that
  -- event.
  -> StreamName
  -- ^ The unique name of this stream. This must be provided by the user.
  -> (JobConfig -> ReaderName -> PartitionId -> FDB.Subspace -> Transaction state)
  -- ^ Called once when a worker is starting up, to set up any state the worker
  -- needs, such as a database connection. Parameters passed to this function
  -- are the same as those passed to the batch read function.
  -> (state -> IO ())
  -- ^ Called to destroy the worker state.
  -> Stream a
customStream readBatch minThreads wmFn streamName setUp destroy =
  let stream = Stream
        { streamReadAndCheckpoint = \cfg rn pid ss n state -> do
            msgs <- readBatch cfg rn pid ss n state
            for_ (fmap ($ jobConfigSS cfg)
                       (streamWatermarkSS stream))
                 $ \wmSS ->
                   for_
                     (maximumMay $ catMaybes $ (fmap (fromJust wmFn) msgs))
                     $ \wm -> setWatermark wmSS wm
            return $ fmap (Nothing, ) msgs
        , streamMinReaderPartitions = minThreads
        , streamWatermarkSS = case wmFn of
            Nothing -> Nothing
            Just _  -> Just $ defaultWmSS streamName
        ,  streamTopicConfig = Nothing
        , streamName = streamName
        , setUpState = setUp
        , destroyState = destroy
        }
      in stream

streamConsumerCheckpointSS :: JobSubspace
                           -> Stream a
                           -> ReaderName
                           -> FDB.Subspace
streamConsumerCheckpointSS jobSS stream rn = case (streamTopicConfig stream) of
  Nothing    -> FDB.extend jobSS [ C.topics
                                 , FDB.Bytes (streamName stream)
                                 , C.readers
                                 , FDB.Bytes rn]
  Just topic -> Topic.readerSS topic rn

isStreamWatermarked :: Stream a -> Bool
isStreamWatermarked = isJust . streamWatermarkSS

instance Functor Stream where
  fmap g Stream{..} =
    Stream
    { streamReadAndCheckpoint =
        \cfg rn pid ss n state ->
          fmap (fmap g) <$> streamReadAndCheckpoint cfg rn pid ss n state
    , ..
    }

instance Filterable Stream where
  mapMaybe g Stream{..} =
    Stream
    { streamReadAndCheckpoint =
        \cfg rdNm pid ss batchSize state
          -> mapMaybe (\(mv, x) -> fmap (mv,) (g x))
             <$> streamReadAndCheckpoint cfg rdNm pid ss batchSize state
    , ..
    }

-- | Returns the current watermark for the given stream, if it can be determined.
getStreamWatermark :: JobSubspace -> Stream a -> Transaction (Maybe Watermark)
getStreamWatermark jobSS stream = case streamWatermarkSS stream of
  Nothing -> return Nothing
  Just wmSS -> getCurrentWatermark (wmSS jobSS) >>= await
