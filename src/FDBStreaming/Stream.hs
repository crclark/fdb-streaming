{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming.Stream (
  Stream(..),
  StreamName,
  isStreamWatermarked,
  streamConsumerCheckpointSS,
  getStreamWatermark
) where

import Data.Maybe (isJust)
import FDBStreaming.JobConfig (JobConfig, JobSubspace)
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
import Data.Witherable (Filterable, mapMaybe)
import Data.Word (Word8)

{-
Brainstorming on how to generalize Stream to multiple input sources.

Right now, Streams represent things that we can both read from and write to.

This is nice because it keeps the types readable, if nothing else.

However, imagine that we can only read from Kafka (which will be true for the
immediate future, until I have time to implement writes, too).

How should Stream be changed?

Decision 1: keep read and write in the same type, or split?
  choice 1: Stream type can be read or written. Throw an error if user tries
            to do the wrong thing. It's actually not possible for the user to
            do the wrong thing at the moment, because only internal stuff can
            write to streams, anyway.
  choice 2: ReadStream and WriteStream types or type classes. Would need to be
            type classes, because we need to be able to write to the thing in
            one step and read from it in the next.

Decision 1: How to allow different implementations of Stream in a way that is
            user-extensible?
  choice 1: Single record, with function callbacks for all user-defined
            behavior. This currently seems like the best option. Good advantage
            is that "boring" default field values could be filled in by helpers
            that we provide.
  choice 2: Sum type, with special case for FDB topic, and "Other" case for
            user-provided stuff (which would look like choice 1). This seems
            strictly worse than choice 1, because the FDB topic case could be
            implemented in terms of the Other case.
  choice 3: Type class. Honestly don't see advantages at this time.

What do we currently do with streams?

- We watermark them, using a versionstamp checkpoint and a watermarkSS.
- We check whether they are watermarked
- We readNAndCheckpoint', returning [(VS, message)]
- We map and filter them with the callback _topicMapFilter
- We run forEachPartition on them to create jobs for each partition.
- writing to them doesn't really matter, because we control writing inside
  internal fns and can always create the structure we currently have already.

-}

-- TODO: add a Monad implementation that checks that all stream names are
-- unique.
type StreamName = ByteString

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
-- checkpoint.

-- TODO: don't the export the fields directly. Export a smart constructor
-- that prevents the user from populating streamTopicConfig. Also exports
-- accessors for the appropriate fields.
data Stream a =
  Stream
  { streamReadAndCheckpoint
      :: JobConfig
      -> ReaderName
      -> PartitionId
      -> FDB.Subspace
      --TODO: Word16?
      -> Word8
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
  , streamWatermarkSS :: Maybe WatermarkSS
    -- ^ A subspace storing watermark data for this stream.
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
  }

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
        \cfg rn pid ss n ->
          fmap (fmap g) <$> streamReadAndCheckpoint cfg rn pid ss n
    , ..
    }

instance Filterable Stream where
  mapMaybe g Stream{..} =
    Stream
    { streamReadAndCheckpoint =
        \cfg rdNm pid ss batchSize
          -> mapMaybe (\(mv, x) -> fmap (mv,) (g x))
             <$> streamReadAndCheckpoint cfg rdNm pid ss batchSize
    , ..
    }

-- | Returns the current watermark for the given stream, if it can be determined.
getStreamWatermark :: Stream a -> Transaction (Maybe Watermark)
getStreamWatermark s = case streamWatermarkSS s of
  Nothing -> return Nothing
  Just wmSS -> getCurrentWatermark wmSS >>= await
