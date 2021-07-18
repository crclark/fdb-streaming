{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.Watermark
  ( Watermark (..),
    WatermarkKey,
    WatermarkSS,
    setWatermark,
    getCurrentWatermark,
    getWatermark,
    minWatermark,
    watermarkMillisSinceEpoch,
    WatermarkBy (..),
    producesWatermark,
    isDefaultWatermark,
    topicWatermarkSS,
  )
where

import Control.DeepSeq (NFData)
import Data.Binary.Get (getInt64le)
import Data.Binary.Put (putInt64le, runPut)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.Data (Data)
import Data.Int (Int64)
import Data.Time (FormatTime, UTCTime)
import Data.Word (Word64)
import FDBStreaming.Topic (Topic, topicCustomMetadataSS)
import FDBStreaming.Util (millisSinceEpoch, millisSinceEpochToUTC, runGetMay)
import qualified FoundationDB as FDB
import FoundationDB as FDB
  ( Future,
    Transaction,
  )
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options.MutationType as Mut
import qualified FoundationDB.Versionstamp as FDB

watermarkMillisSinceEpoch :: Watermark -> Int64
watermarkMillisSinceEpoch = millisSinceEpoch . watermarkUTCTime

millisToBytes :: Int64 -> ByteString
millisToBytes = toStrict . runPut . putInt64le

bytesToMillis :: ByteString -> Maybe Int64
bytesToMillis = runGetMay getInt64le

-- NOTE: unlike most things in this project, we don't need to maintain one
-- watermark per partition to avoid conflicts. Each worker on each iteration
-- will read and then write a watermark. There will be no conflicts when reading
-- watermarks, because they will be reading a watermark based on the version
-- stamp of the last message in the batch they read -- which necessarily comes
-- from a transaction that was already committed. When writing a watermark,
-- they are using a versionstamped key, so there's no way that can conflict.

watermarkIncompleteKey :: WatermarkSS -> Watermark -> ByteString
watermarkIncompleteKey ss watermark =
  FDB.pack
    ss
    [ FDB.Bytes "w",
      FDB.IncompleteVS (FDB.IncompleteVersionstamp 0),
      FDB.Bytes
        $ millisToBytes
        $ millisSinceEpoch
        $ watermarkUTCTime watermark
    ]

versionWatermarkQueryKey :: WatermarkSS -> Word64 -> ByteString
versionWatermarkQueryKey ss version =
  FDB.pack
    ss
    [ FDB.Bytes "w",
      FDB.CompleteVS $
        FDB.CompleteVersionstamp
          (FDB.TransactionVersionstamp version maxBound)
          maxBound
    ]

currentWatermarkQueryKey :: WatermarkSS -> Transaction (Future ByteString)
currentWatermarkQueryKey ss =
  fmap (versionWatermarkQueryKey ss) <$> FDB.getReadVersion

-- | FDB key containing a watermark for a given topic. This is stored as
-- milliseconds since the epoch, in a little-endian integer.
type WatermarkKey = ByteString

-- | For a given topic, a watermark is a function that assigns to a versionstamp
-- (which can be thought of as a processing-time timestamp) an event timestamp
-- representing a point in time up to which we have complete data. Watermarks
-- originate from the roots of the DAG -- whatever process is feeding us data
-- needs to give us information about the times of the events, or we can't
-- produce a watermark. For example, a flat text file may not have any natural
-- timestamp associated with individual lines in the file. In contrast, Kafka
-- can tell us the time at which each event was inserted. For some sources, the
-- watermark is a heuristic, and events may arrive late.
--
-- For technical reasons, we must in some cases return a default minimum
-- watermark when no watermark is otherwise available. In such cases, we use
-- 'minWatermark', which is arbitrarily defined to be the start of the
-- Unix Epoch.
--
-- Watermarks use O(n) storage for each step that is watermarked. On a busy
-- system, this could be as much as 10MiB per day per step.
newtype Watermark = Watermark {watermarkUTCTime :: UTCTime}
  deriving stock (Eq, Data, Ord, Read, Show)
  -- TODO: https://github.com/haskell/time/issues/119
  -- newtype derive a ParseTime instance.
  deriving newtype (FormatTime, NFData)

minWatermark :: Watermark
minWatermark = Watermark $ millisSinceEpochToUTC 0

type WatermarkSS = FDB.Subspace

-- | Sets the watermark for the current FDB version to the given time, using
-- a versionstamped key. The time will be rounded to milliseconds before being
-- stored. Ignores the input watermark if the current watermark is larger.
setWatermark :: WatermarkSS -> Watermark -> Transaction ()
setWatermark ss watermark = do
  current <- getCurrentWatermark ss >>= FDB.await
  let k = watermarkIncompleteKey ss $ maybe watermark (max watermark) current
  FDB.atomicOp k (Mut.setVersionstampedKey "")

parseWatermarkKeyResult :: WatermarkSS -> ByteString -> Maybe Watermark
parseWatermarkKeyResult ss k = case FDB.unpack ss k of
  Left _ -> Nothing
  Right [FDB.Bytes "w", FDB.CompleteVS _, FDB.Bytes millisBytes] ->
    case bytesToMillis millisBytes of
      Nothing -> error "Failed to parse watermark"
      Just watermark -> Just $ Watermark $ millisSinceEpochToUTC watermark
  Right _ -> Nothing

-- Get the current high watermark for the given watermark subspace.
getCurrentWatermark :: WatermarkSS -> Transaction (Future (Maybe Watermark))
getCurrentWatermark ss = do
  q <- currentWatermarkQueryKey ss >>= FDB.await
  let sel = FDB.LastLessOrEq q
  fk <- FDB.getKey sel
  return (fmap (parseWatermarkKeyResult ss) fk)

-- | Given a watermark subspace and a transaction version, return the watermark
-- which was current as of that transaction version.
getWatermark ::
  WatermarkSS ->
  -- | Transaction version
  Word64 ->
  Transaction (Future (Maybe Watermark))
getWatermark ss version = do
  let k = versionWatermarkQueryKey ss version
  let sel = FDB.LastLessOrEq k
  fk <- FDB.getKey sel
  return (fmap (parseWatermarkKeyResult ss) fk)

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

-- | The watermark subspace for this topic. This can be used to manually get
-- and set the watermark on a topic, outside of the stream processing system.
--
-- Technically, Topics have no knowledge of watermarks themselves, but several
-- downstream namespaces attach watermarks to topics, so this is a good place
-- to put this function.
topicWatermarkSS :: Topic -> WatermarkSS
topicWatermarkSS = flip FDB.extend [FDB.Bytes "wm"] . topicCustomMetadataSS
