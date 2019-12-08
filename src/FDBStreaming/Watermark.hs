{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}

module FDBStreaming.Watermark (
  Watermark,
  WatermarkKey,
  WatermarkSS,
  setWatermark,
  getCurrentWatermark,
  getWatermark
) where

import Data.Binary.Get (getInt64le)
import Data.Binary.Put (runPut, putInt64le)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.Int (Int64)
import Data.Time (UTCTime)
import Data.Word (Word64)
import qualified FoundationDB as FDB
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options as FDB
import qualified FoundationDB.Versionstamp as FDB
import FoundationDB as FDB
  (
    Transaction,
    Future
  )
import FDBStreaming.Util (millisSinceEpoch, millisSinceEpochToUTC, runGetMay)

millisToBytes :: Int64 -> ByteString
millisToBytes = toStrict . runPut . putInt64le

bytesToMillis :: ByteString -> Maybe Int64
bytesToMillis = runGetMay getInt64le

watermarkIncompleteKey :: WatermarkSS -> Watermark -> ByteString
watermarkIncompleteKey ss watermark =
  FDB.pack ss [ FDB.Bytes "w"
              , FDB.IncompleteVS (FDB.IncompleteVersionstamp 0)
              , FDB.Bytes $ millisToBytes $ millisSinceEpoch watermark]

versionWatermarkQueryKey :: WatermarkSS -> Word64 -> ByteString
versionWatermarkQueryKey ss version =
  FDB.pack ss [ FDB.Bytes "w"
                , FDB.CompleteVS
                    $ FDB.CompleteVersionstamp
                      (FDB.TransactionVersionstamp version maxBound)
                      maxBound]

currentWatermarkQueryKey :: WatermarkSS -> Transaction (Future ByteString)
currentWatermarkQueryKey ss =
  fmap (versionWatermarkQueryKey ss) <$> FDB.getReadVersion

-- | FDB key containing a watermark for a given topic. This is stored as
-- milliseconds since the epoch, in a little-endian integer.
type WatermarkKey = ByteString

-- | For a given topic, a watermark is a function that assigns to a versionstamp
-- (which can be thought of as a processing-time timestamp) an event timestamp
-- representing a point in time up to which we have complete data. Watermarks
-- originate from the root of the DAG -- whatever process is feeding us data
-- needs to give us information about the times of the events, or we can't
-- produce a watermark. For example, a flat text file may not have any natural
-- timestamp associated with individual lines in the file. In contrast, Kafka
-- can tell us the time at which each event was inserted. In some cases, the
-- watermark is a heuristic, and events may arrive late.
type Watermark = UTCTime

type WatermarkSS = FDB.Subspace

-- | Sets the watermark for the current FDB version to the given time, using
-- a versionstamped key. The time will be rounded to milliseconds before being
-- stored.
setWatermark :: WatermarkSS -> Watermark -> Transaction ()
setWatermark ss watermark = do
  current <- getCurrentWatermark ss >>= FDB.await
  let k = watermarkIncompleteKey ss $ maybe watermark (max watermark) current
  FDB.atomicOp k (FDB.setVersionstampedKey "")

parseWatermarkKeyResult :: WatermarkSS -> ByteString -> Maybe Watermark
parseWatermarkKeyResult ss k = case FDB.unpack ss k of
  Left _ -> Nothing
  Right [FDB.Bytes "w", FDB.CompleteVS _, FDB.Bytes millisBytes] ->
    case bytesToMillis millisBytes of
      Nothing -> error "Failed to parse watermark"
      Just watermark -> Just (millisSinceEpochToUTC watermark)
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
getWatermark :: WatermarkSS
             -> Word64
             -- ^ Transaction version
             -> Transaction (Future (Maybe Watermark))
getWatermark ss version = do
  let sel = FDB.LastLessOrEq (versionWatermarkQueryKey ss version)
  fk <- FDB.getKey sel
  return (fmap (parseWatermarkKeyResult ss) fk)

-- TODO: the current watermarking algorithm could take about 10 MB per day per
-- processing step, if we watermark each batch we process. Could we reduce the
-- overhead further? Don't watermark every batch? And if batches are extremely
-- small, it could be even more overhead.
