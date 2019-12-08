module FDBStreaming.Util (
  millisSinceEpoch,
  millisSinceEpochToUTC,
  runGetMay
) where

import Data.Binary.Get (Get, runGetOrFail)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict)
import Data.Coerce (coerce)
import Data.Fixed (Fixed (MkFixed), E12)
import Data.Time
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Int
import Unsafe.Coerce (unsafeCoerce)

millisSinceEpoch :: UTCTime -> Int64
millisSinceEpoch =
    round . (1e3 *) . toRational . utcTimeToPOSIXSeconds

millisSinceEpochToUTC :: Int64 -> UTCTime
millisSinceEpochToUTC =
  posixSecondsToUTCTime
  . fixedToNominalDiffTime
  . MkFixed -- convert to Pico type in Data.Fixed
  . (* 1000000000) -- convert milliseconds to picoseconds
  . fromIntegral

-- TODO: once we can upgrade to the latest time library, which includes
-- nominalDiffTimeToSeconds and secondsToNominalDiffTime, we won't need this.
-- We currently can't upgrade because stackage doesn't have it in an LTS
-- version yet.
fixedToNominalDiffTime :: Fixed E12 -> NominalDiffTime
fixedToNominalDiffTime = unsafeCoerce

runGetMay :: Get a -> ByteString -> Maybe a
runGetMay g bs = case runGetOrFail g (fromStrict bs) of
  Right (_,_,a) -> Just a
  _ -> Nothing
