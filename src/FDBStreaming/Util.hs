{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.Util (
  currMillisSinceEpoch,
  millisSinceEpoch,
  millisSinceEpochToUTC,
  runGetMay,
  logErrors,
  withOneIn
) where

import Control.Logger.Simple (logError, showText)
import Control.Concurrent (myThreadId)
import Control.Exception
  ( Handler (Handler),
    SomeException,
    catches,
  )
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Binary.Get (Get, runGetOrFail)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict)
import Data.Fixed (Fixed (MkFixed), E12)
import Data.Time.Clock (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Data.Int (Int64)
import Unsafe.Coerce (unsafeCoerce)
import System.Random (Random, randomRIO)

currMillisSinceEpoch :: MonadIO m => m Int64
currMillisSinceEpoch = liftIO (millisSinceEpoch <$> getCurrentTime)

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

logErrors :: String -> IO () -> IO ()
logErrors ident =
  flip
    catches
    [ Handler $ \(e :: SomeException) -> do
        tid <- myThreadId
        logError (showText ident <> " on thread " <> showText tid <> " caught " <> showText e)
    ]

-- | performs the given action with probability 1/n.
withOneIn :: (Random a, Integral a, MonadIO m) => a -> m () -> m ()
withOneIn n action = do
  x <- liftIO $ randomRIO (1,n)
  if x == 1 then action else return ()
