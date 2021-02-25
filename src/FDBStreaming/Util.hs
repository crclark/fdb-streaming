{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

module FDBStreaming.Util (
  currMillisSinceEpoch,
  millisSinceEpoch,
  millisSinceEpochToUTC,
  runGetMay,
  logErrors,
  logAndRethrowErrors,
  withOneIn,
  chunksOfSize,
  streamlyRangeResult
) where

import Control.Logger.Simple (logError, showText)
import Control.Concurrent (myThreadId)
import Control.Exception
  ( Handler (Handler),
    SomeException,
    catches,
    throw
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
import qualified Streamly as S
import qualified Streamly.Prelude as S
import System.Random (Random, randomRIO)
import qualified FoundationDB as FDB
import Control.Monad (when)

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

logAndRethrowErrors :: String -> IO () -> IO ()
logAndRethrowErrors ident =
  flip
    catches
    [ Handler $ \(e :: SomeException) -> do
        tid <- myThreadId
        logError (showText ident <> " on thread " <> showText tid <> " caught " <> showText e)
        throw e
    ]

-- | performs the given action with probability 1/n.
withOneIn :: (Random a, Integral a, MonadIO m) => a -> m () -> m ()
withOneIn n action = do
  x <- liftIO $ randomRIO (1,n)
  when (x == 1) action

-- | @chunksOfSize n sz@ splits a list into chunks, such that for each chunk
-- @c@, @sum $ map sz c@ is less than or equal to @n@, except for elements
-- larger than the chunk size, which will be placed in singleton chunks.
chunksOfSize :: Word -> (a -> Int) -> [a] -> [[a]]
chunksOfSize _ _ [] = []
chunksOfSize n sz xs@(h:_) =
  let (l, rest) = go (sz h) 1 xs
      in take l xs : chunksOfSize n sz rest

  where go _ !l [] = (l, [])
        go !i !l (z:zs)
          | i + sz z > fromIntegral n = (l, zs)
          | otherwise = go (i + sz z) (l + 1) zs

streamlyRangeResult :: (S.IsStream t)
                    => FDB.RangeResult
                    -> t FDB.Transaction (ByteString, ByteString)
streamlyRangeResult rr =
  S.concatMap S.fromFoldable $
  flip S.unfoldrM (Just rr) $ \case
    Nothing -> return Nothing
    (Just (FDB.RangeDone xs)) -> return (Just (xs, Nothing))
    (Just (FDB.RangeMore xs f)) -> do
      next <- FDB.await f
      return (Just (xs, Just next))
