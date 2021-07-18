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
  streamlyRangeResult,
  parseWord64le,
  addOneAtomic,
  subtractOneAtomic,
  addAtomic
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
import Data.Binary.Get (Get, getWord64le, runGet, runGetOrFail)
import Data.Binary.Put (runPut, putInt64le)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Fixed (Fixed (MkFixed))
import Data.Time.Clock (UTCTime, getCurrentTime, secondsToNominalDiffTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Data.Int (Int64)
import qualified Streamly as S
import qualified Streamly.Prelude as S
import System.Random (Random, randomRIO)
import qualified FoundationDB as FDB
import qualified FoundationDB.Options.MutationType as Mut
import Control.Monad (when)

currMillisSinceEpoch :: MonadIO m => m Int64
currMillisSinceEpoch = liftIO (millisSinceEpoch <$> getCurrentTime)

millisSinceEpoch :: UTCTime -> Int64
millisSinceEpoch =
    round . (1e3 *) . toRational . utcTimeToPOSIXSeconds

millisSinceEpochToUTC :: Int64 -> UTCTime
millisSinceEpochToUTC =
  posixSecondsToUTCTime
  . secondsToNominalDiffTime
  . MkFixed -- convert to Pico type in Data.Fixed
  . (* 1000000000) -- convert milliseconds to picoseconds
  . fromIntegral

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

-- This just throws an error because if we fail to parse a number from FDB, we
-- are in a totally broken state, anyway.
parseWord64le :: Num a => ByteString -> a
parseWord64le bs = fromIntegral $ runGet getWord64le $ fromStrict bs

-- | Adds little-endian encoded 1 to the value stored at the given key.
addOneAtomic :: ByteString -> FDB.Transaction ()
addOneAtomic k = FDB.atomicOp k (Mut.add oneLE)
  where oneLE = "\x01\x00\x00\x00\x00\x00\x00\x00"

-- | Subtracts little-endian encoded 1 to the value stored at the given key.
subtractOneAtomic :: ByteString -> FDB.Transaction ()
subtractOneAtomic k = FDB.atomicOp k (Mut.add oneLE)
  where oneLE = "\xff\xff\xff\xff\xff\xff\xff\xff"

-- | Atomically adds little-endian encoded n to the value stored at the given key.
addAtomic :: ByteString -> Int -> FDB.Transaction ()
addAtomic k n = FDB.atomicOp k (Mut.add encoded)
  where encoded = toStrict $ runPut $ putInt64le $ fromIntegral n
