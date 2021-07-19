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
  addAtomic,
  splitOn,
  spanInclusive
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
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Fixed (Fixed (MkFixed))
import qualified Data.Persist as Persist
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

encodedIntLength :: Word
encodedIntLength = fromIntegral $ BS.length $ Persist.encode (maxBound :: Int)

-- | Like 'span', except that the left list includes the first element for which
-- f returned False.
spanInclusive :: (a -> Bool) -> [a] -> ([a], [a])
spanInclusive _ [] = ([], [])
spanInclusive _ [x] = ([x], [])
spanInclusive f (x:y:xs)
  | f x && f y = let (ys, zs) = spanInclusive f (y:xs) in (x : ys, zs)
  | f x && not (f y) = ([x, y], xs)
  | otherwise = ([x], y:xs)

-- | Split a list into chunks by a delimiter function. The delimiter is included
-- at the end of each chunk.
splitOn :: (a -> Bool) -> [a] -> [[a]]
splitOn _ [] = []
splitOn f xs =
  let (chunk, rest) = spanInclusive f xs
    in chunk : splitOn f rest

-- | @chunksOfSize n sz@ splits a list of bytestrings into chunks, such that
-- for each chunk
-- @c@, the length of the serialization of @c@ by the @persist@ library is less
-- than or equal to @n@, except for bytestrings
-- larger than the chunk size, which will be placed in singleton chunks.
chunksOfSize :: Word -> [ByteString] -> [[ByteString]]
chunksOfSize _ [] = []
chunksOfSize 0 xs = map return xs
chunksOfSize desiredChunkSizeBytes bss =
  map (map fst)
  $ splitOn ((>= desiredChunkSizeBytes) . snd)
  $ zip bss
  $ tail
  $ scanl acc prefixLength bss

  where
    -- each bytestring is serialized as length, bytestring.
    sz bs = encodedIntLength + fromIntegral (BS.length bs)

    -- A list is serialized as a word64 length followed by each encoded item.
    prefixLength = 8

    acc l bs = (l + sz bs) `mod` desiredChunkSizeBytes


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
