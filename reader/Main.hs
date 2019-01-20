{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Lib
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.Async.Extra
import Control.Concurrent.STM
import Data.Binary.Get (runGet, getWord64le)
import Data.Binary.Put (runPut, putWord64le)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict, fromStrict)
import qualified Data.IntSet as IS
import Data.Sequence (Seq(..), ViewL(..), ViewR(..))
import Data.Word (Word64)
import Data.Maybe
import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import Options.Generic

data ProgramOpts = ProgramOpts
  { numReaders :: Maybe Int
  , numMsgs    :: Maybe Int
  } deriving (Show, Generic)

instance ParseRecord ProgramOpts

testReader :: ReaderName
testReader = "throughput_test_reader"

parseWord64 :: ByteString -> Integer
parseWord64 = fromIntegral . runGet getWord64le . fromStrict

readIntMsgAndCheckpoint :: TopicConfig -> IO Integer
readIntMsgAndCheckpoint tc = do
  putStrLn "Attempting a read"
  readNAndCheckpoint tc testReader 10 >>= \case
    Empty -> do
      putStrLn "Blocking until new message"
      blockUntilNew tc
      readIntMsgAndCheckpoint tc
    xs -> return $ sum $ fmap (parseWord64 . snd) xs

-- | returns (sum, count of messages read)
readSum :: TopicConfig -> (Integer, Integer) -> IO (Integer, Integer)
readSum tc (!i,!n) = do
  race timeout (readIntMsgAndCheckpoint tc) >>= \case
    Left () -> return (i,n)
    Right j -> do
      putStrLn $ "read: " ++ show j
      readSum tc (j + i, n+1)

  where timeout = threadDelay 1000000 -- 1 second in microseconds

main :: IO ()
main = withFoundationDB currentAPIVersion Nothing $ \case
  Left err -> error (show err)
  Right db -> do
    let ss = subspace [BytesElem "writetest"]
    let tc = makeTopicConfig db ss "throughput_test"
    ProgramOpts{..} <- getRecord "Throughput test"
    let numReaders' = fromMaybe 1 numReaders
    let numMsgs'    = fromMaybe 100 numMsgs
    let start = replicate numReaders' (0,0)
    sumsCounts <- mapConcurrentlyBounded numReaders' (readSum tc) start
    putStrLn $ "Sum of received messages is "
                ++ show (sum $ map fst sumsCounts)
                ++ " and we expected "
                ++ show (div (numMsgs' * (numMsgs' + 1)) 2)
                ++ "\ntotal read transactions: "
                ++ show (sum $ map snd sumsCounts)
    let (delBegin, delEnd) = rangeKeys $ subspaceRange ss
    runTransaction db $ clearRange delBegin delEnd
