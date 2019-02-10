{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import FDBStreaming
import FDBStreaming.Topic
import Control.Concurrent.Async
import Control.Concurrent.Async.Extra
import Control.Concurrent.STM
import Data.Binary.Put (runPut, putWord64le)
import Data.ByteString.Lazy (toStrict)
import qualified Data.IntSet as IS
import Data.Word (Word64)
import Data.Maybe
import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import Options.Generic

data ProgramOpts = ProgramOpts
  { numWriters :: Maybe Int
  , numMsgs    :: Maybe Int
  } deriving (Show, Generic)

instance ParseRecord ProgramOpts

writeWord64 :: TopicConfig -> Word64 -> IO ()
writeWord64 tc x = do
  let bs = runPut $ putWord64le x
  --writeOneMsgTopic tc (toStrict bs)
  return () -- TODO: I deleted writeOneMsgTopic. fix?


main :: IO ()
main = withFoundationDB defaultOptions $ \db -> do
  let ss = subspace [Bytes "writetest"]
  let tc = makeTopicConfig db ss "throughput_test"
  ProgramOpts{..} <- getRecord "Throughput test"
  let numWriters' = fromMaybe 1 numWriters
  let numMsgs'    = fromMaybe 100 numMsgs
  let msgs = [1.. fromIntegral numMsgs']
  mapConcurrentlyBounded_ numWriters' (writeWord64 tc) msgs
