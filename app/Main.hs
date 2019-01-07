{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Lib
import Control.Concurrent.Async
import Control.Concurrent.Async.Extra
import Data.Binary.Put (runPut, putWord64le)
import Data.ByteString.Lazy (toStrict)
import Data.Word (Word64)
import Data.Maybe
import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import Options.Generic

data ProgramOpts = ProgramOpts
  { numWriters :: Maybe Int
  , numReaders :: Maybe Int
  , numMsgs    :: Maybe Int
  } deriving (Show, Generic)

instance ParseRecord ProgramOpts

writeWord64 :: TopicConfig -> Word64 -> IO ()
writeWord64 tc x = do
  let bs = runPut $ putWord64le x
  writeTopic tc "throughput_test" [toStrict bs]

main :: IO ()
main = withFoundationDB currentAPIVersion Nothing $ \case
  Left err -> error (show err)
  Right db -> do
    let ss = subspace [BytesElem "writetest"]
    let tc = TopicConfig db ss
    ProgramOpts{..} <- getRecord "Throughput test"
    let numWriters' = fromMaybe 1 numWriters
    let numReaders' = fromMaybe 1 numReaders
    let numMsgs'    = fromMaybe 100 numMsgs
    let msgs = [1.. fromIntegral numMsgs']
    mapConcurrentlyBounded_ numWriters' (writeWord64 tc) msgs
    let (delBegin, delEnd) = rangeKeys $ subspaceRange ss
    runTransaction db $ clearRange delBegin delEnd
