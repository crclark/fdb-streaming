{-# LANGUAGE OverloadedStrings#-}
{-# LANGUAGE LambdaCase #-}

module Main where

import Lib

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Data.Binary.Put (runPut, putWord64le)
import Data.Binary.Get (runGet, getWord64le)
import Data.Void
import Data.ByteString.Lazy (toStrict, fromStrict)

import FoundationDB as FDB
import FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Tuple as FDB

instance Messageable Int where
  toMessage = toStrict . runPut . putWord64le . fromIntegral
  fromMessage = fromIntegral . runGet getWord64le . fromStrict

writeInts :: TVar Int -> Topic Int -> Int -> Stream Void Int
writeInts state topic n = StreamProducer "writeInts" topic $ do
  curr <- readTVarIO state
  if curr < n
    then do atomically $ modifyTVar' state (+1)
            return $ Just curr
    else return Nothing

keepOdds :: Topic Int -> Topic Int -> Stream Int Int
keepOdds input output = StreamPipe "keepOdds" input output $ \x ->
  if odd x
    then return (Just x)
    else return Nothing

-- TODO: obviously with state as a tvar this can't actually be split into
-- multiple processes yet.
sumInts :: TVar Int -> Topic Int -> Stream Int Void
sumInts state topic = StreamConsumer "sumInts" topic $ \x -> do
  curr <- readTVarIO state
  putStrLn $ "### current value = " ++ show curr
  atomically $ modifyTVar' state (+x)

topo :: FDB.Database -> IO (Stream Void Void)
topo db = do
  let ss = FDB.subspace [FDB.BytesElem "cool_subspace"]
  -- TODO: explicitly creating all these topics will quickly become
  -- unwieldy
  let writeTopic = Topic $ makeTopicConfig db ss "writeIntsTopic"
  let filteredTopic = Topic $ makeTopicConfig db ss "oddIntsTopic"
  writeState <- newTVarIO 0
  sumState <- newTVarIO 0
  -- TODO: composition is error-prone: have to pass the right topics in.
  -- TODO: would be nice to just be able to apply a pipe as a function to
  -- another pipe to do composition.
  return $ StreamComp (writeInts writeState writeTopic 100)
                      (StreamComp (keepOdds writeTopic filteredTopic)
                                  (sumInts sumState filteredTopic))


main :: IO ()
main = withFoundationDB currentAPIVersion Nothing $ \case
  Left err -> error (show err)
  Right db -> do
    t <- topo db
    runStream t
    forever $ threadDelay 1000000
