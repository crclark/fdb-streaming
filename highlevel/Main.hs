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

writeInts :: TVar Int -> Int -> Stream Int
writeInts state n = StreamProducer "writeInts" $ do
  curr <- readTVarIO state
  if curr < n
    then do atomically $ modifyTVar' state (+1)
            return $ Just curr
    else return Nothing

keepOdds :: Stream Int -> Stream Int
keepOdds input = StreamPipe "keepOdds" input $ \x ->
  if odd x
    then return (Just x)
    else return Nothing

-- TODO: obviously with state as a tvar this can't actually be split into
-- multiple processes yet.
sumInts :: TVar Int -> Stream Int -> Stream Void
sumInts state input = StreamConsumer "sumInts" input $ \x -> do
  curr <- readTVarIO state
  putStrLn $ "### current value = " ++ show curr
  atomically $ modifyTVar' state (+x)

topo :: IO (Stream Void)
topo = do
  writeState <- newTVarIO 0
  sumState <- newTVarIO 0
  return $ sumInts sumState $ keepOdds (writeInts writeState 100)

main :: IO ()
main = withFoundationDB currentAPIVersion Nothing $ \case
  Left err -> error (show err)
  Right db -> do
    let ss = FDB.subspace [FDB.BytesElem "cool_subspace"]
    let conf = FDBStreamConfig db ss
    t <- topo
    runStream conf t
    forever $ threadDelay 1000000
