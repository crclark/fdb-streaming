{-# LANGUAGE OverloadedStrings#-}
{-# LANGUAGE LambdaCase #-}

module Main where

import Lib

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
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

writeInts :: StreamName -> TVar Int -> Int -> Stream Int
writeInts sn state n = StreamProducer sn $ do
  curr <- readTVarIO state
  if curr < n
    then do atomically $ modifyTVar' state (+1)
            putStrLn $ show sn ++ " writing " ++ show curr
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

joinId :: Messageable a => StreamName -> Stream a -> Stream a -> Stream (a,a)
joinId sn l r = Stream1to1Join sn l r id id

topo :: IO (Stream Void)
topo = do
  writeState <- newTVarIO 0
  sumState <- newTVarIO 0
  return $ sumInts sumState $ keepOdds (writeInts "write_ints" writeState 100)

joinTopo :: IO (Stream Void)
joinTopo = do
  writeState1 <- newTVarIO 0
  writeState2 <- newTVarIO 0
  let writer1 = writeInts "write1" writeState1 100
  let writer2 = writeInts "write2" writeState2 100
  let join = joinId "intjoin" writer1 writer2
  let printer = StreamConsumer "print" join print
  return printer

topSS :: Subspace
topSS = FDB.subspace [FDB.BytesElem "cool_subspace"]

mainLoop :: Database -> IO ()
mainLoop db = do
  let conf = FDBStreamConfig db topSS
  t <- joinTopo
  runStream conf t
  forever $ threadDelay 1000000

main :: IO ()
main = withFoundationDB currentAPIVersion Nothing $ \case
  Left err -> error (show err)
  Right db -> finally (mainLoop db) $ do
    putStrLn "Cleaning up FDB state"
    let (delBegin, delEnd) = rangeKeys $ subspaceRange topSS
    runTransaction db $ clearRange delBegin delEnd
