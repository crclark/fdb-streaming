{-# LANGUAGE OverloadedStrings#-}

module Main where

import FDBStreaming
import FDBStreaming.Topic

import Control.Monad
import Control.Concurrent
import Control.Concurrent.Async (forConcurrently)
import Control.Concurrent.STM (TVar, readTVarIO, atomically, modifyTVar', newTVarIO)
import Control.Exception
import Data.Binary.Put (runPut, putWord64le)
import Data.Binary.Get (runGet, getWord64le)
import Data.List (sortOn)
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
            return $ Just curr
    else return Nothing

writeIntsTopic :: TopicConfig -> Int -> IO ()
writeIntsTopic tc n = go 0
  where go i = when (i < n) $ do
                 writeTopic tc (map toMessage [i..i+10])
                 go (i+11)

keepOdds :: Stream Int -> Stream Int
keepOdds input = StreamPipe "keepOdds" input $ \x ->
  if odd x
    then return (Just x)
    else return Nothing

-- TODO: obviously with state as a tvar this can't actually be split into
-- multiple processes yet.
sumInts :: TVar Int -> Stream Int -> Stream Void
sumInts state input = StreamConsumer "sumInts" input $ \x ->
  atomically $ modifyTVar' state (+x)

joinId :: Messageable a => StreamName -> Stream a -> Stream a -> Stream (a,a)
joinId sn l r = Stream1to1Join sn l r id id

topo :: IO (Stream Void)
topo = do
  writeState <- newTVarIO 0
  sumState <- newTVarIO 0
  return $ sumInts sumState $ keepOdds (writeInts "write_ints" writeState 100)

printEvery1000 :: (Int, Int) -> IO ()
printEvery1000 (x,_) = when (x `mod` 1000 == 0) (print x)


joinTopo :: Database -> IO (Stream Void)
joinTopo db = do
  writeState1 <- newTVarIO 0
  let rawTC = makeTopicConfig db topSS "raw_writes"
  void $ forkIO $ writeIntsTopic rawTC 100000
  let writer1 = writeInts "write1" writeState1 100000
  let writer2 = StreamExistingTopic "write2" rawTC
  let joiner = joinId "intjoin" writer1 writer2
  let printer = StreamConsumer "print" joiner printEvery1000
  return printer

topSS :: Subspace
topSS = FDB.subspace [FDB.Bytes "cool_subspace"]

printStats :: Database -> Subspace -> IO ()
printStats db ss = do
  tcs <- listExistingTopics db ss
  ts <- forConcurrently tcs $ \tc -> do
    before <- runTransaction db $ getTopicCount tc
    threadDelay 1000000
    after <- runTransaction db $ getTopicCount tc
    return (topicName tc, fromIntegral after - fromIntegral before)
  forM_ (sortOn fst ts) $ \(tn, c) ->
    putStrLn $ show tn ++ ": " ++ show (c :: Int) ++ " msgs/sec"

mainLoop :: Database -> IO ()
mainLoop db = do
  let conf = FDBStreamConfig db topSS
  t <- joinTopo db
  runStream conf t
  forever $ do
    printStats db topSS
    threadDelay 1000000

main :: IO ()
main = withFoundationDB defaultOptions $ \db ->
  finally (mainLoop db) $ do
    putStrLn "Cleaning up FDB state"
    let (delBegin, delEnd) = rangeKeys $ subspaceRange topSS
    runTransaction db $ clearRange delBegin delEnd
