{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE TypeApplications #-}

module Spec.FDBStreaming.Topic (topicTests) where

import Control.Monad (void)
import qualified Data.ByteString.Char8 as BS
import qualified Data.Sequence as Seq
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)
import qualified FoundationDB as FDB
import FoundationDB (Database, runTransaction)
import FoundationDB.Layer.Subspace (Subspace)
import Spec.FDBStreaming.Util (extendRand)
import FDBStreaming.Topic (writeTopic, writeTopicIO, readNAndCheckpoint, readNAndCheckpointIO, makeTopic, getTopicCount, getPartitionCount)
import qualified FDBStreaming.Topic as Topic

readWrite :: Subspace -> Database -> TestTree
readWrite testSS db = testCase "writeTopic and readNAndCheckpoint" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 3 0
  let write pid xs =
        runTransaction db $ void $ writeTopic topic pid xs
  let readP pid = fmap (fmap snd) $ runTransaction db $ readNAndCheckpoint topic pid "tr" 3
  -- write to each of the three partitions
  write 0 ["1", "2", "3"]
  write 1 ["4", "5", "6"]
  write 2 ["7", "8", "9"]
  -- read from each of the three partitions
  xs1 <- readP 0
  xs2 <- readP 1
  xs3 <- readP 2
  xs1 @?= (["1", "2", "3"] :: Seq.Seq BS.ByteString)
  xs2 @?= ["4", "5", "6"]
  xs3 @?= ["7", "8", "9"]

partitionCounts :: Subspace -> Database -> TestTree
partitionCounts testSS db = testCase "getPartitionCount" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2 0
  let write pid xs =
        runTransaction db $ void $ writeTopic topic pid xs
  write 0 ["1", "2", "3"]
  write 1 ["4", "5"]
  c1 <- runTransaction db $ getPartitionCount topic 0
  c2 <- runTransaction db $ getPartitionCount topic 1
  c1 @?= 3
  c2 @?= 2

topicCounts :: Subspace -> Database -> TestTree
topicCounts testSS db = testCase "getPartitionCount" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2 0
  let write pid xs =
        runTransaction db $ void $ writeTopic topic pid xs
  write 0 ["1", "2", "3"]
  write 1 ["4", "5"]
  c <- runTransaction db $ getTopicCount topic
  c @?= 5

checkpoints :: Subspace -> Database -> TestTree
checkpoints testSS db = testCase "readNAndCheckpoint2" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 1 0
  void $ writeTopicIO db topic ["1", "2", "3", "4", "5"]
  xs1 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 2
  xs2 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 2
  xs3 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 1
  xs1 @?= ["1", "2"]
  xs2 @?= ["3", "4"]
  xs3 @?= ["5"]

  xs4 <- fmap snd <$> readNAndCheckpointIO db topic "tr2" 2
  xs4 @?= ["1", "2"]

  ckpt <- runTransaction db $ Topic.getCheckpoint topic 0 "tr"
  ckpts <- runTransaction db $ Topic.getCheckpoints topic "tr" >>= FDB.await
  Seq.singleton ckpt @?= ckpts

chunking :: Subspace -> Database -> TestTree
chunking testSS db = testCase "topic chunking" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 1 1024
  void $ writeTopicIO db topic ["1", "2", "3", "4", "5"]
  void $ writeTopicIO db topic ["6", "7", "8", "9", "10"]
  void $ writeTopicIO db topic ["11", "12", "13", "14", "15"]
  xs1 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 3
  xs2 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 3

  xs3 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 4

  xs4 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 1

  xs5 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 1

  xs6 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 2

  xs7 <- fmap snd <$> readNAndCheckpointIO db topic "tr" 5

  xs1 @?= ["1", "2", "3"]

  xs2 @?= ["4", "5", "6"]

  xs3 @?= ["7", "8", "9", "10"]

  xs4 @?= ["11"]

  xs5 @?= ["12"]

  xs6 @?= ["13", "14"]

  xs7 @?= ["15"]

  xs8 <- fmap snd <$> readNAndCheckpointIO db topic "tr2" 15
  xs8 @?= Seq.fromList [BS.pack (show @Int x) | x <- [1..15]]

topicTests :: Subspace -> Database -> TestTree
topicTests testSS db = testGroup "Topic"
  [ readWrite testSS db
  , partitionCounts testSS db
  , topicCounts testSS db
  , checkpoints testSS db
  , chunking testSS db
  ]
