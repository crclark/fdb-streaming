{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedLists #-}

module Spec.FDBStreaming.Topic where

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)
import FoundationDB (Database, runTransaction)
import FoundationDB.Layer.Subspace (Subspace)
import Spec.FDBStreaming.Util (extendRand)
import FDBStreaming.Topic (writeTopic, writeTopic', readNAndCheckpoint, readNAndCheckpoint', makeTopic, getTopicCount, getPartitionCount)
import Data.ByteString (ByteString)
import qualified Data.Set as Set
import GHC.Exts (IsList(toList))

readWrite :: Subspace -> Database -> TestTree
readWrite testSS db = testCase "writeTopic and readNAndCheckpoint" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 3
  let write pid xs =
        runTransaction db $ writeTopic' topic pid (xs :: [ByteString])
  let readP pid = runTransaction db $ readNAndCheckpoint' topic pid "tr" 3
  -- write to each of the three partitions
  write 0 ["1", "2", "3"]
  write 1 ["4", "5", "6"]
  write 2 ["7", "8", "9"]
  -- read from each of the three partitions
  xs1 <- Set.fromList . toList . fmap snd <$> readP 0
  xs2 <- Set.fromList . toList . fmap snd <$> readP 1
  xs3 <- Set.fromList . toList . fmap snd <$> readP 2
  xs1 @?= ["1", "2", "3"]
  xs2 @?= ["4", "5", "6"]
  xs3 @?= ["7", "8", "9"]

partitionCounts :: Subspace -> Database -> TestTree
partitionCounts testSS db = testCase "getPartitionCount" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2
  let write pid xs =
        runTransaction db $ writeTopic' topic pid (xs :: [ByteString])
  write 0 ["1", "2", "3"]
  write 1 ["4", "5"]
  c1 <- runTransaction db $ getPartitionCount topic 0
  c2 <- runTransaction db $ getPartitionCount topic 1
  c1 @?= 3
  c2 @?= 2

topicCounts :: Subspace -> Database -> TestTree
topicCounts testSS db = testCase "getPartitionCount" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2
  let write pid xs =
        runTransaction db $ writeTopic' topic pid (xs :: [ByteString])
  write 0 ["1", "2", "3"]
  write 1 ["4", "5"]
  c <- runTransaction db $ getTopicCount topic
  c @?= 5

checkpoints :: Subspace -> Database -> TestTree
checkpoints testSS db = testCase "readNAndCheckpoint" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 1
  writeTopic db topic (["1", "2", "3", "4", "5"] :: [ByteString])
  xs1 <- fmap snd <$> readNAndCheckpoint db topic "tr" 2
  xs2 <- fmap snd <$> readNAndCheckpoint db topic "tr" 2
  xs3 <- fmap snd <$> readNAndCheckpoint db topic "tr" 1
  xs1 @?= ["1", "2"]
  xs2 @?= ["3", "4"]
  xs3 @?= ["5"]

  xs4 <- fmap snd <$> readNAndCheckpoint db topic "tr2" 2
  xs4 @?= ["1", "2"]

topicTests :: Subspace -> Database -> TestTree
topicTests testSS db = testGroup "Topic"
  [ readWrite testSS db
  , partitionCounts testSS db
  , topicCounts testSS db
  , checkpoints testSS db
  ]
