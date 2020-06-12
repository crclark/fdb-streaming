{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Utilities for testing pipelines.
module FDBStreaming.Testing (
  testOnInput,
  testJobConfig
) where

import Data.Traversable (for)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel)
import Control.Monad (void)
import FDBStreaming (MonadStream, Stream, StreamPersisted(FDB), runJob, streamFromTopic, runPure, listTopics)
import FDBStreaming.Topic (Topic(topicName), makeTopic, writeTopicIO, TopicName, getTopicCount)
import FDBStreaming.Message (Message (fromMessage, toMessage))
import qualified FDBStreaming.JobConfig as JC
import qualified Data.Map as Map
import Data.Map (Map)

import qualified FoundationDB as FDB
import qualified Control.Logger.Simple as Log

topicCounts :: FDB.Database
            -> JC.JobConfig
            -> (forall m . MonadStream m => m b)
            -> IO (Map TopicName Int)
topicCounts db cfg topology = do
  let tcs = listTopics cfg topology
  FDB.runTransaction db $ fmap Map.fromList $ for tcs $ \tc -> do
    c <- getTopicCount tc
    return (topicName tc, fromIntegral c)

waitCountUnchanged :: FDB.Database
                   -> JC.JobConfig
                   -> (forall m . MonadStream m => m b)
                   -> IO ()
waitCountUnchanged db cfg topology = do
  tc1 <- topicCounts db cfg topology
  threadDelay 1000000
  tc2 <- topicCounts db cfg topology
  if tc1 == tc2 then return () else waitCountUnchanged db cfg topology

-- | Given a small collection of input, and a topology that takes a stream of
-- the same type as input, run the pipeline on that input. Blocks until activity
-- in the pipeline appears to have stopped. Expect this to take a few seconds to
-- run.
testOnInput :: (Message a)
            => JC.JobConfig
            -> [a]
            -> (forall m. MonadStream m => Stream 'FDB a -> m b)
            -> IO b
testOnInput cfg xs topology = do
  -- TODO: what if user has already named a stream "test_input_stream"?
  let inTopic = makeTopic (JC.jobConfigSS cfg) "test_input_stream" 2 0
  void $ writeTopicIO (JC.jobConfigDB cfg) inTopic (fmap toMessage xs)
  let strm = fromMessage <$> streamFromTopic inTopic "test_input_stream"
  job <- async $ runJob cfg (topology strm)
  -- TODO: this is probably incredibly brittle.
  waitCountUnchanged (JC.jobConfigDB cfg) cfg (topology strm)
  cancel job
  return $ runPure cfg (topology strm)

-- | create a JobConfig with simple defaults suitable for running tests on small
-- amounts of data.
testJobConfig :: FDB.Database -> JC.JobSubspace -> JC.JobConfig
testJobConfig db ss = JC.JobConfig
  { JC.jobConfigDB = db
  , JC.jobConfigSS = ss
  , JC.streamMetricsStore = Nothing
  , JC.msgsPerBatch = 100
  , JC.leaseDuration = 5
  , JC.numStreamThreads = 8
  , JC.numPeriodicJobThreads = 1
  , JC.defaultNumPartitions = 2
  , JC.defaultChunkSizeBytes = 0
  , JC.logLevel = Log.LogError
  }
