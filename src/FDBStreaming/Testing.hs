{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Utilities for testing pipelines.
module FDBStreaming.Testing
  ( testOnInput,
    testOnInput2,
    testJobConfig,
    dumpStream,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel)
import qualified Control.Logger.Simple as Log
import Control.Monad (void)
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Traversable (for)
import FDBStreaming (MonadStream, Stream, StreamPersisted (FDB), listTopics, runJob, runPure, streamFromTopic, streamTopic)
import qualified FDBStreaming.JobConfig as JC
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.Topic (Topic (topicName), TopicName, getEntireTopic, getTopicCount, makeTopic, writeTopicIO)
import qualified FoundationDB as FDB
import GHC.Exts (IsList (toList))

topicCounts ::
  FDB.Database ->
  JC.JobConfig ->
  (forall m. MonadStream m => m b) ->
  IO (Map TopicName Int)
topicCounts db cfg topology = do
  let tcs = listTopics cfg topology
  FDB.runTransaction db $ fmap Map.fromList $ for tcs $ \tc -> do
    c <- getTopicCount tc
    return (topicName tc, fromIntegral c)

-- | Returns True if at least one topic count is nonzero, or if there are no
-- topics.
topicCountsNonZero :: Map TopicName Int -> Bool
topicCountsNonZero m = Map.null m || any (> 0) (Map.elems m)

waitCountUnchanged ::
  FDB.Database ->
  JC.JobConfig ->
  (forall m. MonadStream m => m b) ->
  -- | Max iterations
  Int ->
  IO ()
waitCountUnchanged _ _ _ 0 = putStrLn "WARN: test job timed out"
waitCountUnchanged db cfg topology maxIter = do
  tc1 <- topicCounts db cfg topology
  threadDelay 5000000
  tc2 <- topicCounts db cfg topology
  if tc1 == tc2 && topicCountsNonZero tc2
    then return ()
    else waitCountUnchanged db cfg topology (maxIter - 1)

-- | Given a small collection of input, and a topology that takes a stream of
-- the same type as input, run the pipeline on that input. Blocks until activity
-- in the pipeline appears to have stopped. Expect this to take >10 seconds to
-- run.
testOnInput ::
  (Message a) =>
  JC.JobConfig ->
  [a] ->
  (forall m. MonadStream m => Stream 'FDB a -> m b) ->
  IO b
testOnInput cfg xs topology = do
  -- TODO: what if user has already named a stream "test_input_stream"?
  let inTopic = makeTopic (JC.jobConfigSS cfg) "test_input_stream" 2 0
  void $ writeTopicIO (JC.jobConfigDB cfg) inTopic (fmap toMessage xs)
  let strm = fromMessage <$> streamFromTopic inTopic "test_input_stream"
  job <- async $ runJob cfg (topology strm)
  waitCountUnchanged (JC.jobConfigDB cfg) cfg (topology strm) 10
  cancel job
  return $ runPure cfg (topology strm)

-- | Test on two input streams.
testOnInput2 ::
  (Message a, Message b) =>
  JC.JobConfig ->
  [a] ->
  [b] ->
  (forall m. MonadStream m => Stream 'FDB a -> Stream 'FDB b -> m c) ->
  IO c
testOnInput2 cfg xs ys topology = do
  -- TODO: what if user has already named a stream "test_input_stream"?
  let inTopicXs = makeTopic (JC.jobConfigSS cfg) "test_input_stream" 2 0
  let inTopicYs = makeTopic (JC.jobConfigSS cfg) "test_input_stream2" 2 0
  void $ writeTopicIO (JC.jobConfigDB cfg) inTopicXs (fmap toMessage xs)
  void $ writeTopicIO (JC.jobConfigDB cfg) inTopicYs (fmap toMessage ys)
  let strmXs = fromMessage <$> streamFromTopic inTopicXs "test_input_stream"
  let strmYs = fromMessage <$> streamFromTopic inTopicYs "test_input_stream2"
  job <- async $ runJob cfg (topology strmXs strmYs)
  waitCountUnchanged (JC.jobConfigDB cfg) cfg (topology strmXs strmYs) 10
  cancel job
  return $ runPure cfg (topology strmXs strmYs)

-- | create a JobConfig with simple defaults suitable for running tests on small
-- amounts of data.
testJobConfig :: FDB.Database -> JC.JobSubspace -> JC.JobConfig
testJobConfig db ss = JC.JobConfig
  { JC.jobConfigDB = db,
    JC.jobConfigSS = ss,
    JC.streamMetricsStore = Nothing,
    JC.msgsPerBatch = 100,
    JC.leaseDuration = 5,
    JC.numStreamThreads = 8,
    JC.numPeriodicJobThreads = 1,
    JC.defaultNumPartitions = 2,
    JC.defaultChunkSizeBytes = 0,
    JC.logLevel = Log.LogInfo,
    JC.tasksToCleanUp = []
  }

-- | Get the entire contents of a stream. For testing purposes only.
dumpStream :: Message a => FDB.Database -> Stream 'FDB a -> IO [a]
dumpStream db stream = do
  let topic = streamTopic stream
  pidRecordMap <- FDB.runTransaction db $ getEntireTopic topic
  return
    $ fmap fromMessage
    $ mconcat
    $ toList
    $ fmap snd
    $ mconcat
    $ Map.elems pidRecordMap
