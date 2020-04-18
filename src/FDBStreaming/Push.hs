{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

-- (Temporary) module for prototyping an HTTP input/output interface for
-- stream pipelines.
--
-- A quick-and-dirty solution would be really easy, but we want a nice solution.
-- Ideally, it might look something like this:
--
-- > pipeline :: MonadStream m => m ()
-- > pipeline = do
-- >   xs <- httpInput HttpConfig { acceptInput = parseJsonToTweetOrRejectReq
-- >                              , }
-- >   topic1 <- pipeStep "topic1" dostuff xs
-- >   topic2 <- indexBy "user" user
-- >               $ indexBy "tweet_ngrams" ngrams
-- >               $ pipeStep "topic2" dostuff2 xs
-- >   serveHttp "tweets" topic2 -- serves with queries by above indices
-- >   table1 <- aggregate "userTweetCount" (groupBy user xs) ...
-- >   serveHttp "users/counts" table1 --serves, looking up by table's key
--
-- Here's another way we might do it:
--
-- > pipeline :: MonadStream m => Stream Tweet -> m (Topic Tweet, Table User Int)
-- > pipeline xs = do
-- >   topic1 <- pipeStep "topic1" dostuff xs
-- >   topic2 <- indexBy "user" user
-- >               $ indexBy "tweet_ngrams" ngrams
-- >               $ pipeStep "topic2" dostuff2 xs
-- >   table1 <- aggregate "userTweetCount" (groupBy user xs) ...
-- >   return (topic2, table1)
--
-- > main :: IO ()
-- > main = do
-- >   let xs = makeStream "tweets"
-- >   (topic2, table1) <- runJob pipeline
-- >   serveHttp { inputs = [(xs, configAndStuffHere)]
-- >             , outputs = [(topic2, cfg1), (table1, cfg2)] }
--
-- This second way of doing it is less terse, but it separates concerns better.
-- Http access to existing topic/table can be considered a view of the job, that
-- really has nothing to do with the definition of the job's topology, which
-- has up until now been the sole purpose of the MonadStream API. However,
-- defining HTTP input to a job within the job is probably okay. OTOH, what if
-- we want to write to a topic in five different ways -- pull from Kafka and
-- write manually with HTTP? It's weird, but conceivable. Imagine one is used
-- for manual intervention and the other is the default input mechanism, for
-- example.
--
-- Here are some practical problems we will need to deal with when we work on
-- this.
--
-- ### Input
--
-- Input is probably the hardest part of this. Things we need to figure out:
--
-- 1. Fusion? Above, @xs@ gets fed to @topic1@ and @topic2@. Right now, for
-- Kafka input, we create one consumer on the Kafka side for each topic, so we
-- have double the traffic from Kafka than what we strictly need, but each topic
-- is free to move forward in time at its own pace. For HTTP, we don't have this
-- luxury -- the client is pushing data to us; we are not pulling data from it.
-- Therefore, we have only "one chance" to write each message to all of its
-- downstream topics. If we only write to half, we have lost a message! We need
-- to fuse all of the downstream topic writes into one big write, then
-- acknowledge the write. This is surprising to users who are used to separate
-- topic workers working separately -- what if all the writes take lots of CPU
-- and fusing them causes timeouts?
-- 2. Coordinating web server threads. They need to run outside of the lease
-- system -- we can't have servers appearing and disappearing from our worker
-- nodes. Should @runJob@ be responsible for forking off a server? It seems
-- likely that some users won't even need this functionality, and even more would
-- want to run the server in a separate process for ease of monitoring,
-- so we would need
-- a config flag to turn it off. In which case, it might as well be a separate
-- function. But if it's a separate function, that seems to be a case against
-- the first API described above -- it's surprising that @runJob@ doesn't run
-- essential parts of the job, and that we instead must pass the pipeline into
-- two separate run functions to get it all up and running!
--
-- Given the above, I have a new idea. A separate interface for running HTTP
-- inputs into the FDBStreaming system. The API would be something like
--
-- httpInput :: HTTPConfig -> StepName -> StreamStep a b -> m b
--
-- httpInput's output would be in whatever monad or other type that the
-- underlying web framework uses.
--
-- Thinking more, we would actually need a separate API for making push-based
-- callbacks for StreamSteps in general. So we need something like
--
-- pushTopic :: StepName -> (a -> IO (Maybe b)) -> StreamStep a b (Stream b)
--
-- This first one gives us a way to create a StreamStep that we can embellish
-- with the builder interface -- adding watermarks, indexes, whatever.
--
-- makePushCallback :: StreamStep a b out -> (out, Seq a -> Transaction ())
--
-- returning a handle to the output stream and a callback to feed the step with
-- data. Ditto for tables. Not sure how we would do a join directly between
-- HTTP inputs and an existing topic, though.
--
-- Another downside is that we would need to duplicate a lot of functionality
-- from FDBStreaming. The produceStep and aggregateStep functions, I think, would
-- need to be duplicated -- and at that point, what does a StreamStep really
-- represent? A convoluted function for transforming one Stream into another?
-- Why is some of the functionality in produceStep and some stored in the StreamStep
-- record?
--
-- produceStep does things that are relevant only to the lease-based system. It has
-- the job config, it records metrics, etc. But it also handles choosing which
-- partition to write to. Maybe it's not a big deal to replicate it -- it does
-- very little. Hmm, but watermarking happens after that step. We really need
-- to replicate @run@, not just @produceStep@. But, again, what is a streamstep?
-- If we were to take the StreamStep produced by the new function in this
-- namespace, pass it into a MonadStream topology, and @run@ it there, would it
-- work correctly? I think so, but could we accidentally introduce changes in
-- the future that makes it no longer true? Alternatively, if we return a
-- StreamStep from a MonadStream and feed it to makePushCallback, chaos would
-- ensue -- its Stream input fields would be ignored, which is really confusing.
-- Could we fix that by removing the stream input fields, and moving them to be
-- params to 'run' instead? Or we could newtype StreamStep to PushStep and
-- wrap all the watermarkBy etc functions and export them all from this module.
-- That would be weird, though, because people would likely be using both this
-- and the normal FDBStreaming stuff in the same module, so they would be
-- wondering why they need both Push.watermarkBy and watermarkBy...
--
-- Better idea: new PushStream and PushTable types, no sharing with StreamStep.
-- Implement their behavior in this module, out of the more basic parts from
-- other modules. If there is some vital behavior that's only in FDBStreaming,
-- it should be moved into another module and re-used. For stuff like
-- watermarkBy, introduce type classes like Watermarkable.
--
--TODO: delete the above brainstorming and replace with real docs once we decide
-- what to do.

-- | Utilities for pushing data into a stream from outside the stream
-- processing pipeline. This can to be used to insert incoming data from
-- HTTP requests, for example.
--
-- In order to make inserts from outside the system idempotent (which is
-- necessary to avoid duplicated writes if we receive CommitUnknownResult
-- errors), this module's interface uses 'BatchWriter's, which allow you to
-- specify an idempotencyKey for each item you insert.
module FDBStreaming.Push
  ( PushStreamConfig (..),
    runPushStream,
    runPushStream',
  )
where

import Control.Monad (replicateM)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (for_)
import Data.Maybe (fromMaybe, isJust)
import Data.Word (Word16, Word8)
import qualified FDBStreaming.JobConfig as JC
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.Stream (Stream, StreamName)
import FDBStreaming.Stream.Internal (streamFromTopic, setStreamWatermarkByTopic)
import FDBStreaming.Topic (makeTopic, randPartition, topicCustomMetadataSS, writeTopic)
import FDBStreaming.Util.BatchWriter (BatchWriter, BatchWriterConfig, batchWriter, defaultBatchWriterConfig)
import FDBStreaming.Watermark (Watermark, setWatermark, topicWatermarkSS)
import FoundationDB (Transaction)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB

-- | Specifies the configuration of a stream that will be populated by pushing
-- messages to it from outside the pipeline system.
data PushStreamConfig inMsg
  = PushStreamConfig
      { pushStreamWatermarkBy :: Maybe (inMsg -> Transaction Watermark),
        -- TODO: reuse StepConfig for options below?
        pushStreamOutputPartitions :: Maybe Word8,
        pushStreamBatchSize :: Maybe Word16
      }

defaultPushStreamConfig :: PushStreamConfig a
defaultPushStreamConfig = PushStreamConfig Nothing Nothing Nothing

-- | Set up a stream that messages can be pushed to from outside the pipeline.
-- Returns the stream and a set of BatchWriters for writing to it. The stream
-- can be passed into a 'MonadStream' action to serve as a root of a pipeline
-- DAG.
runPushStream ::
  Message inMsg =>
  -- | Job configuration. Must match the JobConfig of whatever
  -- pipeline you want to share the output stream with.
  JC.JobConfig ->
  StreamName ->
  PushStreamConfig inMsg ->
  -- | Config for batch writers
  BatchWriterConfig ->
  -- | Number of batch writers to create
  Word ->
  IO (Stream inMsg, [BatchWriter inMsg])
runPushStream jc sn ps bwc bn = do
  let numPartitions =
        fromMaybe
          (JC.defaultNumPartitions jc)
          (pushStreamOutputPartitions ps)
  let topic = makeTopic (JC.jobConfigSS jc) sn numPartitions
  let stream = (if isJust (pushStreamWatermarkBy ps)
                   then setStreamWatermarkByTopic
                   else id)
                 $ streamFromTopic topic sn
  let writeBatch xs = do
        for_ (pushStreamWatermarkBy ps) $ \wf -> case xs of
          (x : _) -> do
            w <- wf x
            setWatermark (topicWatermarkSS topic) w
          _ -> return ()
        pid <- liftIO $ randPartition topic
        writeTopic topic pid (fmap toMessage xs)
  let bwSS = FDB.extend (topicCustomMetadataSS topic) [FDB.Bytes "pbw"]
  writers <- replicateM (fromIntegral bn) $ batchWriter bwc (JC.jobConfigDB jc) bwSS writeBatch
  return (fmap fromMessage stream, writers)

-- | Simplified version of 'runPushStream' using basic default configuration.
runPushStream' ::
  Message inMsg =>
  JC.JobConfig ->
  StreamName ->
  IO (Stream inMsg, BatchWriter inMsg)
runPushStream' jc sn =
  fmap head <$> runPushStream jc sn defaultPushStreamConfig defaultBatchWriterConfig 1

-- TODO: pushAggrTable interface.
