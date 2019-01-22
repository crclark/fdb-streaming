{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

module Lib where

import Control.Applicative
import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class
import Data.Binary.Get ( runGet
                       , getWord64le
                       , getWord32le
                       , getWord16le
                       , getWord8)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict)
import Data.Foldable (foldlM, toList)
import Data.Maybe (fromJust, catMaybes)
import qualified Data.Sequence as Seq
import Data.Sequence (Seq(..), ViewL(..))
import Data.Word (Word8, Word16, Word64)
import Data.Void
import FoundationDB as FDB
import FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Tuple as FDB
-- TODO: move prefixRangeEnd out of Advanced usage section.
import FoundationDB.Transaction (prefixRangeEnd)
import FoundationDB.Versionstamp (Versionstamp
                                  (CompleteVersionstamp,
                                   IncompleteVersionstamp),
                                  encodeVersionstamp,
                                  TransactionVersionstamp(..),
                                  VersionstampCompleteness(..),
                                  decodeVersionstamp)
import System.IO (stderr, hPutStrLn)

someFunc :: IO ()
someFunc = putStrLn "someFunc"

type TopicName = ByteString

type ReaderName = ByteString

data TopicConfig = TopicConfig { topicConfigDB :: FDB.Database
                               , topicSS :: FDB.Subspace
                               , topicName :: TopicName
                               , topicCountKey :: ByteString
                               , topicMsgsSS :: FDB.Subspace
                               , topicWriteOneKey :: ByteString
                               }
                               deriving Show

makeTopicConfig :: FDB.Database -> FDB.Subspace -> TopicName -> TopicConfig
makeTopicConfig topicConfigDB topicSS topicName = TopicConfig{..} where
  topicCountKey = FDB.pack topicSS [ BytesElem topicName
                                   , BytesElem "meta"
                                   , BytesElem "count"
                                   ]
  topicMsgsSS = FDB.extend topicSS [BytesElem topicName, BytesElem "msgs"]
  topicWriteOneKey = FDB.pack topicMsgsSS [FDB.IncompleteVSElem (IncompleteVersionstamp 0)]

incrTopicCount :: TopicConfig
               -> Transaction ()
incrTopicCount conf = do
  let k = topicCountKey conf
  let one = "\x01"
  FDB.atomicOp FDB.Add k one

getTopicCount :: TopicConfig
              -> Transaction (Maybe Word64)
getTopicCount conf = do
  let k = topicCountKey conf
  cBytes <- FDB.get k >>= await
  -- TODO: partial
  return $ fmap (runGet parse . fromStrict) cBytes
  where parse = getWord64le
                <|> fromIntegral <$> getWord32le
                <|> fromIntegral <$> getWord16le
                <|> fromIntegral <$> getWord8

readerCheckpointKey :: TopicConfig
                    -> ReaderName
                    -> ByteString
readerCheckpointKey TopicConfig{..} rn =
  FDB.pack topicSS [ BytesElem topicName
                   , BytesElem "readers"
                   , BytesElem rn
                   , BytesElem "ckpt"]

writeTopic' :: Traversable t
            => TopicConfig
            -> t ByteString
            -> Transaction ()
writeTopic' tc@TopicConfig{..} bss = do
  _ <- foldlM go 1 bss
  return ()
    where
      go !i bs = do
        let vs = IncompleteVersionstamp i
        let k = FDB.pack topicMsgsSS [FDB.IncompleteVSElem vs]
        FDB.atomicOp FDB.SetVersionstampedKey k bs
        incrTopicCount tc
        return (i+1)

-- TODO: support messages larger than FDB size limit, via chunking.
-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
writeTopic :: Traversable t
           => TopicConfig
           -> t ByteString
           -> IO ()
writeTopic tc@TopicConfig{..} bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  FDB.runTransaction topicConfigDB $ writeTopic' tc bss

-- | Optimized function for writing a single message to a topic. The key is
-- precomputed once, so no time needs to be spent computing it. This may be
-- faster than writing batches of keys. Profile the code to find out!
writeOneMsgTopic :: TopicConfig
                 -> ByteString
                 -> IO ()
writeOneMsgTopic tc@TopicConfig{..} bs = do
  FDB.runTransaction topicConfigDB $ do
    FDB.atomicOp FDB.SetVersionstampedKey topicWriteOneKey bs
    incrTopicCount tc

trOutput :: TopicConfig
         -> (ByteString, ByteString)
         -> (Versionstamp 'Complete, ByteString)
trOutput TopicConfig{..} (k,v) =
  case FDB.unpack topicMsgsSS k of
    Right [CompleteVSElem vs] -> (vs, v)
    Right t -> error $ "unexpected tuple: " ++ show t
    Left err -> error $ "failed to decode "
                        ++ show k
                        ++ " because "
                        ++ show err

readLastN :: TopicConfig
          -> Int
          -> IO (Seq (Versionstamp 'Complete, ByteString))
readLastN tc@TopicConfig{..} n =
  FDB.runTransaction topicConfigDB $ do
    let range = fromJust $
                FDB.prefixRange $
                FDB.subspaceKey topicMsgsSS
    let rangeN = range { rangeReverse = True, rangeLimit = Just n}
    fmap (trOutput tc) <$> FDB.getEntireRange rangeN

getNAfter :: TopicConfig
          -> Int
          -> Versionstamp 'Complete
          -> IO (Seq (Versionstamp 'Complete, ByteString))
getNAfter tc@TopicConfig{..} n vs =
  FDB.runTransaction topicConfigDB $ do
    let range = fromJust $
                FDB.prefixRange $
                FDB.pack topicMsgsSS [FDB.CompleteVSElem vs]
    let rangeN = range {rangeLimit = Just n}
    fmap (trOutput tc) <$> FDB.getEntireRange rangeN

-- TODO: should actually set the watch from the same transaction that did the
-- last read, so that we are guaranteed to be woken by the next write.
blockUntilNew :: TopicConfig -> IO ()
blockUntilNew conf@TopicConfig{..} = do
  let k = topicCountKey
  f <- FDB.runTransaction topicConfigDB (FDB.watch k)
  FDB.awaitIO f >>= \case
    Right () -> return ()
    Left err -> do
      hPutStrLn stderr $ "got error while watching: " ++ show err
      blockUntilNew conf

-- TODO: reader implementation
-- two versions: atomic read and checkpoint, non-atomic read and checkpoint

checkpoint' :: TopicConfig
            -> ReaderName
            -> Versionstamp 'Complete
            -> Transaction ()
checkpoint' tc rn vs = do
  let k = readerCheckpointKey tc rn
  let v = encodeVersionstamp vs
  FDB.atomicOp FDB.ByteMax k v

-- | For a given reader, returns a versionstamp that is guaranteed to be less
-- than the first uncheckpointed message in the topic. If the reader hasn't
-- made a checkpoint yet, returns a versionstamp containing all zeros.
getCheckpoint' :: TopicConfig
               -> ReaderName
               -> Transaction (Versionstamp 'Complete)
getCheckpoint' tc rn = do
  let cpk = readerCheckpointKey tc rn
  bs <- get cpk >>= await
  case decodeVersionstamp <$> bs of
    Just Nothing -> error $ "Failed to decode checkpoint: " ++ show bs
    Just (Just vs) -> return vs
    Nothing -> return $ CompleteVersionstamp (TransactionVersionstamp 0 0) 0

readNPastCheckpoint :: TopicConfig
                    -> ReaderName
                    -> Word8
                    -> Transaction (Seq (Versionstamp 'Complete, ByteString))
readNPastCheckpoint tc rn n = do
  cpvs <- getCheckpoint' tc rn
  let begin = FDB.pack (topicMsgsSS tc) [CompleteVSElem cpvs]
  let end = prefixRangeEnd $ FDB.subspaceKey (topicMsgsSS tc)
  let r = Range { rangeBegin = FirstGreaterThan begin
                , rangeEnd = FirstGreaterOrEq end
                , rangeLimit = Just (fromIntegral n)
                , rangeReverse = False
                }
  fmap (trOutput tc) <$> FDB.getEntireRange r

readNAndCheckpoint :: TopicConfig
                   -> ReaderName
                   -> Word8
                   -> IO (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint tc@TopicConfig{..} rn n = do
  FDB.runTransactionWithConfig conf topicConfigDB $
    readNPastCheckpoint tc rn n >>= \case
      (x@(_ :|> (vs,_))) -> do
        checkpoint' tc rn vs
        return x
      _ -> return mempty
  where conf = FDB.defaultConfig {maxRetries = maxBound}

-- | Exactly once delivery. Contention on a single key -- could be slow!
readAndCheckpoint :: TopicConfig
                  -> ReaderName
                  -> IO (Maybe (Versionstamp 'Complete, ByteString))
readAndCheckpoint tc@TopicConfig{..} rn =
  FDB.runTransactionWithConfig conf topicConfigDB $
    (Seq.viewl <$> readNPastCheckpoint tc rn 1) >>= \case
      EmptyL -> return Nothing
      (x@(vs,_) :< _) -> do
        checkpoint' tc rn vs
        return (Just x)
  where conf = FDB.defaultConfig {maxRetries = maxBound}

-- | At least once delivery. No contention.
nonAtomicReadThenCheckpoint :: TopicConfig
                            -> ReaderName
                            -> IO (Maybe (Versionstamp 'Complete, ByteString))
nonAtomicReadThenCheckpoint tc@TopicConfig{..} rn = do
  res <- FDB.runTransaction topicConfigDB (readNPastCheckpoint tc rn 1)
  case Seq.viewl res of
    EmptyL -> return Nothing
    (x@(vs,_) :< _) -> do
      FDB.runTransaction topicConfigDB (checkpoint' tc rn vs)
      return (Just x)

{-

High-level streaming combinators -- an experiment.

The streams need to be easy to maintain. How to change a topology over time is
an important consideration. What happens if I put a new step in the middle of
a pipeline? What happens if I remove a step? What happens if I change the logic
of a step?

After all of these modifications, what happens when I start my program again
with the existing DB? What happens if I do a rolling deployment, so some are
still running the old version of the code?

-}

-- TODO: error handling for bad parses
class Messageable a where
  toMessage :: a -> ByteString
  fromMessage :: ByteString -> a

-- TODO: argh, find a way to avoid this.
instance Messageable Void where
  toMessage = undefined
  fromMessage = undefined

type StreamName = ByteString

-- TODO: consider generalizing IO to m in future
-- TODO: what about state and folds?
-- TODO: what about truncating old data by timestamp?
-- TODO: probably shouldn't contain Topic info -- pass in DB connection and
-- build it based on StreamName, perhaps.
data Stream a b where
  StreamProducer :: Messageable b
                 => StreamName
                 -- Maybe allows for filtering
                 -> IO (Maybe b)
                 -> Stream Void b
  StreamConsumer :: (Messageable a, Messageable b)
                 => StreamName
                 -> Stream a b
                 -> (b -> IO ())
                 -> Stream a Void
  -- TODO: looks suspiciously similar to monadic bind
  StreamPipe :: (Messageable a, Messageable b, Messageable c)
             => StreamName
             -> Stream a b
             -> (b -> IO (Maybe c))
             -> Stream a c

streamName :: Stream a b -> StreamName
streamName (StreamProducer sn _) = sn
streamName (StreamConsumer sn _ _) = sn
streamName (StreamPipe sn _ _) = sn

-- TODO: other persistence backends
data FDBStreamConfig = FDBStreamConfig {
  streamConfigDB :: FDB.Database,
  streamConfigSS :: FDB.Subspace
}

inputTopic :: FDBStreamConfig -> Stream a b -> TopicConfig
-- TODO: actually StreamProducer has no input topic.
inputTopic sc (StreamProducer sn _) =
  makeTopicConfig (streamConfigDB sc) (streamConfigSS sc) sn
inputTopic sc (StreamConsumer _ inp _) = outputTopic sc inp
inputTopic sc (StreamPipe _ inp _) = outputTopic sc inp

outputTopic :: FDBStreamConfig -> Stream a b -> TopicConfig
-- TODO: StreamConsumer has no output
outputTopic sc s =
  makeTopicConfig (streamConfigDB sc)
                  (streamConfigSS sc)
                  (streamName s <> "_out")

-- | Runs a stream. For each StreamPipe in the tree, reads from its input topic
-- in chunks, runs the monadic action on each input, and writes the result to
-- its output topic.
-- TODO: handle cyclical inputs
runStream :: FDBStreamConfig -> Stream a b -> IO ()
runStream c@FDBStreamConfig{..} s@(StreamProducer _rn step) = do
  -- TODO: what if this thread dies?
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  let outCfg = outputTopic c s
  void $ forkIO $ forever $ do
    xs <- catMaybes <$> replicateM 10 step -- TODO: batch size config
    writeTopic outCfg (fmap toMessage xs)

runStream c@FDBStreamConfig{..} s@(StreamConsumer rn inp step) = do
  runStream c inp
  let inCfg = inputTopic c s
  void $ forkIO $ forever $ do
    -- TODO: if parsing the message fails, should we still checkpoint?
    -- TODO: blockUntilNew
    xs <- readNAndCheckpoint inCfg rn 10
    mapM_ step (fmap (fromMessage . snd) xs)

runStream c@FDBStreamConfig{..} s@(StreamPipe rn inp step) = do
  runStream c inp
  let inCfg = inputTopic c s
  let outCfg = outputTopic c s
  -- TODO: blockUntilNew
  void $ forkIO $ forever $ FDB.runTransactionWithConfig cnf (topicConfigDB inCfg) $ do
    xs <- readNPastCheckpoint inCfg rn 10 --TODO: auto-adjust batch size
    case xs of
      Empty -> return ()
      (_ :|> (vs,_)) -> do
        let inMsgs = fmap (fromMessage . snd) xs
        ys <- catMaybes . toList <$> liftIO (mapM step inMsgs)
        let outMsgs = fmap toMessage ys
        writeTopic' outCfg outMsgs
        checkpoint' inCfg rn vs
  -- TODO: auto-adjust retries?
  where cnf = FDB.defaultConfig {maxRetries = maxBound}

