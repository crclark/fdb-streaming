{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module FDBStreaming.Topic where

import Control.Concurrent.Async (async, waitAny)
import Control.Monad
import Data.Binary.Get ( runGet
                       , getWord64le)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict)
import Data.Foldable (foldlM)
import Data.Maybe (fromMaybe)
import Data.Sequence (Seq(..))
import Data.Word (Word8, Word16, Word64)
import FoundationDB as FDB
import FoundationDB.Options as Op
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
import System.Random (randomRIO)

-- TODO: something less insane
infRetry :: TransactionConfig
infRetry = FDB.defaultConfig {maxRetries = maxBound}

zeroLE :: ByteString
zeroLE = "\x00\x00\x00\x00\x00\x00\x00\x00"

-- | integer one, little endian encoded
oneLE :: ByteString
oneLE = "\x01\x00\x00\x00\x00\x00\x00\x00"

type TopicName = ByteString

type ReaderName = ByteString

-- | Topics are partitioned to scale reads -- since a read essentially reads a
-- checkpoint, grabs messages after that checkpoint, then writes to the
-- checkpoint, the checkpoint is highly contended. By splitting a topic into a
-- separate partitions, readers are able to maintain a separate checkpoint per
-- partition, and reads scale better.
type PartitionId = Integer

-- TODO: consider switching to the directory layer so the subspace strings
-- are shorter
-- TODO: the current schema uses versionstamps in a way that prevents data from
-- being transferred to a new database, because versionstamps are only
-- monotonically increasing for a given DB. We need an "incarnation" prefix
-- before the versionstamp. See the record layer paper for more info.
data TopicConfig = TopicConfig { topicConfigDB :: FDB.Database
                               , topicSS :: FDB.Subspace
                               -- ^ top-level container for all topics used by
                               -- this application.
                               , topicName :: TopicName
                               , topicCountKey :: ByteString
                               , partitionMsgsSS :: PartitionId -> FDB.Subspace
                               , partitionCountKey :: PartitionId -> ByteString
                               , numPartitions :: Integer
                               -- ^ TODO: don't export
                               }

makeTopicConfig :: FDB.Database -> FDB.Subspace -> TopicName -> TopicConfig
makeTopicConfig topicConfigDB topicSS topicName = TopicConfig{..}
  where
    topicCountKey = FDB.pack topicCountSS []

    partitionMsgsSS i = FDB.extend msgsSS [Int i]

    partitionCountKey i = FDB.pack topicCountSS [Int i]

    msgsSS = FDB.extend topicSS [ Bytes "tpcs"
                                , Bytes topicName
                                , Bytes "msgs"
                                ]

    topicCountSS = FDB.extend topicSS [ Bytes "tpcs"
                                      , Bytes topicName
                                      , Bytes "meta"
                                      , Bytes "count"
                                      ]
    numPartitions = 10 -- TODO: make configurable

randPartition :: TopicConfig -> IO PartitionId
randPartition TopicConfig{..} = fromIntegral <$> randomRIO (0,numPartitions)

-- TODO: not efficient from either a Haskell or FDB perspective.
listExistingTopics :: FDB.Database -> FDB.Subspace -> IO [TopicConfig]
listExistingTopics db ss = runTransaction db $ go (FDB.pack ss [Bytes "tpcs"])
  where go :: ByteString -> Transaction [TopicConfig]
        go k = do
          k' <- getKey (FirstGreaterThan k) >>= await
          case FDB.unpack ss k' of
            Right (Bytes "tpcs" : Bytes topicName : _) -> do
              let nextK = FDB.pack ss [Bytes "tpcs", Bytes topicName] <> "0xff"
              rest <- go nextK
              let conf = makeTopicConfig db ss topicName
              return (conf : rest)
            _ -> return []

incrTopicCount :: TopicConfig
               -> Transaction ()
incrTopicCount conf = do
  let k = topicCountKey conf
  FDB.atomicOp k (Op.add oneLE)

getTopicCount :: TopicConfig
              -> Transaction Word64
getTopicCount conf = do
  let k = topicCountKey conf
  bs <- fromMaybe zeroLE <$> (FDB.get k >>= await)
  return $ (runGet getWord64le . fromStrict) bs

incrPartitionCount :: TopicConfig
                   -> PartitionId
                   -> Transaction ()
incrPartitionCount conf i = do
  let k = partitionCountKey conf i
  FDB.atomicOp k (Op.add oneLE)

getPartitionCount :: TopicConfig
                  -> PartitionId
                  -> Transaction Word64
getPartitionCount conf i = do
  let k = partitionCountKey conf i
  bs <- fromMaybe zeroLE <$> (FDB.get k >>= await)
  return $ (runGet getWord64le . fromStrict) bs

readerSS :: TopicConfig -> ReaderName -> Subspace
readerSS TopicConfig{..} rn =
  extend topicSS [Bytes "tpcs", Bytes topicName, Bytes "rdrs", Bytes rn]

readerCheckpointKey :: TopicConfig
                    -> PartitionId
                    -> ReaderName
                    -> ByteString
readerCheckpointKey tc i rn =
  FDB.pack (readerSS tc rn) [Int i, Bytes "ckpt"]

-- | Danger!! It's possible to write multiple messages with the same key
-- if this is called more than once in a single transaction.
writeTopic' :: Traversable t
            => TopicConfig
            -> PartitionId
            -> t ByteString
            -> Transaction ()
writeTopic' tc@TopicConfig{..} p bss =
  void $ foldlM go 1 bss
    where
      go !i bs = do
        let vs = IncompleteVersionstamp i
        let k = FDB.pack (partitionMsgsSS p) [FDB.IncompleteVS vs]
        FDB.atomicOp k (setVersionstampedKey bs)
        incrTopicCount tc
        incrPartitionCount tc p
        return (i+1)

-- TODO: support messages larger than FDB size limit, via chunking.
-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
-- DANGER: can only be called once per topic per transaction.
writeTopic :: Traversable t
           => TopicConfig
           -> t ByteString
           -> IO ()
writeTopic tc@TopicConfig{..} bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  p <- randPartition tc
  FDB.runTransaction topicConfigDB $ writeTopic' tc p bss

trOutput :: TopicConfig
         -> PartitionId
         -> (ByteString, ByteString)
         -> (Versionstamp 'Complete, ByteString)
trOutput TopicConfig{..} p (k,v) =
  case FDB.unpack (partitionMsgsSS p) k of
    Right [CompleteVS vs] -> (vs, v)
    Right t -> error $ "unexpected tuple: " ++ show t
    Left err -> error $ "failed to decode "
                        ++ show k
                        ++ " because "
                        ++ show err

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

-- | Returns a watch for each partition of the given topic.
-- Caveats:
-- 1. if numPartitions * numReaders is large, this could exhaust the
--    max number of watches in FDB (default is 10k).
-- 2. could increase conflicts if all readers are doing this and the write freq
-- is low -- everyone would wake up and try to read from the same partition each
-- time there's a write.
watchTopic' :: TopicConfig -> Transaction [FutureIO PartitionId]
watchTopic' tc = forM [0..numPartitions tc] $ \p ->
  fmap (const p) <$> watch (partitionCountKey tc p)

-- | For use with the return value of 'watchTopic''. Must be called from outside
-- the transaction within which 'watchTopic'' was called.
awaitTopic :: [FutureIO PartitionId] -> IO (Either FDB.Error PartitionId)
awaitTopic fs = do
  (_,x) <- mapM (async . awaitIO) fs >>= waitAny
  return x

watchTopic :: TopicConfig -> IO (Either FDB.Error PartitionId)
watchTopic tc = do
  ws <- runTransaction (topicConfigDB tc) (watchTopic' tc)
  awaitTopic ws

checkpoint' :: TopicConfig
            -> PartitionId
            -> ReaderName
            -> Versionstamp 'Complete
            -> Transaction ()
checkpoint' tc p rn vs = do
  let k = readerCheckpointKey tc p rn
  let v = encodeVersionstamp vs
  FDB.atomicOp k (Op.byteMax v)

-- | For a given reader, returns a versionstamp that is guaranteed to be less
-- than the first uncheckpointed message in the topic partition. If the reader
-- hasn't made a checkpoint yet, returns a versionstamp containing all zeros.
getCheckpoint' :: TopicConfig
               -> PartitionId
               -> ReaderName
               -> Transaction (Versionstamp 'Complete)
getCheckpoint' tc p rn = do
  let cpk = readerCheckpointKey tc p rn
  bs <- get cpk >>= await
  case decodeVersionstamp <$> bs of
    Just Nothing -> error $ "Failed to decode checkpoint: " ++ show bs
    Just (Just vs) -> return vs
    Nothing -> return $ CompleteVersionstamp (TransactionVersionstamp 0 0) 0

readNPastCheckpoint :: TopicConfig
                    -> PartitionId
                    -> ReaderName
                    -> Word8
                    -> Transaction (Seq (Versionstamp 'Complete, ByteString))
readNPastCheckpoint tc p rn n = do
  cpvs <- getCheckpoint' tc p rn
  let begin = FDB.pack (partitionMsgsSS tc p) [CompleteVS cpvs]
  let end = prefixRangeEnd $ FDB.subspaceKey (partitionMsgsSS tc p)
  let r = Range { rangeBegin = FirstGreaterThan begin
                , rangeEnd = FirstGreaterOrEq end
                , rangeLimit = Just (fromIntegral n)
                , rangeReverse = False
                }
  fmap (trOutput tc p) <$> FDB.getEntireRange r

-- TODO: would be useful to have a version of this that returns a watch if
-- there are no new messages.
readNAndCheckpoint' :: TopicConfig
                    -> PartitionId
                    -> ReaderName
                    -> Word8
                    -> Transaction (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint' tc@TopicConfig{..} p rn n =
  readNPastCheckpoint tc p rn n >>= \case
    x@(_ :|> (vs,_)) -> do
      checkpoint' tc p rn vs
      return x
    _ -> return mempty

readNAndCheckpoint :: TopicConfig
                   -> ReaderName
                   -> Word8
                   -> IO (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint tc@TopicConfig{..} rn n = do
  p <- randPartition tc
  FDB.runTransactionWithConfig infRetry topicConfigDB (readNAndCheckpoint' tc p rn n)
