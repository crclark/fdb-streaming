{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module FDBStreaming.Topic
  ( TopicName,
    ReaderName,
    readerSS,
    PartitionId,
    Topic (..),
    makeTopic,
    randPartition,
    watchPartition,
    getCheckpoint',
    getCheckpoints,
    readNAndCheckpoint,
    readNAndCheckpoint',
    writeTopic,
    writeTopic',
    getPartitionCount,
    getTopicCount,
    listExistingTopics,
    watchTopic,
    watchTopic',
    awaitTopicOrTimeout,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
  ( async,
    race,
    waitAny,
  )
import Control.Monad (forM, guard, void)
import Data.Binary.Get
  ( getWord64le,
    runGet,
  )
import Data.Binary.Put
  ( putWord64le,
    runPut,
  )
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Foldable (foldlM)
import Data.Maybe (fromMaybe)
import Data.Sequence (Seq ((:|>)))
import Data.Word
  ( Word16,
    Word64,
    Word8,
  )
-- TODO: move prefixRangeEnd out of Advanced usage section.

import qualified FDBStreaming.Topic.Constants as C
import qualified FoundationDB as FDB
import FoundationDB as FDB (Database, FutureIO, KeySelector (FirstGreaterOrEq, FirstGreaterThan), Range (Range), Transaction, atomicOp, await, awaitIO, getKey, runTransaction, watch, withSnapshot)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options as Op
import FoundationDB.Transaction (prefixRangeEnd)
import FoundationDB.Versionstamp
  ( Versionstamp
      ( IncompleteVersionstamp
      ),
    VersionstampCompleteness (Complete),
    decodeVersionstamp,
    encodeVersionstamp,
  )
import System.Random (randomRIO)

-- | Integer zero, little endian
zeroLE :: ByteString
zeroLE = "\x00\x00\x00\x00\x00\x00\x00\x00"

-- | The unique name of a topic.
type TopicName = ByteString

-- | The name of a registered consumer of a topic. Each consumer of a topic
-- must have a unique name.
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
-- before the versionstamp. See the record layer paper for more info. This might
-- no longer be needed in recent versions of FDB.

-- | Represents an append-only collection of messages, stored in FoundationDB.
-- This collection can be efficiently written to using only FoundationDB atomic
-- ops, eliminating the possibility of transaction conflicts on writes. The
-- data structure is split into n partitions to allow n readers to read and
-- checkpoint their place in the topic without conflicting with one another.
-- While we currently expose the fields of this type, they should not be
-- modified unless you really know what you're doing.
data Topic
  = Topic
      { -- | top-level container for all topics used by
        -- this application.
        topicSS :: FDB.Subspace,
        topicName :: TopicName,
        -- | Key containing the count of items in the topic, as a little endian
        -- integer.
        topicCountKey :: ByteString,
        -- | Returns the subspace containing messages for a given partition.
        partitionMsgsSS :: PartitionId -> FDB.Subspace,
        partitionCountKey :: PartitionId -> ByteString,
        -- | Number of partitions in this topic. In the future, this will be
        -- stored in FoundationDB and allowed to dynamically grow.
        numPartitions :: Integer,
        -- | A subspace for application-specific metadata
        -- to be used by users of this topic. Nothing in
        -- this module will read or write to this
        -- subspace.
        topicCustomMetadataSS :: FDB.Subspace
      }

-- | Create a new topic, contained within the given subspace and with the given
-- name. You may reuse the same subspace for any number of topics -- each topic
-- will be stored in a separate subspace below it.
makeTopic :: FDB.Subspace -> TopicName -> Topic
makeTopic topicSS topicName = Topic {..}
  where
    topicCountKey = FDB.pack topicCountSS []
    partitionMsgsSS i = FDB.extend msgsSS [FDB.Int i]
    partitionCountKey i = FDB.pack topicCountSS [FDB.Int i]
    msgsSS = FDB.extend topicSS [C.topics, FDB.Bytes topicName, C.messages]
    topicCountSS =
      FDB.extend
        topicSS
        [C.topics, FDB.Bytes topicName, C.metaCount]
    numPartitions = 2 -- TODO: make configurable
    topicCustomMetadataSS =
      FDB.extend topicSS [C.topics, FDB.Bytes topicName, C.customMeta]

randPartition :: Topic -> IO PartitionId
randPartition Topic {..} = randomRIO (0, numPartitions - 1)

-- TODO: not efficient from either a Haskell or FDB perspective.
-- | Returns all topics that currently exist in the given subspace. This function
-- is not particularly efficient and shouldn't be used too heavily.
listExistingTopics :: FDB.Database -> FDB.Subspace -> IO [Topic]
listExistingTopics db ss = runTransaction db $ go (FDB.pack ss [C.topics])
  where
    go :: ByteString -> Transaction [Topic]
    go k = do
      k' <- getKey (FirstGreaterThan k) >>= await
      case FDB.unpack ss k' of
        Right (FDB.Int 0 : FDB.Bytes topicName : _) -> do
          let nextK = FDB.pack ss [C.topics, FDB.Bytes topicName] <> "0xff"
          rest <- go nextK
          let conf = makeTopic ss topicName
          return (conf : rest)
        _ -> return []

-- | Increments the count of messages in a given topic.
incrTopicCountBy :: Topic -> Word64 -> Transaction ()
incrTopicCountBy conf n = do
  let k = topicCountKey conf
  let bs = runPut $ putWord64le n
  FDB.atomicOp k (Op.add $ toStrict bs)

getTopicCount :: Topic -> Transaction Word64
getTopicCount conf = do
  let k = topicCountKey conf
  bs <- fromMaybe zeroLE <$> (FDB.get k >>= await)
  return $ (runGet getWord64le . fromStrict) bs

-- | Increments the count of messages in a given topic partition.
incrPartitionCountBy :: Topic -> PartitionId -> Word64 -> Transaction ()
incrPartitionCountBy conf pid n = do
  let k = partitionCountKey conf pid
  let bs = runPut $ putWord64le n
  FDB.atomicOp k (Op.add $ toStrict bs)

getPartitionCount :: Topic -> PartitionId -> Transaction Word64
getPartitionCount conf i = do
  let k = partitionCountKey conf i
  bs <- fromMaybe zeroLE <$> (FDB.get k >>= await)
  return $ (runGet getWord64le . fromStrict) bs

-- | A private subspace for a consumer of a topic. The consumer may record
-- anything it wants in this subspace.
readerSS :: Topic -> ReaderName -> FDB.Subspace
readerSS Topic {..} rn =
  FDB.extend topicSS [C.topics, FDB.Bytes topicName, C.readers, FDB.Bytes rn]

-- | The subspace containing checkpoints for a given topic consumer.
readerCheckpointSS :: Topic -> ReaderName -> FDB.Subspace
readerCheckpointSS tc rn = FDB.extend (readerSS tc rn) [C.checkpoint]

readerCheckpointKey :: Topic -> PartitionId -> ReaderName -> ByteString
readerCheckpointKey tc i rn = FDB.pack (readerCheckpointSS tc rn) [FDB.Int i]

-- TODO: make idempotent to deal with CommitUnknownResult

-- | Write a batch of messages to this topic.
-- Danger!! It's possible to write multiple messages with the same key
-- if this is called more than once in a single transaction.
writeTopic' ::
  Traversable t =>
  Topic ->
  PartitionId ->
  t ByteString ->
  Transaction ()
writeTopic' tc@Topic {..} p bss = do
  incrPartitionCountBy tc p (fromIntegral $ length bss)
  incrTopicCountBy tc (fromIntegral $ length bss)
  void $ foldlM go 1 bss
  where
    go !i bs = do
      let vs = IncompleteVersionstamp i
      let k = FDB.pack (partitionMsgsSS p) [FDB.IncompleteVS vs]
      FDB.atomicOp k (Op.setVersionstampedKey bs)
      return (i + 1)

-- TODO: support messages larger than FDB size limit, via chunking.

-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
-- DANGER: can only be called once per topic per transaction.
writeTopic ::
  Traversable t =>
  FDB.Database ->
  Topic ->
  t ByteString ->
  IO ()
writeTopic db tc@Topic {..} bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  p <- randPartition tc
  FDB.runTransaction db $ writeTopic' tc p bss

parseOutput ::
  Topic ->
  PartitionId ->
  (ByteString, ByteString) ->
  (Versionstamp 'Complete, ByteString)
parseOutput Topic {..} p (k, v) = case FDB.unpack (partitionMsgsSS p) k of
  Right [FDB.CompleteVS vs] -> (vs, v)
  Right t -> error $ "unexpected tuple: " ++ show t
  Left err -> error $ "failed to decode " ++ show k ++ " because " ++ show err

newtype TopicWatch = TopicWatch {unTopicWatch :: [FutureIO PartitionId]}
  deriving (Show)

-- | Returns a FoundationDB watch for a given partition of a topic. If any
-- messages are written to this partition, the watch will fire.
-- Currently, streaming jobs do not use this functionality, because watches do
-- not seem to fire promptly enough to maintain real time throughput. However,
-- other advanced use cases may find this useful.
watchPartition :: Topic -> PartitionId -> Transaction (FutureIO ())
watchPartition tc pid = watch (partitionCountKey tc pid)

-- | Returns a watch for each partition of the given topic.
-- Caveats:
-- 1. if numPartitions * numReaders is large, this could exhaust the
--    max number of watches in FDB (default is 10k).
-- 2. could increase conflicts if all readers are doing this and the write freq
-- is low -- everyone would wake up and try to read from the same partition each
-- time there's a write.
--
-- Currently, streaming jobs do not use this functionality, because watches do
-- not seem to fire promptly enough to maintain real time throughput. However,
-- other advanced use cases may find this useful.
watchTopic' :: Topic -> Transaction TopicWatch
watchTopic' tc = fmap TopicWatch $ forM [0 .. numPartitions tc - 1] $ \pid ->
  fmap (const pid) <$> watch (partitionCountKey tc pid)

-- | For use with the return value of 'watchTopic''. Must be called from outside
-- the transaction within which 'watchTopic'' was called.
awaitTopic :: TopicWatch -> IO (Either FDB.Error PartitionId)
awaitTopic (TopicWatch fs) = do
  (_, x) <- mapM (async . awaitIO) fs >>= waitAny
  return x

-- | Waits at most n microseconds for a new message to be written to the given
-- set of partitions. Returns the first partition to be written to within the
-- time limit, if any. Returns 'Nothing' on timout.
awaitTopicOrTimeout :: Int -> TopicWatch -> IO (Maybe PartitionId)
awaitTopicOrTimeout timeout futures =
  race (threadDelay timeout) (awaitTopic futures) >>= \case
    Left _ -> return Nothing
    Right (Left _) -> return Nothing -- TODO: handle errors better
    Right (Right p) -> return (Just p)

-- | Block until a new message is written to the given topic.
--
-- Currently, streaming jobs do not use this functionality, because watches do
-- not seem to fire promptly enough to maintain real time throughput. However,
-- other advanced use cases may find this useful.
watchTopic :: FDB.Database -> Topic -> IO (Either FDB.Error PartitionId)
watchTopic db tc = do
  ws <- runTransaction db (watchTopic' tc)
  awaitTopic ws

checkpoint' ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Versionstamp 'Complete ->
  Transaction ()
checkpoint' tc p rn vs = do
  let k = readerCheckpointKey tc p rn
  let v = encodeVersionstamp vs
  FDB.atomicOp k (Op.byteMax v)

decodeCheckpoint :: ByteString -> Versionstamp 'Complete
decodeCheckpoint bs = case decodeVersionstamp bs of
  Nothing -> error $ "Failed to decode checkpoint: " ++ show bs
  Just vs -> vs

-- | For a given reader, returns a versionstamp that is guaranteed to be less
-- than the first unread message in the topic partition. If the reader
-- hasn't made a checkpoint yet, returns a versionstamp containing all zeros.
getCheckpoint' ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Transaction (Versionstamp 'Complete)
getCheckpoint' tc p rn = do
  let cpk = readerCheckpointKey tc p rn
  bs <- FDB.get cpk >>= await
  case decodeCheckpoint <$> bs of
    Just vs -> return vs
    Nothing -> return minBound

-- | Return all checkpoints for all partitions for a reader.
getCheckpoints ::
  Topic ->
  ReaderName ->
  Transaction (FDB.Future (Seq (Versionstamp 'Complete)))
getCheckpoints tc rn = do
  let ss = readerCheckpointSS tc rn
  let ssRange = FDB.subspaceRange ss
  rangeResult <- FDB.getRange' ssRange FDB.StreamingModeWantAll
  return
    $ flip fmap rangeResult
    $ \case
      FDB.RangeDone kvs -> fmap (decodeCheckpoint . snd) kvs
      -- TODO: we need to be able to register monadic callbacks on
      -- futures to handle this.
      FDB.RangeMore _ _ -> error "Internal error: unexpectedly large number of partitions"

readNPastCheckpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Word8 ->
  Transaction (Seq (Versionstamp 'Complete, ByteString))
readNPastCheckpoint tc p rn n = do
  cpvs <- getCheckpoint' tc p rn
  let begin = FDB.pack (partitionMsgsSS tc p) [FDB.CompleteVS cpvs]
  let end = prefixRangeEnd $ FDB.subspaceKey (partitionMsgsSS tc p)
  let r = Range
        { rangeBegin = FirstGreaterThan begin,
          rangeEnd = FirstGreaterOrEq end,
          rangeLimit = Just (fromIntegral n),
          rangeReverse = False
        }
  fmap (parseOutput tc p) <$> withSnapshot (FDB.getEntireRange r)

-- TODO: would be useful to have a version of this that returns a watch if
-- there are no new messages.
-- NOTE: doing readNPastCheckpoint as a snapshot read breaks the exactly-once
-- guarantee.

-- | Read N messages from the given topic partition. These messages are guaranteed
-- to be previously unseen for the given ReaderName, and will be checkpointed so
--that they are never seen again.
readNAndCheckpoint' ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Word8 ->
  Transaction (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint' tc@Topic {..} p rn n =
  readNPastCheckpoint tc p rn n >>= \case
    x@(_ :|> (vs, _)) -> do
      checkpoint' tc p rn vs
      return x
    _ -> return mempty

-- | Read N messages from a random partition of the given topic, and checkpoints
-- so that they will never be seen by the same reader again.
readNAndCheckpoint ::
  FDB.Database ->
  Topic ->
  ReaderName ->
  Word8 ->
  IO (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint db tc@Topic {..} rn n = do
  p <- randPartition tc
  FDB.runTransaction db (readNAndCheckpoint' tc p rn n)
