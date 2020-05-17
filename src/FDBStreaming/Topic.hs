{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module FDBStreaming.Topic
  ( TopicName,
    ReaderName,
    readerSS,
    PartitionId,
    Topic (..),
    makeTopic,
    randPartition,
    watchPartition,
    Checkpoint(..),
    getCheckpoint,
    getCheckpoints,
    readNAndCheckpoint,
    readNAndCheckpointIO,
    writeTopic,
    writeTopicIO,
    getPartitionCount,
    getTopicCount,
    listExistingTopics,
    watchTopic,
    watchTopicIO,
    awaitTopicOrTimeout,
    getEntireTopic
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
  ( async,
    race,
    waitAny,
  )
import Data.Binary.Get
  ( getWord64le,
    runGet,
  )
import Data.Binary.Put
  ( putWord64le,
    runPut,
  )
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Foldable (forM_)
import Data.Function ((&))
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import qualified Data.Sequence as Seq
import Data.Sequence (Seq ((:|>)))
import Data.Traversable (for)
import Data.Word
  ( Word16,
    Word64,
    Word8,
  )

import FDBStreaming.Util (chunksOfSize, streamlyRangeResult)
import qualified FDBStreaming.Topic.Constants as C
import qualified FoundationDB as FDB
import FoundationDB as FDB (Database, FutureIO, KeySelector (FirstGreaterOrEq, FirstGreaterThan), Range (Range), Transaction, atomicOp, await, awaitIO, getKey, runTransaction, watch)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options.MutationType as Mut
import FoundationDB.Transaction (prefixRangeEnd)
import FoundationDB.Versionstamp
  ( Versionstamp
      ( IncompleteVersionstamp
      ),
    VersionstampCompleteness (Complete),
  )
import qualified Streamly.Prelude as S
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
type PartitionId = Word8

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
        -- | Returns the subspace containing messages for a given partition.
        partitionMsgsSS :: PartitionId -> FDB.Subspace,
        -- | Key containing the count of messages for a given partition.
        partitionCountKey :: PartitionId -> ByteString,
        -- | Desired number of bytes to pack into each FDB value storing the
        -- messages of this topic. For small messages, this can greatly
        -- improve throughput and reduce FDB storage usage. Set to 0
        -- to disable this behavior.
        desiredChunkSizeBytes :: Word,
        -- | Number of partitions in this topic. In the future, this will be
        -- stored in FoundationDB and allowed to dynamically grow.
        numPartitions :: Word8,
        -- | A subspace for application-specific metadata
        -- to be used by users of this topic. Nothing in
        -- this module will read or write to this
        -- subspace.
        topicCustomMetadataSS :: FDB.Subspace
      }

-- | Create a new topic, contained within the given subspace and with the given
-- name. You may reuse the same subspace for any number of topics -- each topic
-- will be stored in a separate subspace below it.
makeTopic :: FDB.Subspace
          -- ^ Top-level subspace containing all topics used by this application.
          -> TopicName
          -- ^ Name of the topic. Must be unique to the given subspace.
          -> Word8
          -- ^ Number of partitions in this topic. The number of partitions
          -- bounds the number of consumers that may concurrently read from this
          -- topic.
          -> Word
          -- ^ Desired number of bytes to pack into each FDB value storing the
          -- messages of this topic. For small messages, this can greatly
          -- increase throughput and reduce FDB storage usage. Set to 0
          -- to disable this behavior.
          -> Topic
makeTopic topicSS topicName p desiredChunkSizeBytes = Topic {..}
  where
    partitionMsgsSS i = FDB.extend msgsSS [FDB.Int (fromIntegral i)]
    partitionCountKey i = FDB.pack topicCountSS [FDB.Int (fromIntegral i)]
    msgsSS = FDB.extend topicSS [C.topics, FDB.Bytes topicName, C.messages]
    topicCountSS =
      FDB.extend
        topicSS
        [C.topics, FDB.Bytes topicName, C.metaCount]
    numPartitions = p
    topicCustomMetadataSS =
      FDB.extend topicSS [C.topics, FDB.Bytes topicName, C.customMeta]

randPartition :: Topic -> IO PartitionId
randPartition Topic {..} = randomRIO (0, numPartitions - 1)

-- TODO: not efficient from either a Haskell or FDB perspective.
-- | Returns all topics that currently exist in the given subspace. This function
-- is not particularly efficient and shouldn't be used too heavily.
--
-- DANGER: For debugging purposes only. 'desiredChunkSizeBytes' and 'numPartitions'
-- are not currently stored in FoundationDB -- it's only known from your code. Thus,
-- the returned Topic values have incorrect partition counts. This will be
-- fixed in the future. https://github.com/crclark/fdb-streaming/issues/3
-- This means that the result of 'getTopicCount' will be incorrect for topics
-- returned by this function.
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
          let conf = makeTopic ss topicName 1 0
          return (conf : rest)
        _ -> return []

getPartitionCountF :: Topic -> PartitionId -> Transaction (FDB.Future Word64)
getPartitionCountF topic i = do
  let k = partitionCountKey topic i
  fmap (maybe 0 (runGet getWord64le . fromStrict)) <$> FDB.get k

getTopicCount :: Topic -> Transaction Word64
getTopicCount topic = do
  fs <- for [0 .. numPartitions topic - 1] (getPartitionCountF topic)
  cs <- traverse await fs
  return (sum cs)


-- | Increments the count of messages in a given topic partition.
incrPartitionCountBy :: Topic -> PartitionId -> Word64 -> Transaction ()
incrPartitionCountBy topic pid n = do
  let k = partitionCountKey topic pid
  let bs = runPut $ putWord64le n
  FDB.atomicOp k (Mut.add $ toStrict bs)

getPartitionCount :: Topic -> PartitionId -> Transaction Word64
getPartitionCount topic i = do
  let k = partitionCountKey topic i
  bs <- fromMaybe zeroLE <$> (FDB.get k >>= await)
  return $ (runGet getWord64le . fromStrict) bs

-- | A private subspace for a consumer of a topic. The consumer may record
-- anything it wants in this subspace.
readerSS :: Topic -> ReaderName -> FDB.Subspace
readerSS Topic {..} rn =
  FDB.extend topicSS [C.topics, FDB.Bytes topicName, C.readers, FDB.Bytes rn]

-- | The subspace containing checkpoints for a given topic consumer.
readerCheckpointSS :: Topic -> ReaderName -> FDB.Subspace
readerCheckpointSS topic rn = FDB.extend (readerSS topic rn) [C.checkpoint]

readerCheckpointKey :: Topic -> PartitionId -> ReaderName -> ByteString
readerCheckpointKey topic i rn =
  FDB.pack (readerCheckpointSS topic rn) [FDB.Int (fromIntegral i)]

-- | Write a batch of messages to this topic.
-- Danger!! It's possible to write multiple messages with the same key
-- if this is called more than once in a single transaction.
--
-- Returns the FDB keys (containing incomplete versionstamps) corresponding to
-- where each item in the input batch was written, along with the integer index
-- describing where in the array of values written to that key the item was
-- placed (see 'desiredChunkSizeBytes' for details).
writeTopic ::
  Topic ->
  PartitionId ->
  --TODO watch for regression from switching to list here
  [ByteString] ->
  Transaction [(ByteString, Int)]
writeTopic topic@Topic {..} p bss = do
  incrPartitionCountBy topic p (fromIntegral $ length bss)
  let chunks = chunksOfSize desiredChunkSizeBytes BS.length bss
  let mkKey i = FDB.pack
                  (partitionMsgsSS p)
                  [FDB.IncompleteVS (IncompleteVersionstamp i)]
  let keyedIndexedChunks = [ (mkKey i, zip [0..] chunk)
                           | (i, chunk) <- zip [0..] chunks
                           ]
  forM_ keyedIndexedChunks $ \(k, bs) -> do
    let v = FDB.encodeTupleElems (map (FDB.Bytes . snd) bs)
    FDB.atomicOp k (Mut.setVersionstampedKey v)
  return [(k, i) | (k,cs) <- keyedIndexedChunks, (i,_) <- cs]

-- TODO: support messages larger than FDB size limit, via chunking.

-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
-- Exposed primarily for testing purposes. This operation is not idempotent,
-- and if it throws CommitUnknownResult, you won't know whether the write
-- succeeded. For more robust write utilities,
-- see 'FDBStreaming.Util.BatchWriter'.
writeTopicIO ::
  FDB.Database ->
  Topic ->
  [ByteString] ->
  IO [(ByteString, Int)]
writeTopicIO db topic@Topic {..} bss = do
  p <- randPartition topic
  FDB.runTransaction db $ writeTopic topic p bss

parseTopicKV ::
  Topic ->
  PartitionId ->
  (ByteString, ByteString) ->
  (Versionstamp 'Complete, [ByteString])
parseTopicKV Topic {..} p (k, v) =
  case (FDB.unpack (partitionMsgsSS p) k, FDB.decodeTupleElems v) of
    (Right [FDB.CompleteVS vs], Right bs) | allBytes bs -> (vs, (unBytes bs))
    (Right [FDB.CompleteVS _], Right _) -> error "unexpected topic chunk format"
    (Right t, _) -> error $ "unexpected tuple: " ++ show t
    (Left err, _) -> error $ "failed to decode " ++ show k ++ " because " ++ show err


    where
      allBytes []                 = True
      allBytes (FDB.Bytes _ : xs) = allBytes xs
      allBytes _                  = False

      unBytes []                 = []
      unBytes (FDB.Bytes x : xs) = x : unBytes xs
      unBytes _                  = error "unreachable case in parseTopicKV"

-- | Parse a topic key/value pair (in the format (versionstamp, sequence of
-- messages)) to a list of messages, each paired with the Checkpoint that points
-- to that message. This checkpoint is the checkpoint that we
-- would use if we were to stop reading at that message.
topicKVToCheckpointMessage ::
  Topic ->
  PartitionId ->
  (ByteString, ByteString)
  -> [(Checkpoint, ByteString)]
topicKVToCheckpointMessage t pid kv =
  let (vs, msgs) = parseTopicKV t pid kv
      imsgs = zip [0..] msgs
      in [(Checkpoint vs i, msg) | (i, msg) <- imsgs]

newtype TopicWatch = TopicWatch {unTopicWatch :: [FutureIO PartitionId]}
  deriving (Show)

-- | Returns a FoundationDB watch for a given partition of a topic. If any
-- messages are written to this partition, the watch will fire.
-- Currently, streaming jobs do not use this functionality, because watches do
-- not seem to fire promptly enough to maintain real time throughput. However,
-- other advanced use cases may find this useful.
watchPartition :: Topic -> PartitionId -> Transaction (FutureIO ())
watchPartition topic pid = watch (partitionCountKey topic pid)

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
watchTopic :: Topic -> Transaction TopicWatch
watchTopic tc = fmap TopicWatch $ for [0 .. numPartitions tc - 1] $ \pid ->
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
watchTopicIO :: FDB.Database -> Topic -> IO (Either FDB.Error PartitionId)
watchTopicIO db topic = do
  ws <- runTransaction db (watchTopic topic)
  awaitTopic ws

-- | A checkpoint consists of a versionstamp, indicating which key we last read
-- from, and an integer. The integer is an index inside of the list of
-- bytestrings stored at the versionstamp key, indicating the last bytestring we
-- read within that key.
--
-- Recall that we pack multiple messages into each FDB key/value pair using a
-- chunking scheme (see 'desiredChunkSizeBytes'). So when the reader of the
-- topic asks for the next n messages, the batch of messages might stop in the
-- middle of a chunk. Thus, we need to remember where within the chunk we
-- stopped. This of course has the downside that we transmit some extra data
-- on each read and throw it away, but the hope is that the efficiency gain of
-- packing more data into each key/value pair makes up for it.
--
-- For example, if our topic key/values look like
--
-- @k1, [b0, b1, b2]@
-- @k2, [b0, b1, b2]@
-- @k3, [b0, b1, b2]@
--
-- Where each k is a versionstamp key and each b is a message within that key's
-- value (chunk), then if the current checkpoint is (k2, 1), we have already
-- seen all messages up to and including k2,b1. The next batch of messages we
-- read will start at k2,b2.
data Checkpoint = Checkpoint (Versionstamp 'Complete) Int
  deriving (Show, Eq, Ord, Bounded)

checkpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Checkpoint ->
  Transaction ()
checkpoint topic p rn (Checkpoint vs i) = do
  let k = readerCheckpointKey topic p rn
  let v = FDB.encodeTupleElems [FDB.CompleteVS vs, FDB.Int (fromIntegral i)]
  FDB.atomicOp k (Mut.byteMax v)

decodeCheckpoint :: ByteString -> Checkpoint
decodeCheckpoint bs = case FDB.decodeTupleElems bs of
  Left err -> error $ "Failed to decode checkpoint: " ++ err
  (Right [FDB.CompleteVS vs, FDB.Int i]) -> Checkpoint vs (fromIntegral i)
  (Right _) -> error "unexpected bytes while decoding checkpoint "

-- | For a given reader, returns a 'Checkpoint' that is guaranteed to be less
-- than the first unread message in the topic partition. If the reader
-- hasn't made a checkpoint yet, returns a Checkpoint containing all zeros.
getCheckpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Transaction Checkpoint
getCheckpoint topic p rn = do
  let cpk = readerCheckpointKey topic p rn
  bs <- FDB.get cpk >>= await
  case decodeCheckpoint <$> bs of
    Just vs -> return vs
    Nothing -> return (Checkpoint minBound (-1))

-- | Return all checkpoints for all partitions for a reader.
getCheckpoints ::
  Topic ->
  ReaderName ->
  Transaction (FDB.Future (Seq Checkpoint))
getCheckpoints topic rn = do
  let ss = readerCheckpointSS topic rn
  let ssRange = FDB.subspaceRange ss
  rangeResult <- FDB.getRange' ssRange FDB.StreamingModeWantAll
  return
    $ flip fmap rangeResult
    $ \case
      FDB.RangeDone kvs -> fmap (decodeCheckpoint . snd) kvs
      -- TODO: we need to be able to register monadic callbacks on
      -- futures to handle this.
      FDB.RangeMore _ _ -> error "Internal error: unexpectedly large number of partitions"


-- | Read n messages past the given checkpoint. The message pointed to by the
-- checkpoint is not included in the results -- remember that checkpoints point
-- at the last message acknowledged to have been read.
-- Returns a sequence of messages, and a Checkpoint pointing at the last
-- message in that sequence.
-- WARNING: wrapping readNPastCheckpoint with 'withSnapshot' breaks the
-- exactly-once guarantee.
-- TODO: in some cases this will return a checkpoint that points at the last
-- message of a chunk. In that case, when we read again, we unnecessarily read
-- that key again, even though there are no more unread messages in it. Could we
-- make Checkpoint a sum type that can encode when we should only read the next
-- key, not including the one of the current checkpoint?
readNPastCheckpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Word16 ->
  Transaction (Seq (Checkpoint, ByteString))
readNPastCheckpoint topic pid rn n = do
  ckpt@(Checkpoint cpvs _) <- getCheckpoint topic pid rn
  let begin = FDB.pack (partitionMsgsSS topic pid) [FDB.CompleteVS cpvs]
  let end = prefixRangeEnd $ FDB.subspaceKey (partitionMsgsSS topic pid)
  let r = Range
        { rangeBegin = FirstGreaterOrEq begin,
          rangeEnd = FirstGreaterOrEq end,
          -- +1 because the checkpoint can point at a fully-read kv, in which
          -- case we don't want it to count towards the limit. See above TODO.
          rangeLimit = Just (fromIntegral n + 1),
          rangeReverse = False
        }
  rr <- FDB.getRange' r FDB.StreamingModeSmall >>= FDB.await
  streamlyRangeResult rr
    -- Parse bytestrings to Stream of chunks: Stream [(Checkpoint, ByteString)]
    & fmap (topicKVToCheckpointMessage topic pid)
    -- flatten chunks into top-level stream: Stream (Checkpoint, ByteString)
    & S.concatMap S.fromFoldable
    -- Drop msgs that we have already processed
    & S.dropWhile ((<= ckpt) . fst)
    -- take the requested number of msgs
    & S.take (fromIntegral n)
    & S.toList
    & fmap Seq.fromList

-- | Read N messages from the given topic partition. These messages are guaranteed
-- to be previously unseen for the given ReaderName, and will be checkpointed so
--that they are never seen again.
readNAndCheckpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Word16 ->
  Transaction (Seq (Checkpoint, ByteString))
readNAndCheckpoint topic@Topic {..} p rn n =
  readNPastCheckpoint topic p rn n >>= \case
    x@(_ :|> (ckpt, _)) -> do
      checkpoint topic p rn ckpt
      return x
    x -> return x

-- | Read N messages from a random partition of the given topic, and checkpoints
-- so that they will never be seen by the same reader again.
readNAndCheckpointIO ::
  FDB.Database ->
  Topic ->
  ReaderName ->
  Word16 ->
  IO (Seq (Checkpoint, ByteString))
readNAndCheckpointIO db topic@Topic {..} rn n = do
  p <- randPartition topic
  FDB.runTransaction db (readNAndCheckpoint topic p rn n)

-- | Gets all data from the given topic. For debugging and testing purposes.
-- This will try to load the entire topic into memory.
getEntireTopic ::
  Topic ->
  Transaction (Map PartitionId (Seq (Versionstamp 'Complete, [ByteString])))
getEntireTopic topic@Topic {..} = do
  partitions <- for [0 .. numPartitions - 1] $ \pid -> do
    let begin = FDB.pack (partitionMsgsSS pid) [FDB.CompleteVS minBound]
    let end = prefixRangeEnd $ FDB.subspaceKey (partitionMsgsSS pid)
    let r = Range
          { rangeBegin = FirstGreaterThan begin,
            rangeEnd = FirstGreaterOrEq end,
            rangeLimit = Nothing,
            rangeReverse = False
          }
    (pid,) . fmap (parseTopicKV topic pid) <$> FDB.getEntireRange r
  return $ Map.fromList partitions
