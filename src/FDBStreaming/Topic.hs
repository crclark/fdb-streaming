{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming.Topic
  ( TopicName,
    ReaderName,
    readerSS,
    PartitionId,
    Topic (..),
    makeTopic,
    randPartition,
    watchPartition,
    Checkpoint (..),
    getCheckpoint,
    getCheckpoints,
    readNAndCheckpoint,
    readNAndCheckpointIO,
    writeTopic,
    CoordinateUncommitted (..),
    Coordinate (..),
    checkpointToCoordinate,
    coordinateToCheckpoint,
    get,
    writeTopicIO,
    getPartitionCount,
    getTopicCount,
    listExistingTopics,
    watchTopic,
    watchTopicIO,
    awaitTopicOrTimeout,

    -- * Advanced usage
    getEntireTopic,
    peekNPastCheckpoint,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
  ( async,
    race,
    waitAny,
  )
import Data.Binary.Put
  ( putWord64le,
    runPut,
  )
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (forM_)
import Data.Function ((&))
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Sequence as Seq
import Data.Sequence (Seq ((:|>)))
import Data.Traversable (for)
import Data.Word
  ( Word16,
    Word64,
    Word8,
  )
import qualified FDBStreaming.Topic.Constants as C
import FDBStreaming.Util (chunksOfSize, streamlyRangeResult, parseWord64le)
import qualified FoundationDB as FDB
import FoundationDB as FDB (Database, FutureIO, KeySelector (FirstGreaterOrEq, FirstGreaterThan), RangeQuery (RangeQuery), Transaction, atomicOp, await, awaitIO, getKey, runTransaction, watch)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options.MutationType as Mut
import FoundationDB.Transaction (prefixRangeEnd)
import FoundationDB.Versionstamp
  ( Versionstamp
      ( IncompleteVersionstamp
      ),
    VersionstampCompleteness (Complete, Incomplete),
  )
import GHC.Generics (Generic)
import Safe (fromJustNote)
import qualified Streamly.Prelude as S
import System.Random (randomRIO)
import Data.ByteString.Lazy.Char8 (toStrict)

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

-- | Identifies a single message in a topic. This must be committed by passing
-- it to an indexing function in the same transaction in which this coordinate
-- was created with 'writeTopic'. After the transaction is committed, other
-- indexing functions can be used to fetch the coordinate as a 'Coordinate',
-- which can be passed to 'get'.
data CoordinateUncommitted
  = CoordinateUncommitted PartitionId (Versionstamp 'Incomplete) Int
  deriving (Eq, Show, Ord, Generic)

-- | Identifies a single message in a topic.
data Coordinate = Coordinate PartitionId (Versionstamp 'Complete) Int
  deriving (Eq, Show, Ord, Generic)

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

-- TODO: it seems that Coordinates are more general than checkpoints. Can we
-- remove checkpoints as a separate type? Or make checkpoints a newtype for
-- coordinates? They do serve two different purposes, but they both ultimately
-- point at a particular message.

-- | Convert a coordinate to a checkpoint. The difference between the two is
-- that a coordinate contains a PartitionId and a checkpoint does not, because
-- checkpoints are stored per partition.
coordinateToCheckpoint :: Coordinate -> Checkpoint
coordinateToCheckpoint (Coordinate _ vs i) = Checkpoint vs i

checkpointToCoordinate :: PartitionId -> Checkpoint -> Coordinate
checkpointToCoordinate pid (Checkpoint vs i) = Coordinate pid vs i

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
        -- | Subspace containing all partitions.
        msgsSS :: FDB.Subspace,
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
makeTopic ::
  -- | Top-level subspace containing all topics used by this application.
  FDB.Subspace ->
  -- | Name of the topic. Must be unique to the given subspace.
  TopicName ->
  -- | Number of partitions in this topic. The number of partitions
  -- bounds the number of consumers that may concurrently read from this
  -- topic.
  Word8 ->
  -- | Desired number of bytes to pack into each FDB value storing the
  -- messages of this topic. For small messages, this can greatly
  -- increase throughput and reduce FDB storage usage. Set to 0
  -- to disable this behavior.
  Word ->
  Topic
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
  fmap (maybe 0 parseWord64le) <$> FDB.get k

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
  bs <- FDB.get k >>= await
  return $ maybe 0 parseWord64le bs

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
-- Returns the coordinate corresponding to
-- where each item in the input batch was written.
--
-- Because the returned Coordinate contains an incomplete versionstamp, the only
-- way to
-- remember where each message was written is to write the keys somewhere else
-- in the same transaction, or to get the committed version of the transaction
-- after it completes. Once you have a Coordinate with a complete versionstamp,
-- you can pass it to 'get' to recover the message.
writeTopic ::
  Topic ->
  PartitionId ->
  --TODO watch for regression from switching to list here
  [ByteString] ->
  Transaction [CoordinateUncommitted]
writeTopic topic@Topic {..} p bss = do
  incrPartitionCountBy topic p (fromIntegral $ length bss)
  let chunks = chunksOfSize desiredChunkSizeBytes BS.length bss
  let mkTuple i =
        [ FDB.Int $ fromIntegral p,
          FDB.IncompleteVS (IncompleteVersionstamp i)
        ]
  let mkCoord pid i = CoordinateUncommitted pid (IncompleteVersionstamp i)
  let keyedIndexedChunks =
        [ (mkCoord p i, FDB.pack msgsSS t, zip [0 ..] chunk)
          | (i, chunk) <- zip [0 ..] chunks,
            let t = mkTuple i
        ]
  forM_ keyedIndexedChunks $ \(_, k, bs) -> do
    -- TODO: this has the unhappy side effect of increasing the size of
    -- messages encoded by the persist library significantly. persist pads with
    -- \x00, which FDB's tuple layer uses as a marker for the end of bytes, so
    -- all the \x00 bytes get escaped with \ff by the tuple layer encoding.
    -- In other words, every \x00 needs two bytes to encode.
    let v = FDB.encodeTupleElems (map (FDB.Bytes . snd) bs)
    FDB.atomicOp k (Mut.setVersionstampedKey v)
  return [m i | (m, _, cs) <- keyedIndexedChunks, (i, _) <- cs]

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
  IO [CoordinateUncommitted]
writeTopicIO db topic bss = do
  p <- randPartition topic
  FDB.runTransaction db $ writeTopic topic p bss

parseTopicV :: ByteString -> [ByteString]
parseTopicV vals = case FDB.decodeTupleElems vals of
  Right bs | allBytes bs -> unBytes bs
  _ -> error "parseTopicV: unexpected topic chunk format"
  where
    allBytes [] = True
    allBytes (FDB.Bytes _ : xs) = allBytes xs
    allBytes _ = False
    unBytes [] = []
    unBytes (FDB.Bytes x : xs) = x : unBytes xs
    unBytes _ = error "unreachable case in parseTopicKV"

parseTopicKV ::
  Topic ->
  (ByteString, ByteString) ->
  (Versionstamp 'Complete, [ByteString])
parseTopicKV Topic {..} (k, v) =
  case (FDB.unpack msgsSS k, parseTopicV v) of
    (Right [FDB.Int _, FDB.CompleteVS vs], bs) -> (vs, bs)
    (Right t, _) -> error $ "unexpected key tuple: " ++ show t
    (Left err, _) -> error $ "failed to decode " ++ show k ++ "with msgsSS " ++ show msgsSS ++ " because " ++ show err

-- | Parse a topic key/value pair (in the format (versionstamp, sequence of
-- messages)) to a list of messages, each paired with the Coordinate that points
-- to that message. This coordinate can be used to produce the checkpoint that
-- we would use if we were to stop reading at that message.
topicKVToCoordinateMessage ::
  Topic ->
  PartitionId ->
  (ByteString, ByteString) ->
  [(Coordinate, ByteString)]
topicKVToCoordinateMessage t pid kv =
  let (vs, msgs) = parseTopicKV t kv
      imsgs = zip [0 ..] msgs
   in [(Coordinate pid vs i, msg) | (i, msg) <- imsgs]

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
getCheckpoint' ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Transaction (FDB.Future Checkpoint)
getCheckpoint' topic p rn = do
  let cpk = readerCheckpointKey topic p rn
  fmap (maybe (Checkpoint minBound (-1)) decodeCheckpoint) <$> FDB.get cpk

getCheckpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Transaction Checkpoint
getCheckpoint topic p rn =
  getCheckpoint' topic p rn >>= FDB.await

-- | Return all checkpoints for all partitions for a reader.
getCheckpoints ::
  Topic ->
  ReaderName ->
  Transaction (FDB.Future (Seq Checkpoint))
getCheckpoints topic rn = do
  let pids = Seq.fromList [0 .. numPartitions topic - 1]
  sequenceA <$> traverse (\pid -> getCheckpoint' topic pid rn) pids

-- | Read n messages past 'ReaderName''s current checkpoint. Does not update the
-- checkpoint.
peekNPastCheckpoint ::
  -- The message pointed to by the
  -- checkpoint is not included in the results -- remember that checkpoints point
  -- at the last message acknowledged to have been read.
  -- Returns a sequence of messages, and a Checkpoint pointing at the last
  -- message in that sequence.
  -- WARNING: wrapping peekNPastCheckpoint with 'withSnapshot' breaks the
  -- exactly-once guarantee.
  -- TODO: in some cases this will return a checkpoint that points at the last
  -- message of a chunk. In that case, when we read again, we unnecessarily read
  -- that key again, even though there are no more unread messages in it. Could we
  -- make Checkpoint a sum type that can encode when we should only read the next
  -- key, not including the one of the current checkpoint?

  Topic ->
  PartitionId ->
  ReaderName ->
  Word16 ->
  Transaction (Seq (Coordinate, ByteString))
peekNPastCheckpoint topic pid rn n = do
  ckpt@(Checkpoint cpvs _) <- getCheckpoint topic pid rn
  let begin = FDB.pack (partitionMsgsSS topic pid) [FDB.CompleteVS cpvs]
  let end = fromJustNote "peekNPastCheckpoint: impossible subspace range"
            $ prefixRangeEnd
            $ FDB.subspaceKey (partitionMsgsSS topic pid)

  let r = RangeQuery
        { rangeBegin = FirstGreaterOrEq begin,
          rangeEnd = FirstGreaterOrEq end,
          -- +1 because the checkpoint can point at a fully-read kv, in which
          -- case we don't want it to count towards the limit. See above TODO.
          rangeLimit = Just (fromIntegral n + 1),
          rangeReverse = False
        }
  rr <- FDB.withSnapshot (FDB.getRange' r FDB.StreamingModeSmall) >>= FDB.await
  streamlyRangeResult rr
    -- Parse bytestrings to Stream of chunks: Stream [(Coordinate, ByteString)]
    & fmap (topicKVToCoordinateMessage topic pid)
    -- flatten chunks into top-level stream: Stream (Coordinate, ByteString)
    & S.concatMap S.fromFoldable
    -- Drop msgs that we have already processed
    & S.dropWhile ((<= ckpt) . coordinateToCheckpoint . fst)
    -- take the requested number of msgs
    & S.take (fromIntegral n)
    & S.toList
    & fmap Seq.fromList

-- | Read N messages from the given topic partition. These messages are guaranteed
-- to be previously unseen for the given ReaderName, and will be checkpointed so
-- that they are never seen again.
readNAndCheckpoint ::
  Topic ->
  PartitionId ->
  ReaderName ->
  Word16 ->
  Transaction (Seq (Coordinate, ByteString))
readNAndCheckpoint topic p rn n =
  peekNPastCheckpoint topic p rn n >>= \case
    x@(_ :|> (coord, _)) -> do
      checkpoint topic p rn (coordinateToCheckpoint coord)
      return x
    x -> return x

-- | Given a 'Coordinate' pointing at a message, get that message. See 'writeTopic'
-- for more details on how to get a coordinate to pass to this function. Returns
-- 'Nothing' if the message does not exist in the topic. If that happens,
-- perhaps you passed a coordinate from one topic to another topic.
-- Note: there is probably no need for further optimization to support the use
-- case of fetching many Coordinates that are packed into the same key/value
-- pair, because the FDB client already caches reads of the same key within a
-- transaction.
get :: Topic -> Coordinate -> Transaction (FDB.Future (Maybe ByteString))
get Topic {msgsSS} (Coordinate pid vs i) = do
  let k = FDB.pack msgsSS [FDB.Int $ fromIntegral pid, FDB.CompleteVS vs]
  fv <- FDB.get k
  return $ fmap (fmap getFrom) fv
  where
    getFrom bs = (!! i) $ parseTopicV bs

-- | Read N messages from a random partition of the given topic, and checkpoints
-- so that they will never be seen by the same reader again.
readNAndCheckpointIO ::
  FDB.Database ->
  Topic ->
  ReaderName ->
  Word16 ->
  IO (Seq (Coordinate, ByteString))
readNAndCheckpointIO db topic rn n = do
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
    let end = fromJustNote "getEntireTopic: impossible subspace range"
              $ prefixRangeEnd
              $ FDB.subspaceKey (partitionMsgsSS pid)

    let r = RangeQuery
          { rangeBegin = FirstGreaterThan begin,
            rangeEnd = FirstGreaterOrEq end,
            rangeLimit = Nothing,
            rangeReverse = False
          }
    (pid,) . fmap (parseTopicKV topic) <$> FDB.getEntireRange r
  return $ Map.fromList partitions
