{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module FDBStreaming.Topic where

import Control.Monad
import Data.Binary.Get ( runGet
                       , getWord64le)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
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

-- TODO: something less insane
infRetry :: TransactionConfig
infRetry = FDB.defaultConfig {maxRetries = maxBound}

type TopicName = ByteString

type ReaderName = ByteString

-- TODO: consider switching to the directory layer so the subspace strings
-- are shorter
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
  topicCountKey = FDB.pack topicSS [ Bytes "tpcs"
                                   , Bytes topicName
                                   , Bytes "meta"
                                   , Bytes "count"
                                   ]
  topicMsgsSS = FDB.extend topicSS [Bytes "tpcs", Bytes topicName, Bytes "msgs"]
  topicWriteOneKey = FDB.pack topicMsgsSS [FDB.IncompleteVS (IncompleteVersionstamp 0)]

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

-- TODO: currently not used by anything, but might be useful.
incrTopicCount :: TopicConfig
               -> Transaction ()
incrTopicCount conf = do
  let k = topicCountKey conf
  let one = "\x01\x00\x00\x00\x00\x00\x00\x00"
  FDB.atomicOp k (Op.add one)

getTopicCount :: TopicConfig
              -> Transaction Word64
getTopicCount conf = do
  let k = topicCountKey conf
  cBytes <- fromMaybe "" <$> (FDB.get k >>= await)
  let paddedBytes = cBytes
                    <> (BS.pack (take (8 - BS.length cBytes) (cycle [0])))
  return $ (runGet getWord64le . fromStrict) paddedBytes

readerSS :: TopicConfig -> ReaderName -> Subspace
readerSS TopicConfig{..} rn =
  extend topicSS [Bytes "rdrs", Bytes rn]

readerCheckpointKey :: TopicConfig
                    -> ReaderName
                    -> ByteString
readerCheckpointKey tc rn =
  FDB.pack (readerSS tc rn) [Bytes "ckpt"]

-- | Danger!! It's possible to write multiple messages with the same key
-- if this is called more than once in a single transaction.
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
        let k = FDB.pack topicMsgsSS [FDB.IncompleteVS vs]
        FDB.atomicOp k (setVersionstampedKey bs)
        incrTopicCount tc
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
  FDB.runTransaction topicConfigDB $ writeTopic' tc bss

-- | Optimized function for writing a single message to a topic. The key is
-- precomputed once, so no time needs to be spent computing it. This may be
-- faster than writing batches of keys. Profile the code to find out!
-- DANGER: can only be called once per topic per transaction. Cannot be mixed
-- with other write functions in a single transaction.
writeOneMsgTopic :: TopicConfig
                 -> ByteString
                 -> IO ()
writeOneMsgTopic tc@TopicConfig{..} bs = do
  FDB.runTransaction topicConfigDB $ do
    FDB.atomicOp topicWriteOneKey (setVersionstampedKey bs)
    incrTopicCount tc

trOutput :: TopicConfig
         -> (ByteString, ByteString)
         -> (Versionstamp 'Complete, ByteString)
trOutput TopicConfig{..} (k,v) =
  case FDB.unpack topicMsgsSS k of
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

checkpoint' :: TopicConfig
            -> ReaderName
            -> Versionstamp 'Complete
            -> Transaction ()
checkpoint' tc rn vs = do
  let k = readerCheckpointKey tc rn
  let v = encodeVersionstamp vs
  FDB.atomicOp k (Op.byteMax v)

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
  let begin = FDB.pack (topicMsgsSS tc) [CompleteVS cpvs]
  let end = prefixRangeEnd $ FDB.subspaceKey (topicMsgsSS tc)
  let r = Range { rangeBegin = FirstGreaterThan begin
                , rangeEnd = FirstGreaterOrEq end
                , rangeLimit = Just (fromIntegral n)
                , rangeReverse = False
                }
  fmap (trOutput tc) <$> FDB.getEntireRange r

-- TODO: would be useful to have a version of this that returns a watch if
-- there are no new messages.
readNAndCheckpoint' :: TopicConfig
                    -> ReaderName
                    -> Word8
                    -> Transaction (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint' tc@TopicConfig{..} rn n =
  readNPastCheckpoint tc rn n >>= \case
    (x@(_ :|> (vs,_))) -> do
      checkpoint' tc rn vs
      return x
    _ -> return mempty

readNAndCheckpoint :: TopicConfig
                   -> ReaderName
                   -> Word8
                   -> IO (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint tc@TopicConfig{..} rn n =
  FDB.runTransactionWithConfig infRetry topicConfigDB (readNAndCheckpoint' tc rn n)
