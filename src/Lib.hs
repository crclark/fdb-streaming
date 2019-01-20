{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Lib where

import Control.Applicative
import Control.Monad
import Data.Binary.Get ( runGet
                       , runGetOrFail
                       , getWord64le
                       , getWord32le
                       , getWord16le
                       , getWord8
                       , Get)
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict)
import Data.Foldable (toList, foldlM)
import Data.Maybe (fromJust)
import qualified Data.Sequence as Seq
import Data.Sequence (Seq(..), ViewL(..), ViewR(..))
import Data.Word (Word8, Word16, Word64)
import FoundationDB as FDB
import FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Tuple as FDB
-- TODO: move prefixRangeEnd out of Advanced usage section.
import FoundationDB.Transaction (prefixRangeEnd)
import FoundationDB.Versionstamp (Versionstamp
                                  (CompleteVersionstamp,
                                   IncompleteVersionstamp),
                                  decodeTransactionVersionstamp,
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

-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
writeTopic :: Traversable t
           => TopicConfig
           -> t ByteString
           -> IO ()
writeTopic tc@TopicConfig{..} bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  FDB.runTransaction topicConfigDB $ do
    _ <- foldlM go 1 bss
    return ()
    where
      go !i bs = do
        let vs = IncompleteVersionstamp i
        let k = FDB.pack topicMsgsSS [FDB.IncompleteVSElem vs]
        FDB.atomicOp FDB.SetVersionstampedKey k bs
        incrTopicCount tc
        return (i+1)

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
                FDB.subspaceKey topicMsgsSS
    let rangeN = range {rangeLimit = Just n}
    fmap (trOutput tc) <$> FDB.getEntireRange rangeN

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
