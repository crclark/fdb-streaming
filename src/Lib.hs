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
import Data.Sequence (Seq)
import Data.Word (Word16, Word64)
import FoundationDB as FDB
import FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Tuple as FDB
import FoundationDB.Versionstamp (Versionstamp
                                  (IncompleteVersionstamp),
                                  decodeTransactionVersionstamp,
                                  TransactionVersionstamp(..),
                                  VersionstampCompleteness(..),
                                  decodeVersionstamp)
import System.IO (stderr, hPutStrLn)

someFunc :: IO ()
someFunc = putStrLn "someFunc"

type TopicName = ByteString

data TopicConfig = TopicConfig { topicConfigDB :: FDB.Database
                               , topicSS :: FDB.Subspace
                               }

topicCountKey :: TopicConfig -> TopicName -> ByteString
topicCountKey TopicConfig{..} tn =
  FDB.pack topicSS [ BytesElem tn
                   , BytesElem "meta"
                   , BytesElem "count"
                   ]

incrTopicCount :: TopicConfig
               -> TopicName
               -> Transaction ()
incrTopicCount conf tn = do
  let k = topicCountKey conf tn
  let one = "\x01"
  FDB.atomicOp FDB.Add k one

getTopicCount :: TopicConfig
              -> TopicName
              -> Transaction (Maybe Word64)
getTopicCount conf tn = do
  let k = topicCountKey conf tn
  cBytes <- FDB.get k >>= await
  -- TODO: partial
  return $ fmap (runGet parse . fromStrict) cBytes
  where parse = getWord64le
                <|> fromIntegral <$> getWord32le
                <|> fromIntegral <$> getWord16le
                <|> fromIntegral <$> getWord8

-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
writeTopic :: Traversable t
           => TopicConfig
           -> TopicName
           -> t ByteString
           -> IO ()
writeTopic tc@TopicConfig{..} tname bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  FDB.runTransaction topicConfigDB $ do
    n <- foldlM go 1 bss
    incrTopicCount tc tname
    where
      go !i bs = do
        let vs = IncompleteVersionstamp i
        let k = FDB.pack topicSS [ FDB.BytesElem tname
                                 , FDB.BytesElem "contents"
                                 , FDB.IncompleteVSElem vs]
        FDB.atomicOp FDB.SetVersionstampedKey k bs
        return (i+1)

trOutput :: TopicConfig
         -> (ByteString, ByteString)
         -> (Versionstamp 'Complete, ByteString)
trOutput (TopicConfig _ ss) (k,v) =
  case FDB.unpack ss k of
    Right [BytesElem _, BytesElem _, CompleteVSElem vs] -> (vs, v)
    Right t -> error $ "unexpected tuple: " ++ show t
    Left err -> error $ "failed to decode "
                        ++ show k
                        ++ " because "
                        ++ show err

readLastN :: TopicConfig
          -> TopicName
          -> Int
          -> IO (Seq (Versionstamp 'Complete, ByteString))
readLastN tc@TopicConfig{..} tn n =
  FDB.runTransaction topicConfigDB $ do
    let range = fromJust $
                FDB.prefixRange $
                FDB.pack topicSS [ FDB.BytesElem tn
                                 , FDB.BytesElem "contents"]
    let rangeN = range { rangeReverse = True, rangeLimit = Just n}
    fmap (trOutput tc) <$> FDB.getEntireRange rangeN

getNAfter :: TopicConfig
          -> TopicName
          -> Int
          -> Versionstamp 'Complete
          -> IO (Seq (Versionstamp 'Complete, ByteString))
getNAfter tc@TopicConfig{..} tn n vs =
  FDB.runTransaction topicConfigDB $ do
    let range = fromJust $
                FDB.prefixRange $
                FDB.pack topicSS [FDB.BytesElem tn]
    let rangeN = range {rangeLimit = Just n}
    fmap (trOutput tc) <$> FDB.getEntireRange rangeN

blockUntilNew :: TopicConfig -> TopicName -> IO ()
blockUntilNew conf@TopicConfig{..} tn = do
  let k = topicCountKey conf tn
  f <- FDB.runTransaction topicConfigDB (FDB.watch k)
  FDB.awaitIO f >>= \case
    Right () -> return ()
    Left err -> do
      hPutStrLn stderr $ "got error while watching: " ++ show err
      blockUntilNew conf tn

