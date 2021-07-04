{-# LANGUAGE OverloadedStrings #-}

module Spec.FDBStreaming.Util.BatchWriter
  ( batchWriterTests,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, poll)
import Control.Monad (replicateM, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (toStrict)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import FDBStreaming.Topic (getTopicCount, makeTopic, writeTopic)
import qualified FDBStreaming.Util.BatchWriter as BW
import FoundationDB (runTransaction)
import qualified FoundationDB as FDB
import qualified FoundationDB.Layer.Subspace as FDB
import Spec.FDBStreaming.Util (extendRand)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)

newWrite :: IO (BW.BatchWrite ByteString)
newWrite = do
  uuid <- UUID.nextRandom
  let bytes = "1234"
  let l = fromIntegral $ BS.length bytes
  return $ BW.BatchWrite (toStrict $ UUID.toByteString uuid) (Just l) bytes

canceledTxn :: FDB.Subspace -> FDB.Database -> TestTree
canceledTxn testSS db = testCase "Fails when transaction canceled" $ do
  ss <- extendRand testSS
  bw <- BW.batchWriter BW.defaultBatchWriterConfig db ss (\_ -> FDB.cancel)
  x <- newWrite
  res <- BW.write bw x
  res @?= BW.Failed (FDB.Error (FDB.MaxRetriesExceeded (FDB.CError FDB.TransactionCanceled)))

writeSuccess :: FDB.Subspace -> FDB.Database -> TestTree
writeSuccess testSS db = testCase "write" $ do
  bwSS <- extendRand testSS
  tSS <- extendRand testSS
  let topic = makeTopic tSS "test" 1 0
  let f = writeTopic topic 0
  bw <- BW.batchWriter BW.defaultBatchWriterConfig db bwSS (void . f)
  x <- newWrite
  res <- BW.write bw x
  res @?= BW.Success
  res2 <- BW.write bw x
  res2 @?= BW.SuccessAlreadyExisted
  c <- runTransaction db $ getTopicCount topic
  c @?= 1

isPending :: Async a -> IO Bool
isPending a = do
  x <- poll a
  return $ case x of
    Nothing -> True
    _ -> False

batchSize :: FDB.Subspace -> FDB.Database -> TestTree
batchSize testSS db = testCase "maxBatchSize" $ do
  bwSS <- extendRand testSS
  tSS <- extendRand testSS
  let topic = makeTopic tSS "test" 1 0
  let f = writeTopic topic 0
  bw <-
    BW.batchWriter
      BW.defaultBatchWriterConfig
        { BW.maxBatchSize = 3,
          BW.desiredMaxLatencyMillis = 5000
        }
      db
      bwSS
      (void . f)
  [x, y] <- replicateM 2 (async $ newWrite >>= BW.write bw)
  pendingX <- isPending x
  pendingY <- isPending y
  assertBool "first two writes pending" (pendingX && pendingY)
  _ <- async $ newWrite >>= BW.write bw
  threadDelay 1000000
  pendingX' <- isPending x
  assertBool "Batch was committed after third write received" (not pendingX')
  c <- runTransaction db $ getTopicCount topic
  c @?= 3

idempotencyKeyTimeout :: FDB.Subspace -> FDB.Database -> TestTree
idempotencyKeyTimeout testSS db = testCase "minIdempotencyMemoryDurationSeconds" $ do
  bwSS <- extendRand testSS
  tSS <- extendRand testSS
  let topic = makeTopic tSS "test" 1 0
  let f = writeTopic topic 0
  let cfg = BW.defaultBatchWriterConfig {BW.minIdempotencyMemoryDurationSeconds = 1}
  bw <- BW.batchWriter cfg db bwSS (void . f)
  x <- newWrite
  res <- BW.write bw x
  res @?= BW.Success
  -- Wait for longer than memory duration, and we can write the same value again
  threadDelay 3000000
  res3 <- BW.write bw x
  res3 @?= BW.Success

batchWriterTests :: FDB.Subspace -> FDB.Database -> TestTree
batchWriterTests testSS db =
  testGroup
    "BatchWriter"
    [ canceledTxn testSS db,
      writeSuccess testSS db,
      batchSize testSS db,
      idempotencyKeyTimeout testSS db
    ]
