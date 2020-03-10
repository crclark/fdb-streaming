{-# LANGUAGE OverloadedStrings #-}

module Spec.FDBStreaming.Watermark
  ( watermarks,
  )
where

import Data.Int (Int64)
import Data.Time (UTCTime, addUTCTime, getCurrentTime)
import FDBStreaming.Util (millisSinceEpoch)
import FDBStreaming.Watermark
  ( Watermark (Watermark, watermarkUTCTime),
    getCurrentWatermark,
    getWatermark,
    setWatermark,
  )
import FoundationDB (Database, runTransaction)
import qualified FoundationDB as FDB
import FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB.Versionstamp as FDB
import Spec.FDBStreaming.Util (extendRand)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

addSecond :: UTCTime -> UTCTime
addSecond = addUTCTime 1

-- Round to milliseconds
rounded :: Watermark -> Int64
rounded = millisSinceEpoch . watermarkUTCTime

watermarks :: Subspace -> Database -> TestTree
watermarks ss db =
  testGroup
    "watermarks"
    [ testCase "should get what we set" $ do
        watermarkSS <- extendRand ss
        t <- Watermark <$> getCurrentTime
        runTransaction db $ setWatermark watermarkSS t
        mw <- runTransaction db $ getCurrentWatermark watermarkSS >>= FDB.await
        (fmap rounded mw) @?= Just (rounded t),
      testCase "allows looking up watermark by version" $ do
        watermarkSS <- extendRand ss
        t <- Watermark <$> getCurrentTime
        fv <- runTransaction db $ do
          setWatermark watermarkSS t
          FDB.getVersionstamp
        (Right (Right (FDB.TransactionVersionstamp v _))) <- FDB.awaitIO fv
        runTransaction db $ setWatermark watermarkSS (Watermark $ addSecond $ watermarkUTCTime t)
        mw <- runTransaction db $ getWatermark watermarkSS v >>= FDB.await
        (fmap rounded mw) @?= Just (rounded t),
      testCase "is guaranteed to monotonically increase, regardless of input" $ do
        watermarkSS <- extendRand ss
        t1 <- getCurrentTime
        let t2 = addSecond t1
        runTransaction db $ setWatermark watermarkSS $ Watermark t2
        runTransaction db $ setWatermark watermarkSS $ Watermark t1
        mw <- runTransaction db $ getCurrentWatermark watermarkSS >>= FDB.await
        (fmap rounded mw) @?= Just (rounded $ Watermark t2),
      testCase "returns nothing for minBound version" $ do
        watermarkSS <- extendRand ss
        mw <- runTransaction db $ getWatermark watermarkSS 0 >>= FDB.await
        mw @?= Nothing
    ]
