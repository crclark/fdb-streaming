{-# LANGUAGE OverloadedStrings #-}

module Spec.FDBStreaming.Watermark (
  watermarks
) where

import Data.Time (UTCTime, addUTCTime, getCurrentTime)
import Data.Int (Int64)
import FDBStreaming.Util (millisSinceEpoch)
import FDBStreaming.Watermark
  ( Watermark(Watermark, watermarkUTCTime),
    getWatermark,
    getCurrentWatermark,
    setWatermark
  )
import FoundationDB (Database, runTransaction)
import FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB as FDB
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Versionstamp as FDB

import Test.HUnit.Base ()
import Test.Hspec (SpecWith, it, shouldBe)

addSecond :: UTCTime -> UTCTime
addSecond = addUTCTime 1

-- Round to milliseconds
rounded :: Watermark -> Int64
rounded = millisSinceEpoch . watermarkUTCTime

watermarks :: Subspace -> Database -> SpecWith ()
watermarks ss db = do
  let watermarkSS = FDB.extend ss [FDB.Bytes "watermarktest"]
  it "should get what we set" $ do
    t <- Watermark <$> getCurrentTime
    runTransaction db $ setWatermark watermarkSS t
    mw <- runTransaction db $ getCurrentWatermark watermarkSS >>= FDB.await
    (fmap rounded mw) `shouldBe` Just (rounded t)

  it "allows looking up watermark by version" $ do
    t <- Watermark <$> getCurrentTime
    fv <- runTransaction db $ do setWatermark watermarkSS t
                                 FDB.getVersionstamp
    (Right (Right (FDB.TransactionVersionstamp v _))) <- FDB.awaitIO fv
    runTransaction db $ setWatermark watermarkSS (Watermark $ addSecond $ watermarkUTCTime t)
    mw <- runTransaction db $ getWatermark watermarkSS v >>= FDB.await
    (fmap rounded mw) `shouldBe` Just (rounded t)

  it "is guaranteed to monotonically increase, regardless of input" $ do
    t1 <- getCurrentTime
    let t2 = addSecond t1
    runTransaction db $ setWatermark watermarkSS $ Watermark t2
    runTransaction db $ setWatermark watermarkSS $ Watermark t1
    mw <- runTransaction db $ getCurrentWatermark watermarkSS >>= FDB.await
    (fmap rounded mw) `shouldBe` Just (rounded $ Watermark t2)

  it "returns nothing for minBound version" $ do
    mw <- runTransaction db $ getWatermark watermarkSS 0 >>= FDB.await
    mw `shouldBe` Nothing

