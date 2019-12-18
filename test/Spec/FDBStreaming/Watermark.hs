{-# LANGUAGE OverloadedStrings #-}

module Spec.FDBStreaming.Watermark (
  watermarks
) where

import Data.Time (UTCTime, addUTCTime, getCurrentTime)

import FDBStreaming.Util (millisSinceEpoch)
import FDBStreaming.Watermark (getWatermark, getCurrentWatermark, setWatermark)
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

watermarks :: Subspace -> Database -> SpecWith ()
watermarks ss db = do
  let watermarkSS = FDB.extend ss [FDB.Bytes "watermarktest"]
  it "should get what we set" $ do
    t <- getCurrentTime
    runTransaction db $ setWatermark watermarkSS t
    mw <- runTransaction db $ getCurrentWatermark watermarkSS >>= FDB.await
    fmap millisSinceEpoch mw `shouldBe` Just (millisSinceEpoch t)

  it "allows looking up watermark by version" $ do
    t <- getCurrentTime
    fv <- runTransaction db $ do setWatermark watermarkSS t
                                 FDB.getVersionstamp
    (Right (Right (FDB.TransactionVersionstamp v _))) <- FDB.awaitIO fv
    runTransaction db $ setWatermark watermarkSS (addSecond t)
    mw <- runTransaction db $ getWatermark watermarkSS v >>= FDB.await
    fmap millisSinceEpoch mw `shouldBe` Just (millisSinceEpoch t)

  it "is guaranteed to monotonically increase, regardless of input" $ do
    t1 <- getCurrentTime
    let t2 = addSecond t1
    runTransaction db $ setWatermark watermarkSS t2
    runTransaction db $ setWatermark watermarkSS t1
    mw <- runTransaction db $ getCurrentWatermark watermarkSS >>= FDB.await
    fmap millisSinceEpoch mw `shouldBe` Just (millisSinceEpoch t2)


