{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

import Data.Word
import Test.Hspec
import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import FoundationDB.Versionstamp
import Lib
import qualified Data.Sequence as Seq
import GHC.Exts (IsList(toList))

txnVersion :: Versionstamp a -> TransactionVersionstamp
txnVersion (CompleteVersionstamp x _) = x
txnVersion _ = error "no version"

testSS :: Subspace
testSS = subspace [BytesElem "testtest"]

cleanup :: Database -> IO ()
cleanup db = runTransaction db $ do
  let (begin, end) = rangeKeys $ subspaceRange testSS
  clearRange begin end

main :: IO ()
-- TODO: should withFoundationDB return Either Error a instead of
-- passing Either Error DB into the continuation?
main = withFoundationDB currentAPIVersion Nothing $
  \(Right db) -> hspec $ after_ (cleanup db) $ do
    let tc = TopicConfig db testSS
    let tn = "test"
    describe "read write" $ do
      it "has consistent versionstamps" $ do
        writeTopic tc tn ["msg1"]
        [(vs,msg1)] <- toList <$> readLastN tc tn 1
        Just vs' <- runTransaction db $ getLastWrite tc tn
        count <- runTransaction db $ getTopicCount tc tn
        txnVersion vs `shouldBe` vs'
        count `shouldBe` Just 1
