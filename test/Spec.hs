{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Exception (finally)
import Data.Word
import Test.Hspec
import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import FoundationDB.Versionstamp
import qualified Data.Sequence as Seq
import GHC.Exts (IsList(toList))

import Spec.FDBStreaming.TaskLease

import FDBStreaming.Topic

txnVersion :: Versionstamp a -> TransactionVersionstamp
txnVersion (CompleteVersionstamp x _) = x
txnVersion _ = error "no version"

main :: IO ()
main = withFoundationDB defaultOptions $
  \db -> flip finally (cleanup db)
         $ hspec
         $ before_ (cleanup db)
         $ do
    let tc = TopicConfig db testSS
    let tn = "test"
    let testSS = subspace [Bytes "fdbstreaming-test"]
    describe "read write" $ do
      it "placeholder" $ do
        1 `shouldBe` 1
    describe "leases" $ do
      -- TODO: still some lingering bugs with the state machine tester, where
      -- it tries to generate sequences of commands that are impossible to
      -- carry out (like locking a task before creating it).
      --it "works" (smProp db)
      leaseProps testSS db
