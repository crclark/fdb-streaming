{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Exception (finally)
import Test.Hspec
import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import FoundationDB.Versionstamp

import Spec.FDBStreaming.TaskLease

import FDBStreaming.Topic

testSS :: Subspace
testSS = subspace [Bytes "fdbstreaming-test"]

cleanup :: Database -> IO ()
cleanup db = do
  let (begin, end) = rangeKeys $ subspaceRange testSS
  runTransactionWithConfig defaultConfig {timeout=5000} db $ clearRange begin end

main :: IO ()
main = withFoundationDB defaultOptions $
  \db -> flip finally (cleanup db)
         $ hspec
         $ before_ (cleanup db)
         $ do
    describe "leases" $
      -- TODO: still some lingering bugs with the state machine tester, where
      -- it tries to generate sequences of commands that are impossible to
      -- carry out (like locking a task before creating it).
      --it "works" (smProp db)
      leaseProps testSS db
