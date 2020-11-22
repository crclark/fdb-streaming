{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-missing-export-lists #-}

import FoundationDB
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import Spec.FDBStreaming (jobTests)
import Spec.FDBStreaming.AggrTable (tableProps)
import Spec.FDBStreaming.Index (indexTests)
import Spec.FDBStreaming.TaskLease
import Spec.FDBStreaming.Topic (topicTests)
import Spec.FDBStreaming.Util (utilProps)
import Spec.FDBStreaming.Util.BatchWriter (batchWriterTests)
import Spec.FDBStreaming.Watermark (watermarks)
import Spec.FDBStreaming.Joins.OneToMany (oneToManyJoinTests)
import Test.Tasty

testSS :: Subspace
testSS = subspace [Bytes "fdbstreaming-test"]

cleanup :: Database -> IO ()
cleanup db = do
  let (begin, end) = rangeKeys $ subspaceRange testSS
  runTransactionWithConfig defaultConfig {timeout = 5000} db $ clearRange begin end

allTests :: Database -> TestTree
allTests db =
  testGroup
    "Tests"
    [ leaseProps testSS db,
      tableProps testSS db,
      watermarks testSS db,
      topicTests testSS db,
      batchWriterTests testSS db,
      indexTests testSS db,
      utilProps,
      jobTests testSS db,
      oneToManyJoinTests testSS db
    ]

main :: IO ()
main = withFoundationDB defaultOptions $ \db -> do
  cleanup db
  defaultMain $ allTests db
  cleanup db
