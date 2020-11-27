{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}

module Spec.FDBStreaming.Joins.OneToMany
  ( oneToManyJoinTests,
  )
where

import Data.Maybe (isJust, isNothing, fromJust)
import FDBStreaming.Message (Message (fromMessage))
import qualified FDBStreaming.Joins.OneToMany as OTM
import qualified FoundationDB as FDB
import FoundationDB (runTransaction)
import qualified FoundationDB.Layer.Subspace as FDB
import Spec.FDBStreaming.Util (extendRand)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)

lStoreMsgs :: FDB.Subspace -> FDB.Database -> TestTree
lStoreMsgs testSS db = testCase "lStoreMsgs: set and get" $ do
  ss <- OTM.oneToManyJoinSS <$> extendRand testSS
  let v = 1 :: Int
  runTransaction db $ OTM.setLStoreMsg ss v v
  result <- runTransaction db $ OTM.getLStoreMsg ss v >>= FDB.await
  result @?= Just v

rBacklogMsgs :: FDB.Subspace -> FDB.Database -> TestTree
rBacklogMsgs testSS db = testCase "rBacklogMsgs: set, get, delete" $ do
  ss <- OTM.oneToManyJoinSS <$> extendRand testSS
  let k = 1 :: Int
  let v = 2 :: Int
  exists1 <- runTransaction db $ OTM.existsRBacklog ss k >>= FDB.await
  runTransaction db $ OTM.addRBacklogMessage ss k v 0
  exists2 <- runTransaction db $ OTM.existsRBacklog ss k >>= FDB.await
  backlog <- runTransaction db $ OTM.getAndDeleteRBacklogMessages ss k 5
  exists1 @?= False
  exists2 @?= True
  backlog @?= [v]

flushableBackloggedKeys :: FDB.Subspace -> FDB.Database -> TestTree
flushableBackloggedKeys testSS db = testCase "flushableBackloggedKeys" $ do
  ss <- OTM.oneToManyJoinSS <$> extendRand testSS
  let k = 1 :: Int

  noKey <- runTransaction db $ OTM.getArbitraryFlushableBackloggedKey ss 0 >>= FDB.await
  noKey @?= Nothing

  runTransaction db $ OTM.addFlushableBackloggedKey ss 0 k

  result <- runTransaction db $ OTM.getArbitraryFlushableBackloggedKey ss 0 >>= FDB.await
  assertBool "got flushable key" (isJust result)
  fromMessage (fromJust result) @?= k

  runTransaction db $ OTM.removeFlushableBackloggedKey ss 0 k

  result2 <- runTransaction db $ OTM.getArbitraryFlushableBackloggedKey ss 0 >>= FDB.await
  assertBool "didn't get flushable key" (isNothing result2)

lThenR :: FDB.Subspace -> FDB.Database -> TestTree
lThenR testSS db = testCase "l-msg followed by r-msg" $ do
  ss <- OTM.oneToManyJoinSS <$> extendRand testSS
  let toKey = id
  let joinFn x y = return (x + y)
  runTransaction db $ OTM.lMessageJob ss 0 toKey [1 :: Int]
  result <- runTransaction db $ OTM.rMessageJob ss toKey [1 :: Int] joinFn
  result @?= [2]

rThenLThenBacklog :: FDB.Subspace -> FDB.Database -> TestTree
rThenLThenBacklog testSS db = testCase "r-msg followed by l-msg: join done by backlog" $ do
  ss <- OTM.oneToManyJoinSS <$> extendRand testSS
  let toKey = id
  let joinFn x y = return (x + y)
  rResult <- runTransaction db $ OTM.rMessageJob ss toKey [1 :: Int] joinFn
  runTransaction db $ OTM.lMessageJob ss 0 toKey [1 :: Int]
  flushResult <- runTransaction db $ OTM.flushBacklogJob ss 0 joinFn 5
  rResult @?= []
  flushResult @?= [2 :: Int]

oneToManyJoinTests :: FDB.Subspace -> FDB.Database -> TestTree
oneToManyJoinTests testSS db =
  testGroup "oneToManyJoin"
  [ lStoreMsgs testSS db
  , rBacklogMsgs testSS db
  , flushableBackloggedKeys testSS db
  , lThenR testSS db
  , rThenLThenBacklog testSS db ]
