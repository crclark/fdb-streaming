{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -fno-warn-missing-import-lists #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -fno-warn-unused-top-binds #-}

module Spec.FDBStreaming.TaskLease
 ( leaseProps
 )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, wait)
import Control.Monad (forM_, replicateM, void)
import Control.Monad.IO.Class (liftIO)
import Data.IORef (atomicModifyIORef, newIORef, readIORef)
import qualified Data.Map as Map
import qualified Data.Sequence as Seq
import FDBStreaming.TaskLease
    ( AcquiredLease(AcquiredLease),
      EnsureTaskResult(AlreadyExists, NewlyCreated),
      TryAcquireResult(TryAcquireSuccess, TryAcquireIsLocked),
      HowAcquired(Available, RandomExpired),
      TaskSpace(TaskSpace),
      TaskSpace,
      acquireRandom,
      ensureTask,
      removeTask,
      tryAcquire,
      listAllTasks )
import qualified FDBStreaming.TaskRegistry as TR
import FoundationDB (Database, runTransaction)
import FoundationDB.Layer.Subspace (Subspace)
import Spec.FDBStreaming.Util (extendRand)

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)

leaseProps :: Subspace -> Database -> TestTree
leaseProps testSS db =
  testGroup
    "Lease properties"
    [ mutualExclusion testTaskSpace db,
      uniformRandomness testTaskSpace db,
      mutualExclusionRandom testTaskSpace db,
      taskDeletion testTaskSpace db
    ]
  where
    testTaskSpace = TaskSpace testSS

isNewlyCreated :: EnsureTaskResult -> Bool
isNewlyCreated (NewlyCreated _) = True
isNewlyCreated _ = False

isAlreadyExists :: EnsureTaskResult -> Bool
isAlreadyExists (AlreadyExists _) = True
isAlreadyExists _ = False

isTryAcquireSuccess :: TryAcquireResult -> Bool
isTryAcquireSuccess (TryAcquireSuccess _) = True
isTryAcquireSuccess _ = False

isTryAcquireIsLocked :: TryAcquireResult -> Bool
isTryAcquireIsLocked (TryAcquireIsLocked) = True
isTryAcquireIsLocked _ = False

taskDeletion :: TaskSpace -> Database -> TestTree
taskDeletion (TaskSpace testSS) db =
  testCase "removing tasks" $ do
    ss <- TaskSpace <$> extendRand testSS
    let t1 = "testTask1"
    let t2 = "testTask2"
    runTransaction db $ removeTask ss t1
    create1 <- runTransaction db $ ensureTask ss t1
    assertBool "t1 not newly created" $ isNewlyCreated create1
    create2 <- runTransaction db $ ensureTask ss t2
    assertBool "t2 not newly created" $ isNewlyCreated create2
    runTransaction db $ removeTask ss t1
    create12 <- runTransaction db $ ensureTask ss t1
    assertBool "t1 not newly created after delete and create" $ isNewlyCreated create12
    create22 <- runTransaction db $ ensureTask ss t2
    assertBool "t2 didn't already exist after removing t1" $ isAlreadyExists create22
    runTransaction db $ removeTask ss t1
    allTasks <- runTransaction db $ listAllTasks ss
    assertBool "t1 still listed in all tasks" (allTasks == Seq.fromList [t2])
    acquireResult <- runTransaction db $ tryAcquire ss t2 5
    assertBool "couldn't acquire t2" (isTryAcquireSuccess acquireResult)
    runTransaction db $ removeTask ss t2
    allTasks2 <- runTransaction db $ listAllTasks ss
    assertBool "t2 still listed in all tasks" (allTasks2 == Seq.empty)

mutualExclusion :: TaskSpace -> Database -> TestTree
mutualExclusion (TaskSpace testSS) db =
  testCase "Shouldn't be acquirable when already acquired" $ do
    ss <- TaskSpace <$> extendRand testSS
    let taskName = "testTask"
    res <- runTransaction db $ ensureTask ss taskName
    assertBool "created" $ isNewlyCreated res
    res2 <- runTransaction db $ ensureTask ss taskName
    assertBool "creating again: already exists" $ isAlreadyExists res2
    acq <- runTransaction db $ tryAcquire ss taskName 5
    assertBool "Failed to acquire a new lock" (isTryAcquireSuccess acq)
    acq2 <- runTransaction db $ tryAcquire ss taskName 5
    assertBool "Acquired a lock that should be locked already" (isTryAcquireIsLocked acq2)
    threadDelay 7000000
    acq3 <- runTransaction db $ tryAcquire ss taskName 5
    assertBool "Failed to acquire a lock that should be expired" (isTryAcquireSuccess acq3)
    assertBool "Acquired same lease for same lock twice" (acq /= acq3)

mutualExclusionRandom :: TaskSpace -> Database -> TestTree
mutualExclusionRandom (TaskSpace testSS) db =
  testCase "With only one task, acquireRandom should be equivalent to acquire" $ do
    ss <- TaskSpace <$> extendRand testSS
    let taskName = "testTask2"
    res <- runTransaction db $ ensureTask ss taskName
    assertBool "created" $ isNewlyCreated res
    acq1 <- runTransaction db $ acquireRandom ss (const 5)
    acq1 @?= Just (taskName, AcquiredLease 1, Available)
    acq2 <- runTransaction db $ acquireRandom ss (const 5)
    acq2 @?= Nothing
    threadDelay 7000000
    acq3 <- runTransaction db $ acquireRandom ss (const 5)
    acq3 @?= Just (taskName, AcquiredLease 2, RandomExpired)
    acq4 <- runTransaction db $ acquireRandom ss (const 5)
    acq4 @?= Nothing

uniformRandomness :: TaskSpace -> Database -> TestTree
uniformRandomness (TaskSpace testSS) db =
  testCase "Should acquire each task once when they each get locked" $ do
    ss <- extendRand testSS
    taskReg <- TR.empty ss
    let tasks =
          [ "task1",
            "task2",
            "task3",
            "task4",
            "task5",
            "task6",
            "task7",
            "task8",
            "task9",
            "task10"
          ]
    taskRunCounts <- liftIO $ newIORef (Map.fromList (zip tasks (repeat (0 :: Int))))
    let task taskName _ _ =
          liftIO
            $ void
            $ atomicModifyIORef
              taskRunCounts
              (\n -> (Map.adjust succ taskName n, ()))
    let addTask' tr taskName = TR.addTask tr taskName 100 (task taskName)
    runTransaction db $ forM_ tasks $ addTask' taskReg
    asyncs <- replicateM 50 (async $ TR.runRandomTask db taskReg)
    forM_ asyncs wait
    finalCounts <- readIORef taskRunCounts
    finalCounts @?= Map.fromList (zip tasks (repeat 1))
    oneMore <- TR.runRandomTask db taskReg
    oneMore @?= False
