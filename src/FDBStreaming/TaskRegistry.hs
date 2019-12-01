{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.TaskRegistry (
  TaskRegistry(..),
  empty,
  numTasks,
  addTask,
  runRandomTask
) where

import           Control.Concurrent (myThreadId)
import           FoundationDB (Transaction)
import qualified FoundationDB as FDB
import           FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import           Data.Map (Map)
import qualified Data.Map as M

import FDBStreaming.TaskLease (TaskSpace, TaskName, TaskID, ReleaseResult, EnsureTaskResult(AlreadyExists, NewlyCreated), taskSpace, ensureTask, acquireRandomUnbiased, isLeaseValid, isLocked, release)

-- | A registry to store continuously-recurring tasks. Processes can ask the
-- registry for a task assignment. The registry is responsible for acquiring
-- a distributed lease for the task, ensuring exclusive access to the task
-- across all workers.

-- This comes with a caveat: the worker must manually check that the lease
-- is still valid as it runs! While this restriction is inconvenient, it is
-- unavoidable -- process execution could be delayed arbitrarily after lease
-- acquisition, and leases eventually time out and unlock themselves again.
-- This also gives us some flexibility -- for some tasks, we might already have
-- achieved safety through transaction design, and we just want to use leases
-- to reduce conflicts. In such cases, it's not important to check that the lock
-- still holds.
data TaskRegistry =
  TaskRegistry {
    taskRegistrySpace :: TaskSpace
    -- TODO: this should be in an IORef -- addTask is effectful anyway; it's
    -- confusing that half of it is effectful and half isn't.
    , getTaskRegistry
        :: Map TaskName
               (TaskID, Transaction Bool -> Transaction ReleaseResult -> IO ())
    , taskRegistryLeaseDuration :: Int
    }

empty
  :: Subspace
      -- ^ Base subspace to contain task lease data. This will be extended
      -- with the string "TS"
  -> Int
      -- ^ Duration for which leases should be acquired, in seconds.
  -> TaskRegistry
empty ss =
  TaskRegistry (taskSpace $ FDB.extend ss [FDB.Bytes "TS"]) mempty

numTasks :: TaskRegistry -> Int
numTasks = M.size . getTaskRegistry

addTask
  :: TaskRegistry
  -> TaskName
        -- ^ A unique string name for this task. If the given name already
        -- exists, it will be overwritten.
  -> (Transaction Bool -> Transaction ReleaseResult -> IO ())
        -- ^ An action that is run once per lease acquisition. It is passed two
        -- transactions: one that returns true if the acquired lease is still
        -- valid, and another that releases the lease. The latter can be used to
        -- atomically finish a longer-running computation.
  -> Transaction TaskRegistry
addTask (TaskRegistry ts tr dur) taskName f = do
  taskID <- extractID <$> ensureTask ts taskName
  let tr' = M.insert taskName (taskID, f) tr
  return $ TaskRegistry ts tr' dur
 where
  extractID (AlreadyExists t) = t
  extractID (NewlyCreated  t) = t

-- | Run a random task in the task registry. If no tasks are available, returns
-- false.
runRandomTask :: FDB.Database -> TaskRegistry -> IO Bool
runRandomTask db (TaskRegistry ts tr dur) =
  FDB.runTransaction db (acquireRandomUnbiased ts dur) >>= \case
    Nothing                -> do
      tid <- myThreadId
      putStrLn $ show tid ++ "couldn't find an unlocked task."
      --threadDelay (dur * 1000000 `div` 2)
      return False
    Just (taskName, lease, howAcquired) -> case M.lookup taskName tr of
      Nothing -> do
        tid <- myThreadId
        putStrLn $ show tid ++ " found an invalid task " ++ show taskName
                   ++ " not present in " ++ show (M.keys tr)
        return False --TODO: warn? this implies tasks outside registry
      Just (taskID, f) -> do
        tid <- myThreadId
        putStrLn $ show tid
                   ++ " starting on task "
                   ++ show taskName
                   ++ " with task ID "
                   ++ show taskID
                   ++ " acquired by "
                   ++ show howAcquired
        f ((&&) <$> isLeaseValid ts taskID lease
                <*> isLocked ts taskName)
          (release ts taskName lease)
        return True
