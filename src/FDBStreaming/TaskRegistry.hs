{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming.TaskRegistry (
  TaskRegistry(..),
  empty,
  numTasks,
  addTask,
  runRandomTask
) where

import           Control.Concurrent (myThreadId)
import           Control.Monad.IO.Class ( liftIO )
import           Data.IORef (IORef, newIORef, atomicModifyIORef', readIORef)
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
    , getTaskRegistry
        :: IORef (Map TaskName
                      ( TaskID
                      , Int
                      , Transaction Bool -> Transaction ReleaseResult -> IO ()))
    }

empty
  :: Subspace
      -- ^ Base subspace to contain task lease data. This will be extended
      -- with the string "TS"
  -> IO TaskRegistry
empty ss =
  TaskRegistry
  <$> pure (taskSpace $ FDB.extend ss [FDB.Bytes "TS"])
  <*> newIORef mempty

numTasks :: TaskRegistry -> IO Int
numTasks tr = M.size <$> readIORef (getTaskRegistry tr)

addTask
  :: TaskRegistry
  -> TaskName
        -- ^ A unique string name for this task. If the given name already
        -- exists, it will be overwritten.
  -> Int
  -- ^ How long to lock the task when it runs, in seconds.
  -> (Transaction Bool -> Transaction ReleaseResult -> IO ())
        -- ^ An action that is run once per lease acquisition. It is passed two
        -- transactions: one that returns true if the acquired lease is still
        -- valid, and another that releases the lease. The latter can be used to
        -- atomically finish a longer-running computation.
  -> Transaction ()
addTask (TaskRegistry ts tr) taskName dur f = do
  taskID <- extractID <$> ensureTask ts taskName
  liftIO $ atomicModifyIORef' tr ((, ()) . M.insert taskName (taskID, dur, f))
 where
  extractID (AlreadyExists t) = t
  extractID (NewlyCreated  t) = t

-- | Run a random task in the task registry. If no tasks are available, returns
-- false.
runRandomTask :: FDB.Database -> TaskRegistry -> IO Bool
runRandomTask db (TaskRegistry ts tr) = do
  tr' <- readIORef tr
  let toDur taskName = case M.lookup taskName tr' of
                         Nothing -> 5 -- code below will warn
                         Just (_, dur, _) -> dur
  FDB.runTransaction db (acquireRandomUnbiased ts toDur) >>= \case
    Nothing                -> do
      tid <- myThreadId
      --putStrLn $ show tid ++ "couldn't find an unlocked task."
      return False
    Just (taskName, lease, howAcquired) -> case M.lookup taskName tr' of
      Nothing -> do
        tid <- myThreadId
        {-
        putStrLn $ show tid ++ " found an invalid task " ++ show taskName
                   ++ " not present in " ++ show (M.keys tr')
        -}
        return False --TODO: warn? this implies tasks outside registry
      Just (taskID, _dur, f) -> do
        tid <- myThreadId
        {-
        putStrLn $ show tid
                   ++ " starting on task "
                   ++ show taskName
                   ++ " with task ID "
                   ++ show taskID
                   ++ " acquired by "
                   ++ show howAcquired
        -}
        f ((&&) <$> isLeaseValid ts taskID lease
                <*> isLocked ts taskName)
          (release ts taskName lease)
        return True
