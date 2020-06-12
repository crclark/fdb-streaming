{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming.TaskRegistry
  ( TaskRegistry (..),
    empty,
    numTasks,
    addTask,
    runRandomTask,
  )
where

import Control.Concurrent (myThreadId)
import Control.Logger.Simple (logDebug, showText)
import Control.Monad.IO.Class (liftIO)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Map (Map)
import qualified Data.Map as M
import FDBStreaming.TaskLease (EnsureTaskResult (AlreadyExists, NewlyCreated), ReleaseResult, TaskID, TaskName, TaskSpace, acquireRandomUnbiased, ensureTask, isLeaseValid, isLocked, release, taskSpace)
import FDBStreaming.Util (logAndRethrowErrors)
import FoundationDB (Transaction)
import qualified FoundationDB as FDB
import FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB

-- | A registry to store continuously-recurring tasks. Processes can ask the
-- registry for a task assignment. The registry is responsible for acquiring
-- a distributed lease for the task, ensuring exclusive access to the task
-- across all workers.
data -- This comes with a caveat: the worker must manually check that the lease
  -- is still valid as it runs! While this restriction is inconvenient, it is
  -- unavoidable -- process execution could be delayed arbitrarily after lease
  -- acquisition, and leases eventually time out and unlock themselves again.
  -- This also gives us some flexibility -- for some tasks, we might already have
  -- achieved safety through transaction design, and we just want to use leases
  -- to reduce conflicts. In such cases, it's not important to check that the lock
  -- still holds.
  TaskRegistry
  = TaskRegistry
      { taskRegistrySpace :: TaskSpace,
        getTaskRegistry ::
          IORef
            ( Map TaskName
                ( TaskID,
                  Int,
                  Transaction Bool -> Transaction ReleaseResult -> IO ()
                )
            )
      }

empty ::
  -- | Base subspace to contain task lease data. This will be extended
  -- with the string "TS"
  Subspace ->
  IO TaskRegistry
empty ss =
  TaskRegistry
    <$> pure (taskSpace $ FDB.extend ss [FDB.Bytes "TS"])
    <*> newIORef mempty

numTasks :: TaskRegistry -> IO Int
numTasks tr = M.size <$> readIORef (getTaskRegistry tr)

addTask ::
  TaskRegistry ->
  -- | A unique string name for this task. If the given name already
  -- exists, it will be overwritten.
  TaskName ->
  -- | How long to lock the task when it runs, in seconds.
  Int ->
  -- | An action that is run once per lease acquisition. It is passed two
  -- transactions: one that returns true if the acquired lease is still
  -- valid, and another that releases the lease. The latter can be used to
  -- atomically finish a longer-running computation.
  (Transaction Bool -> Transaction ReleaseResult -> IO ()) ->
  Transaction ()
addTask (TaskRegistry ts tr) taskName dur f = do
  taskID <- extractID <$> ensureTask ts taskName
  liftIO $ atomicModifyIORef' tr ((,()) . M.insert taskName (taskID, dur, f))
  where
    extractID (AlreadyExists t) = t
    extractID (NewlyCreated t) = t

-- | Run a random task in the task registry. If no tasks are available, returns
-- false.
runRandomTask :: FDB.Database -> TaskRegistry -> IO Bool
runRandomTask db (TaskRegistry ts tr) = do
  tr' <- readIORef tr
  let toDur taskName = case M.lookup taskName tr' of
        Nothing -> 5 -- code below will warn
        Just (_, dur, _) -> dur
  FDB.runTransaction db (acquireRandomUnbiased ts toDur) >>= \case
    Nothing -> return False
    Just (taskName, lease, howAcquired) -> case M.lookup taskName tr' of
      Nothing ->
        return False --TODO: warn? this implies tasks outside registry
      Just (taskID, _dur, f) -> do
        tid <- myThreadId
        logDebug $
          showText tid
            <> " starting on task "
            <> showText taskName
            <> " with task ID "
            <> showText taskID
            <> " acquired by "
            <> showText howAcquired
        logAndRethrowErrors (show taskName) $
          f
            ( (&&) <$> isLeaseValid ts taskID lease
                <*> isLocked ts taskName
            )
            (release ts taskName lease)
        return True
