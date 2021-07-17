{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module FDBStreaming.TaskRegistry
  ( TaskRegistry (..),
    empty,
    numTasks,
    addTask,
    removeTask,
    runRandomTask,
  )
where

import Control.Concurrent (myThreadId)
import Control.Logger.Simple (logWarn, logDebug, showText)
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Map (Map)
import qualified Data.Map as M
import FDBStreaming.TaskLease (EnsureTaskResult (AlreadyExists, NewlyCreated), ReleaseResult, TaskID, TaskName, TaskSpace, acquireRandom, ensureTask, isLeaseValid, isLocked, release, taskSpace)
import qualified FDBStreaming.TaskLease as TL
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
-- This comes with a caveat: the worker must manually check that the lease
-- is still valid as it runs! While this restriction is inconvenient, it is
-- unavoidable -- process execution could be delayed arbitrarily after lease
-- acquisition, and leases eventually time out and unlock themselves again.
-- This also gives us some flexibility -- for some tasks, we might already have
-- achieved safety through transaction design, and we just want to use leases
-- to reduce conflicts. In such cases, it's not important to check that the lock
-- still holds.
data TaskRegistry
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
  TaskRegistry (taskSpace $ FDB.extend ss [FDB.Bytes "TS"]) <$> newIORef mempty

numTasks :: TaskRegistry -> IO Int
numTasks tr = M.size <$> readIORef (getTaskRegistry tr)

-- | Returns True iff the given TaskName has already been registered within this
-- process. If this is true, it is strong evidence that there is an internal bug
-- in the user's pipeline or in FDBStreaming itself, and that it has generated
-- the same TaskName for two distinct tasks. If that happens, parts of the job
-- DAG will never run.
-- Note that this is distinct from the same TaskName being registered inside
-- FoundationDB multiple times, which is expected to happen when many processes
-- start up -- each one registers all tasks in the DAG, which is harmless
-- because the operation is idempotent.
alreadyRegisteredLocally :: MonadIO m => TaskRegistry -> TaskName -> m Bool
alreadyRegisteredLocally (TaskRegistry _ tr) taskName = do
  taskMap <- liftIO $ readIORef tr
  return (M.member taskName taskMap)

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
addTask reg@(TaskRegistry ts tr) taskName dur f = do
  logDebug $ "Registering task " <> showText taskName
  taskID <- extractID <$> ensureTask ts taskName
  alreadyRegistered <- alreadyRegisteredLocally reg taskName
  when alreadyRegistered $ do
    logWarn $ "Task is already registered: "
               <> showText taskName
  liftIO $ atomicModifyIORef' tr ((,()) . M.insert taskName (taskID, dur, f))
  where
    extractID (AlreadyExists t) = t
    extractID (NewlyCreated t) = t

-- | Remove a task from the TaskRegistry.
removeTask ::
  TaskRegistry ->
  TaskName ->
  Transaction ()
removeTask (TaskRegistry ts tr) taskName = do
  TL.removeTask ts taskName
  liftIO $ atomicModifyIORef' tr ((,()) . M.delete taskName)

-- | Run a random task in the task registry. If no tasks are available, returns
-- false.
runRandomTask :: FDB.Database -> TaskRegistry -> IO Bool
runRandomTask db (TaskRegistry ts tr) = do
  tr' <- readIORef tr
  let toDur taskName = case M.lookup taskName tr' of
        Nothing -> 5 -- code below will warn
        Just (_, dur, _) -> dur
  FDB.runTransaction db (acquireRandom ts toDur) >>= \case
    Nothing -> return False
    Just (taskName, lease, howAcquired) -> case M.lookup taskName tr' of
      Nothing -> do
        logWarn $
          " found unexpected task "
          <> showText taskName
          <> " acquired by "
          <> showText howAcquired
          <> ". If you have redeployed your job after"
          <> " deleting a pipeline step, please add"
          <> " " <> showText taskName
          <> " to 'tasksToCleanUp' in your JobConfig and redeploy. Afterwards,"
          <> " you can set 'tasksToCleanUp' to an empty list again."
          <> " If you have not recently renamed or deleted a step, please ensure"
          <> " that you are using a different jobConfigSS"
          <> " from all other jobs running on this FoundationDB cluster."
          <> " This task will be ignored and this thread will try to find other"
          <> " tasks to execute."
        return True
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
                <*> isLocked ts taskName taskID
            )
            (release ts taskName lease)
        logDebug $ "Finished running task " <> showText taskName
        return True
