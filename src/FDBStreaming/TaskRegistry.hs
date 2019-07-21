{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}

module FDBStreaming.TaskRegistry  where

import           FoundationDB (Transaction)
import qualified FoundationDB as FDB
import           FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import           Data.Map (Map)
import qualified Data.Map as M

import FDBStreaming.TaskLease

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
empty ss dur =
  TaskRegistry (taskSpace $ FDB.extend ss [FDB.Bytes "TS"]) mempty dur

addTask
  :: TaskRegistry
  -> TaskName
        -- ^ A unique string name for this task.If the given name already
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

runRandomTask :: FDB.Database -> TaskRegistry -> IO ()
runRandomTask db (TaskRegistry ts tr dur) =
  FDB.runTransaction db (acquireRandom ts dur) >>= \case
    Nothing                -> return ()
    Just (taskName, lease) -> case M.lookup taskName tr of
      Nothing -> return () --TODO: warn? this implies tasks outside registry
      Just (taskID, f) ->
        f (isLeaseValid ts taskID lease) (release ts taskName lease)
