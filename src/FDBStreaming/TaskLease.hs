{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

-- | Locks for recurring, unending tasks. Acquiring a 'TaskName' allows a worker
-- to exclusively work on a task for a period of time. The worker can then
-- transactionally commit the result of its work, failing if the lock has since
-- been reassigned to another worker (as it would in the case where the worker
-- failed to commit its results before its lease on the lock timed out).
-- All workers must have NTP configured correctly. A worker with a bad clock
-- could lock a task for longer than expected, or steal a lock before it
-- has actually timed out.
-- This design is based on the <https://github.com/apple/foundationdb/blob/master/layers/taskbucket/__init__.py task bucket layer>
-- in FoundationDB.
--
-- Currently, no logic in FDBStreaming requires mutual exclusion to function, so
-- even if NTP is misconfigured, the exactly-once semantics will be preserved.
-- However, locks are used to reduce conflicts between workers. Performance will
-- degrade if a worker has a rogue clock.
--
-- This implementation is inspired by the FoundationDB
-- <https://github.com/apple/foundationdb/blob/master/layers/taskbucket/__init__.py task bucket layer>, but has several significant differences.
module FDBStreaming.TaskLease (
  secondsSinceEpoch,
  TaskSpace(..),
  taskSpace,
  TaskID(..),
  TaskName(..),
  AcquiredLease(leaseNumber),
  isLeaseValid,
  ensureTask,
  removeTask,
  EnsureTaskResult(..),
  TryAcquireResult(..),
  tryAcquire,
  acquireRandom,
  HowAcquired(..),
  release,
  ReleaseResult(..),
  getTaskID,
  isLocked,
  listAllTasks
) where

import FDBStreaming.TaskLease.Internal
