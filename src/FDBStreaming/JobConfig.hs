module FDBStreaming.JobConfig (
  JobConfig(..),
  JobSubspace
) where

import Data.Word (Word8)
import qualified FoundationDB as FDB
import qualified FoundationDB.Layer.Subspace as FDB
import qualified System.Metrics as Metrics

-- | The top-level subspace that contains all state for a given streaming job.
type JobSubspace = FDB.Subspace

-- TODO: other persistence backends
-- TODO: should probably rename to TopologyConfig
data JobConfig
  = JobConfig
      { jobConfigDB :: FDB.Database,
        -- | subspace that will contain all state for the stream topology
        jobConfigSS :: JobSubspace,
        streamMetricsStore :: Maybe Metrics.Store,
        -- | Number of messages to process per transaction per thread per partition
        msgsPerBatch :: Word8,
        -- | Length of time an individual worker should work on a single stage of the
        -- pipeline before stopping and trying to work on something else. Higher
        -- values are more efficient in normal operation, but if enough machines fail,
        -- higher values can be a worst-case lower bound on end-to-end latency.
        -- Only applies to pipelines run with the LeaseBasedStreamWorker monad.
        leaseDuration :: Int,
        -- | Number of threads to dedicate to running each step of your stream
        -- topology. A good default is to set this to the number of cores you
        -- have available.
        numStreamThreads :: Int,
        -- | Number of threads to dedicate to periodic background jobs that are
        -- run by the stream processing system. This includes propagating
        -- watermarks, cleaning up old data, etc. A good default is one thread
        -- -- the thread will be mostly idle.
        numPeriodicJobThreads :: Int
      }
