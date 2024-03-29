-- | Contains the 'JobConfig' type.
module FDBStreaming.JobConfig
  ( JobConfig (..),
    JobSubspace,
    PartitionCount
  )
where

import           Data.ByteString                ( ByteString )
import qualified Control.Logger.Simple as Log
import Data.Word (Word16, Word8)
import qualified FoundationDB as FDB
import qualified FoundationDB.Layer.Subspace as FDB
import qualified System.Metrics as Metrics

-- | The top-level subspace that contains all state for a given streaming job.
type JobSubspace = FDB.Subspace

type PartitionCount = Word8

-- | Specifies configuration for a single streaming job. This is passed to
-- 'FDBStreaming.runJob'.
data JobConfig
  = JobConfig
      { -- | The connection to FoundationDB. You can create a connection with
        -- 'FDB.withFoundationDB' from the @foundationdb-haskell@ package.
        jobConfigDB :: FDB.Database,
        -- | Subspace that will contain all state for the stream topology. A
        -- subspace is essentially a common prefix shared by a set of keys. See
        -- FoundationDB's docs for more info.
        jobConfigSS :: JobSubspace,
        -- | Optional metrics store from the @ekg-core@ package. If supplied,
        -- the job will report metrics regarding messages processed.
        streamMetricsStore :: Maybe Metrics.Store,
        -- | Number of messages to process per transaction per worker thread.
        -- The larger the messages being processed, the smaller this number
        -- should be. This can be overridden on a per-step basis with
        -- 'FDBStreaming.StreamStep.Internal.StreamStepConfig'.
        msgsPerBatch :: Word16,
        -- | Length of time an individual worker should work on a single stage of the
        -- pipeline before stopping and trying to work on something else. Higher
        -- values are more efficient in normal operation, but if machines fail,
        -- higher values can be a worst-case lower bound on end-to-end latency.
        leaseDuration :: Int,
        -- | Number of threads to dedicate to running each step of your stream
        -- topology. It's probably a good idea to set this to no more than 8.
        -- Prefer running more instances of the executable, rather than one
        -- instance with many threads -- the FoundationDB client library is
        -- single-threaded, and can become a bottleneck if too many threads are
        -- sharing it.
        numStreamThreads :: Int,
        -- | Number of threads to dedicate to periodic background jobs that are
        -- run by the stream processing system. This includes propagating
        -- watermarks, cleaning up old data, etc. A good default is one thread
        -- -- the thread will be mostly idle.
        numPeriodicJobThreads :: Int,
        -- | Default number of partitions per stream and table. In streams, the
        -- number of concurrent readers equals the number of partitions, so more
        -- partitions means more throughput at the expense of more worker threads.
        -- In tables, the number of concurrent writers is bounded by the number of
        -- partitions.
        defaultNumPartitions :: PartitionCount,
        -- | Default target number of bytes to store per k/v in topics. Small
        -- messages will be combined into one k/v to reach this target. This
        -- generally improves throughput and reduces disk usage in FDB.
        defaultChunkSizeBytes :: Word,
        -- | Logging level for fdb-streaming log messages
        logLevel :: Log.LogLevel,
        -- | Internal tasks to remove from the task registry when the job
        -- starts. This is used to clean up old state after modifying an
        -- existing job and removing or renaming a step in the pipeline. After
        -- your modified job has started, you will see warnings about unexpected
        -- tasks. Add these task names to this list and redeploy. They will
        -- be cleaned up when the job starts. You can then set this to an
        -- empty list again. The job migration helper, once it exists, will also
        -- help you in setting this value if needed.
        tasksToCleanUp :: [ByteString]
      }
