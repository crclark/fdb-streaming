{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | This module exports a BatchWriter interface for FoundationDB that solves
-- the following problems:
--
-- * Idempotency: when writing to FDBStreaming from an outside system, we need
-- idempotency to protect against CommitUnknownResult errors: circumstances
-- where we don't know whether the write succeeded. Note that this is not a
-- problem for the majority of FDBStreaming operations, which are already
-- idempotent (and monotonic).
-- * Achieving high throughput: FoundationDB requires us to batch writes to
-- achieve high throughput. However, we would like to be able to ignore batching
-- when we write to FDBStreaming from external systems. By transparently
-- batching in the background, the BatchWriter interface gives support for
-- one-write-at-a-time semantics while maintaining good throughput.
module FDBStreaming.Util.BatchWriter (
  BatchWriterConfig(..),
  BatchWriter,
  BatchWrite(..),
  BatchWriteResult(..),
  batchWriter,
  batchWriterAsync,
  defaultBatchWriterConfig,
  write
) where

import Data.Either (partitionEithers)
import Data.Traversable (for)
import Data.Foldable (for_)
import Data.Int (Int64)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import qualified FoundationDB as FDB
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import Control.Concurrent.Async (Async, async)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBQueue (TBQueue, newTBQueueIO, writeTBQueue, flushTBQueue, tryReadTBQueue, isFullTBQueue)
import Control.Concurrent.STM.TMVar (TMVar, putTMVar, newEmptyTMVarIO, takeTMVar)
import Data.ByteString (ByteString)
import Data.Maybe (fromMaybe, maybe)
import System.Clock (Clock(Monotonic), getTime, toNanoSecs, diffTimeSpec)
import FDBStreaming.Util (currMillisSinceEpoch, logErrors, withOneIn)

-- | The result of a batch write request.
data BatchWriteResult =
  -- | The write succeeded.
  Success
  -- | The write was not made, because a write with the same idempotency key
  -- was already committed in the past.
  | SuccessAlreadyExisted
  -- | The queue of pending writes is full. This indicates that the writer isn't
  -- able to keep up with the volume of write requests it is receiving. You may
  -- need to create more BatchWriter instances, or FoundationDB itself may be
  -- saturated.
  | FailedQueueFull
  -- | The write failed because the FoundationDB transaction failed to commit.
  -- The underlying error is returned.
  | Failed FDB.Error
  deriving (Eq, Show)

data BatchWrite a = BatchWrite
  { -- | Must be unique to this write. If not unique, only the first write will
    -- succeed.
    idempotencyKey :: ByteString
    -- | The size of the write in bytes. If provided, this will contribute to
    -- the 'maxBatchBytes' limit on the batch.
  , writeSizeHint :: Maybe Word
  , batchWriteItem :: a
  -- | TMVar that will be filled by the writer loop with the result of the write
  -- operation.
  } deriving (Eq, Show)

-- | Represents a single write request to the BatchWriter.
data BatchWriteSpec a = BatchWriteSpec
  { batchWriteSpec :: BatchWrite a
  , result :: TMVar BatchWriteResult
  } deriving (Eq)

data BatchWriterConfig = BatchWriterConfig
  { -- | Desired maximum latency of a single write operation. The BatchWriter
    -- will wait at most this long for writes to batch before committing a
    -- batch, even if waiting longer could allow us to commit a bigger batch.
    -- Note that total latency will be longer than this value -- it does not
    -- count the time taken to run the batch write callback; it only limits how
    -- long a batch will wait to fill up with writes before trying to commit.
    desiredMaxLatencyMillis :: Word
    -- | The maximum number of bytes to write in a batch. This only counts the
    -- length of the messages themselves, not the size of keys and any other
    -- operations that may be added in the 'writeBatch' callback. Only
    -- 'BatchWrite' messages for which the optional field 'writeSizeHint' is
    -- provided will be counted against this limit.
  , maxBatchBytes :: Word
    -- | Maximum number of uncommitted writes to queue. If the batch writer is
    -- unable to keep up with write requests, this queue will fill up. Once
    -- full, writes will be rejected with 'QueueFull'. This gives us a
    -- backpressure mechanism.
  , maxQueueSize :: Word
    -- | Maximum number of writes to commit in one batch.
  , maxBatchSize :: Word
    -- | Minimum number of seconds to store an idempotency key after it has
    -- first been seen. Set this so that it is unlikely or impossible that your
    -- application will send duplicate writes this many seconds apart. Larger
    -- values will increase the storage used in FoundationDB for idempotency
    -- keys.
    --
    -- Note: To ensure that the implementation is scalable and efficient, keys
    -- may be remembered for longer than this time period in practice. This
    -- setting is a lower bound.
    --
    -- Warning: changing this value for a 'BatchWriter' that has already been
    -- running in a given subspace will cause all past idempotency keys to be
    -- forgotten.
  , minIdempotencyMemoryDurationSeconds :: Word
    -- | Probabilistically attempt to delete idempotency keys in FDB older than
    -- minIdempotencyMemoryDurationSeconds approximately every
    -- idempotencyCleanupPeriod batches.
  , idempotencyCleanupPeriod :: Word
  } deriving (Eq, Show)

defaultBatchWriterConfig :: BatchWriterConfig
defaultBatchWriterConfig = BatchWriterConfig 300 10_000 1000 500 (60*60*24) 500

-- | Handle to an asynchronous thread that periodically commits batches of
-- writes to FoundationDB.
data BatchWriter a = BatchWriter
  { batchWriterConfig :: BatchWriterConfig
  , queue :: TBQueue (BatchWriteSpec a)
  , idemSS :: FDB.Subspace
  , writeBatch :: [a] -> FDB.Transaction ()
  , _batchWriterAsync :: Async ()
  }

-- | Returns the 'Async' thread the batch writer is running on.
batchWriterAsync :: BatchWriter a -> Async ()
batchWriterAsync = _batchWriterAsync

-- | Left -> new. Right -> already written.
type IdemCheck a = Either (BatchWriteSpec a) (BatchWriteSpec a)

mkIdemSS :: FDB.Subspace -> Int64 -> FDB.Subspace
mkIdemSS ss i = FDB.extend ss [FDB.Bytes "id", FDB.Int (fromIntegral i)]

idemSlot :: BatchWriterConfig -> Int64 -> IO Int64
idemSlot BatchWriterConfig{minIdempotencyMemoryDurationSeconds} n = do
  t <- currMillisSinceEpoch
  let d = fromIntegral minIdempotencyMemoryDurationSeconds
  let slot = t - (t `mod` d) + n*d
  return slot

lastIdemSS :: BatchWriterConfig -> FDB.Subspace -> IO FDB.Subspace
lastIdemSS cfg ss = do
  slot <- idemSlot cfg (-1)
  return (mkIdemSS ss slot)

currIdemSS :: BatchWriterConfig -> FDB.Subspace -> IO FDB.Subspace
currIdemSS cfg ss = do
  slot <- idemSlot cfg 0
  return (mkIdemSS ss slot)

nextIdemSS :: BatchWriterConfig -> FDB.Subspace -> IO FDB.Subspace
nextIdemSS cfg ss = do
  slot <- idemSlot cfg 1
  return (mkIdemSS ss slot)

cleanupOldIdemKeys :: BatchWriterConfig -> FDB.Subspace -> FDB.Transaction ()
cleanupOldIdemKeys cfg ss = do
  let totalRange = FDB.subspaceRange ss
  lastSS <- liftIO $ lastIdemSS cfg ss
  let lastSlotRange = FDB.subspaceRange lastSS
  kBeginF <- FDB.getKey (FDB.rangeBegin totalRange)
  kEnd <- FDB.getKey (FDB.rangeEnd lastSlotRange) >>= FDB.await
  kBegin <- FDB.await kBeginF
  FDB.clearRange kBegin kEnd

pushIdemCheck :: BatchWriterConfig
              -> FDB.Subspace
              -> TBQueue (FDB.Future (IdemCheck a))
              -> BatchWriteSpec a
              -> FDB.Transaction ()
pushIdemCheck cfg ss q w = do
  ss' <- liftIO $ currIdemSS cfg ss
  let k = FDB.pack ss' [FDB.Bytes $ idempotencyKey (batchWriteSpec w)]
  res <- FDB.get k
  liftIO
    $ atomically
    $ writeTBQueue q
    $ fmap (maybe (Left w) (const $ Right w)) res

writeIdemKeys :: BatchWriterConfig
              -> FDB.Subspace
              -> [BatchWriteSpec a]
              -> FDB.Transaction ()
writeIdemKeys cfg ss xs = do
  ssCurr <- liftIO $ currIdemSS cfg ss
  ssNext <- liftIO $ nextIdemSS cfg ss
  for_ xs $ \x -> for_ [ssCurr, ssNext] $ \ss' -> do
    let k = FDB.pack ss' [FDB.Bytes $ idempotencyKey (batchWriteSpec x)]
    FDB.set k ""

getIdemLookupResults :: TBQueue (FDB.Future (IdemCheck a))
                     -> FDB.Transaction ([BatchWriteSpec a], [BatchWriteSpec a])
getIdemLookupResults q = do
  xs <- liftIO $ atomically $ flushTBQueue q
  xs' <- for xs FDB.await
  return $ partitionEithers xs'

-- | General outline of how this works: create a transaction, then start pulling
-- items from the queue of write requests in a loop. For each 'BatchWrite'
-- request, push an idempotency check future onto a second queue (hiding
-- the latency of the check). On each iteration of this loop, check if we have
-- hit any of the configured thresholds. If we have, commit the batch.
--
-- A lot of the complexity comes from needing to keep track of the result for
-- each 'BatchWrite' request so that we can fill the TMVar for each one with
-- the correct result. We need to track the requests we have received regardless
-- of whether the FDB transaction succeeds or fails, which means we need to keep
-- them in a separate collection in addition to returning them from the
-- transaction.
batchWriterLoop :: FDB.Database
                -> BatchWriterConfig
                -> TBQueue (BatchWriteSpec a)
                -> FDB.Subspace
                -> ([a] -> FDB.Transaction ())
                -> IO ()
batchWriterLoop db cfg@BatchWriterConfig{..} inQ ss f = forever $ logErrors ("batch writer: " ++ show ss) $ do
  -- Before anything else, try to clean up old keys
  withOneIn idempotencyCleanupPeriod
    $ FDB.runTransaction db
    $ cleanupOldIdemKeys cfg ss

  -- stores pending idempotency key lookup futures.
  idemLookups <- newTBQueueIO (fromIntegral maxBatchSize)
  -- stores all items received in this iteration.
  allRcvd <- newTBQueueIO (fromIntegral maxBatchSize)
  startTime <- getTime Monotonic
  res <- FDB.runTransactionWithConfig' tCfg db (go startTime 0 0 idemLookups allRcvd)
  case res of
    Left err -> do
      xs <- atomically $ flushTBQueue allRcvd
      for_ xs $ \x -> atomically $ putTMVar (result x) (Failed err)
    Right (newlyWritten, alreadyWritten) -> do
      for_ alreadyWritten $ \x -> atomically $ putTMVar (result x) SuccessAlreadyExisted
      for_ newlyWritten $ \x -> atomically $ putTMVar (result x) Success

  where
    go startTime !currBytes !currBatchSize idemLookups allRcvd = do
      shouldCommit <- terminationCondition startTime currBytes currBatchSize
      if shouldCommit
        then do
          (toWrite, alreadyWritten) <- getIdemLookupResults idemLookups
          f (fmap (batchWriteItem . batchWriteSpec) toWrite)
          writeIdemKeys cfg ss toWrite
          return (toWrite, alreadyWritten)
        else
          liftIO (atomically $ tryReadTBQueue inQ) >>= \case
            Nothing ->
              go startTime currBytes currBatchSize idemLookups allRcvd
            Just x -> do
              pushIdemCheck cfg ss idemLookups x
              liftIO $ atomically $ writeTBQueue allRcvd x
              go startTime
                 (currBytes + fromMaybe 0 (writeSizeHint (batchWriteSpec x)))
                 (succ currBatchSize)
                 idemLookups
                 allRcvd

    terminationCondition startTime currBytes currBatchSize = do
      elapsedMillis <- (`div` 1000000)
                        . toNanoSecs
                        . diffTimeSpec startTime
                       <$> liftIO (getTime Monotonic)
      return (fromIntegral elapsedMillis >= desiredMaxLatencyMillis
              || currBytes >= maxBatchBytes
              || currBatchSize >= maxBatchSize)

    tCfg = FDB.TransactionConfig
            { -- Not idempotent because of the side effects of reading/writing
              -- queues. The idempotence of this entire process happens at a
              -- higher level of abstraction.
              idempotent = False
            , snapshotReads = False
            -- Again, because of side effects, we can't retry without
            -- potentially losing messages.
            , maxRetries = 0
            -- Max timeout, since the user can set 'desiredMaxLatencyMillis'.
            , timeout = 5000}

-- | Create a 'BatchWriter'.
batchWriter :: BatchWriterConfig
            -> FDB.Database
            -> FDB.Subspace
            -- ^ Subspace to store idempotency keys
            -> ([a] -> FDB.Transaction ())
            -- ^ Callback to commit a batch of writes. This callback doesn't
            -- need to handle idempotency checks; this is done automatically by
            -- the batch writer loop. This callback *must* accept all
            -- items it is given, or throw an exception. Any other behavior can
            -- result in lost writes.
            -> IO (BatchWriter a)
batchWriter batchWriterConfig@BatchWriterConfig{maxQueueSize} db idemSS writeBatch = do
  queue <- newTBQueueIO (fromIntegral maxQueueSize)
  _batchWriterAsync <- async (batchWriterLoop db batchWriterConfig queue idemSS writeBatch)
  return BatchWriter{..}

-- | Write an item using a 'BatchWriter'. Blocks until the batch this write
-- has been assigned to has been committed.
write :: BatchWriter a -> BatchWrite a -> IO BatchWriteResult
write bw x = do
  tmvar <- newEmptyTMVarIO
  wasEnqueued <- atomically $ do
    full <- isFullTBQueue (queue bw)
    if full
      then return False
      else do
        writeTBQueue (queue bw) (BatchWriteSpec x tmvar)
        return True
  if wasEnqueued
    then atomically (takeTMVar tmvar)
    else return FailedQueueFull
