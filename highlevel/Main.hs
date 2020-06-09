{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import GHC.Records
import           FDBStreaming
import qualified FDBStreaming.AggrTable as AT
import           FDBStreaming.Topic
import           FDBStreaming.Watermark (Watermark(Watermark, watermarkUTCTime), WatermarkSS, getCurrentWatermark)
import qualified FDBStreaming.Util.BatchWriter as BW
import qualified FDBStreaming.Push as Push
import FDBStreaming.Util (millisSinceEpoch)
import           Control.Monad
import           Control.Monad.IO.Class (liftIO)
import           Control.Concurrent
import           Control.Concurrent.Async       ( async, wait, forConcurrently )
import           Control.Concurrent.STM         ( TVar
                                                , readTVarIO
                                                , atomically
                                                , modifyTVar'
                                                , newTVarIO
                                                )
import           Control.Exception              ( catches
                                                , Handler(..)
                                                , SomeException
                                                )
import Control.Logger.Simple (LogLevel(LogDebug))
import           Data.List                      ( sortOn )
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Coerce                    ( coerce )
import           Data.Time.Clock                ( diffUTCTime, getCurrentTime )
import           Data.Word                      ( Word16, Word8 )
import           FoundationDB                  as FDB
import           FoundationDB.Error
import           FoundationDB.Layer.Subspace   as FDB
import           FoundationDB.Layer.Tuple      as FDB
import qualified FoundationDB.Options.NetworkOption as NetOp
import Data.Foldable (for_)
import Data.UUID as UUID
import           Data.UUID                      ( UUID )
import           Data.UUID.V4                   as UUID
                                                ( nextRandom )
import           GHC.Generics                   ( Generic )
import           System.Random                  ( randomIO, randomRIO )
import           Data.Maybe                     ( fromMaybe, fromJust )
import           Data.Persist                     ( Persist )
import qualified Data.Persist                    as Persist
import           Data.Functor.Identity (Identity(..))
import           Data.Monoid (All (..))
import           System.Clock (Clock(Monotonic), getTime, toNanoSecs, diffTimeSpec)
import qualified System.Metrics as Metrics
import           System.Metrics.Distribution (Distribution)
import qualified System.Metrics.Distribution as Distribution
import           System.Metrics.Gauge (Gauge)
import qualified System.Metrics.Gauge as Gauge
import System.Remote.Monitoring.Statsd (defaultStatsdOptions, forkStatsd)
import           Text.Printf                    ( printf )
import Options.Generic
import Data.Int (Int64)
import Data.Either (fromRight)
import qualified Data.Text as Text

newtype Timestamp = Timestamp { unTimestamp :: Int64 }
  deriving (Show, Eq, Ord, Generic, Persist)

instance Persist UUID where
  put = Persist.put . UUID.toWords
  get = (\(q,w,e,r) -> UUID.fromWords q w e r) <$> Persist.get

newtype OrderID = OrderID { unOrderID :: UUID }
  deriving (Show, Eq, Ord, Generic, Persist)

instance Message OrderID where
  toMessage = Persist.encode
  fromMessage = OrderID . fromRight (error "Failed to decode OrderID") . Persist.decode

instance AT.TableKey OrderID where
  toKeyBytes = toMessage
  fromKeyBytes = fromMessage

data Order = Order
  { placedAt :: Timestamp
  , orderID :: OrderID
  , isFraud :: Bool
  , isInStock :: Bool
  , orderInstructions :: Text
  } deriving (Show, Eq, Generic, Persist)

instance Message Order where
  toMessage = Persist.encode
  fromMessage = fromRight (error "Failed to decode Order") . Persist.decode

randOrder :: IO Order
randOrder = do
  placedAt  <- Timestamp . millisSinceEpoch <$> getCurrentTime
  orderID   <- OrderID <$> UUID.nextRandom
  isFraud   <- randomIO
  isInStock <- randomIO
  let orderInstructions = "This is some text, in order to simulate a larger message."
  return Order { .. }

data FraudResult = FraudResult
  { orderID :: OrderID
  , isFraud :: Bool
  } deriving (Show, Eq, Generic, Persist)

instance Message FraudResult where
  toMessage = Persist.encode
  fromMessage = fromRight (error "Failed to decode FraudResult") . Persist.decode

-- TODO: use getField
fraudOrderID :: FraudResult -> OrderID
fraudOrderID FraudResult{orderID} = orderID

-- | Super-sophisticated fraud detection! Asks the order if it is fraudulent.
isFraudulent :: Order -> FraudResult
isFraudulent Order{..} = FraudResult{..}

data InStockResult = InStockResult
  { orderID ::  OrderID
  , isInStock :: Bool
  } deriving (Show, Eq, Generic, Persist)

instance Message InStockResult where
  toMessage = Persist.encode
  fromMessage = fromRight (error "failed to decode InStockResult") . Persist.decode

invOrderID :: InStockResult -> OrderID
invOrderID InStockResult{orderID} = orderID

inventoryCheck :: Order -> InStockResult
inventoryCheck Order { .. } = InStockResult { .. }

data OrderDetails = OrderDetails
  { orderID :: OrderID
  , details :: ByteString
  } deriving (Show, Eq, Ord, Generic, Persist)

instance Message OrderDetails where
  toMessage = Persist.encode
  fromMessage = fromRight (error "failed to decode OrderDetails") . Persist.decode

randOrderDetails :: Order -> IO OrderDetails
randOrderDetails Order { .. } = do
  -- TODO: This was a bottleneck because we were creating a Gen on each call.
  -- Switch from random to randomGen.
  -- It called withSystemRandom, whose docs warn
  -- "This is a somewhat expensive function, and is intended to be called only occasionally"
  details <- return "hi"
  return OrderDetails { .. }

goodDetails :: ByteString -> Bool
goodDetails bs = odd $ sum $ BS.unpack bs

data LatencyStats = LatencyStats
  { timeElapsed :: !Int64
  , numFinished :: !Int
  } deriving (Show, Eq, Ord)

instance Semigroup LatencyStats where
  (LatencyStats x1 y1) <> (LatencyStats x2 y2) =
    LatencyStats (x1 + x2) (y1 + y2)

withProbability :: Int -> IO () -> IO ()
withProbability p f = do
  r <- randomRIO (1,100)
  when (r <= p) f

awaitOrder
  :: Database
  -> AT.AggrTable OrderID All
  -> TVar LatencyStats
  -> Distribution
  -> Gauge
  -> Order
  -> IO ()
awaitOrder db table stats latencyDist awaitGauge order@Order{orderID} = withProbability 1 $ catches (do
  Gauge.inc awaitGauge
  _ <- AT.getBlocking db table orderID
  endTime <- millisSinceEpoch <$> getCurrentTime
  let diffMillis = endTime - (unTimestamp $ placedAt order)
  let statsDiff = LatencyStats diffMillis 1
  atomically $ modifyTVar' stats (<> statsDiff)
  Distribution.add latencyDist (fromIntegral diffMillis)
  Gauge.dec awaitGauge
  )
  [ Handler (\(e :: Error) ->
              putStrLn $ "Caught " ++ show e ++ " while awaiting table data")
  , Handler (\(e :: SomeException) ->
                putStrLn $ "Caught " ++ show e ++ " while awaiting table data!")
  ]

-- | Pushes a batch of random orders onto the given topic, awaits its
-- arrival in the given AggrTable, and updates the given latency statistics
-- TVar.
placeAndAwaitOrders :: Database
                    -> BW.BatchWriter Order
                    -> AT.AggrTable OrderID All
                    -> TVar LatencyStats
                    -> Distribution
                    -> Gauge
                    -> Int
                    -- ^ batch size
                    -> Bool
                    -- ^ whether to watch orders to measure end_to_end_latency
                    -> IO ()
placeAndAwaitOrders db bw table stats latencyDist awaitGauge batchSize shouldWatch = catches ( do
  orders <- replicateM batchSize randOrder
  writeAsyncs <- forM orders $ \order -> async $ BW.write bw $ BW.BatchWrite (toMessage (getField @"orderID" order)) Nothing order
  for_ writeAsyncs $ \a -> do
    res <- wait a
    when (BW.isFailed res) $ putStrLn $ "Failed write: " ++ show res
  when shouldWatch
    $ forM_ orders $ awaitOrder db table stats latencyDist awaitGauge
  )
  [ Handler (\case
               Error (MaxRetriesExceeded (CError NotCommitted)) ->
                 putStrLn "Caught NotCommitted when writing to topic!"
               e -> putStrLn $ "caught fdb error " ++ show e ++ " while writing to topic")
  , Handler (\(e :: SomeException) ->
                putStrLn $ "Caught " ++ show e ++ " while writing to topic!")
  ]

-- TODO: far too many params!
orderGeneratorLoop :: Database
                   -> BW.BatchWriter Order
                   -> AT.AggrTable OrderID All
                   -> Int
                   -- ^ requests per second
                   -> Int
                   -- ^ batch size
                   -> TVar LatencyStats
                   -> Distribution
                   -> Gauge
                   -> Bool
                   -- ^ whether to watch orders to measure end-to-end latency
                   -> IO ()
orderGeneratorLoop db bw table rps batchSize stats latencyDist awaitGauge shouldWatch = do
  let delay = 1000000 `div` (rps `div` batchSize)
  void $ forkIO $ placeAndAwaitOrders db bw table stats latencyDist awaitGauge batchSize shouldWatch
  threadDelay delay
  orderGeneratorLoop db bw table rps batchSize stats latencyDist awaitGauge shouldWatch

latencyReportLoop :: TVar LatencyStats -> IO ()
latencyReportLoop stats = do
  LatencyStats{timeElapsed, numFinished} <- readTVarIO stats
  let avgMilliseconds = (timeElapsed `div` fromIntegral numFinished)
  putStrLn $ "Processed "
             ++ show numFinished
             ++ " orders with average latency of "
             ++ show avgMilliseconds
             ++ " milliseconds"
  threadDelay 1000000
  latencyReportLoop stats

topology :: (MonadStream m) => Stream Order -> m (AT.AggrTable OrderID All)
topology input = do
  let fraudChecks = fmap isFraudulent input
  let invChecks = fmap inventoryCheck input
  let dtls = benignIO (fmap Just . randOrderDetails) input
  fraudInvJoin <- oneToOneJoin "f_i_join"
                               fraudChecks
                               invChecks
                               fraudOrderID
                               invOrderID
                               (\FraudResult{orderID, isFraud}
                                  InStockResult{isInStock}
                                  -> (orderID, not isFraud && isInStock))
  finalJoin <- oneToOneJoin "final_join"
                            fraudInvJoin
                            dtls
                            fst
                            (\OrderDetails{orderID} -> orderID)
                            (\(oid, isGood) OrderDetails{details}
                              -> (oid, All $ isGood && goodDetails details))
  let grouped = groupBy (pure . fst) finalJoin
  orderStatusTable <- aggregate "order_table" grouped snd
  return orderStatusTable

printStats :: Database -> Subspace -> Word8 -> IO ()
printStats db ss numPartitions = catches (do
  tcs <- map (\t -> (t :: Topic) {numPartitions = numPartitions})
         <$> listExistingTopics db ss
  ts  <- forConcurrently tcs $ \tc -> do
    beforeT <- getTime Monotonic
    before <- runTransaction db $ withSnapshot $ getTopicCount tc
    threadDelay 1000000
    after <- runTransaction db $ withSnapshot $ getTopicCount tc
    afterT <- getTime Monotonic
    let diffSecs = (fromIntegral $ toNanoSecs $ diffTimeSpec afterT beforeT)
                   / 10**9
    return ( topicName tc
           , ((fromIntegral after - fromIntegral before) / diffSecs)
           , after
           )
  forM_ (sortOn (\(x,_,_) -> x) ts)
    $ \(tn, c, after) ->
      printf "%s: %.1f msgs/sec and %d msgs total\n" (show tn) (c :: Double) after)
  [ Handler (\(e :: SomeException) ->
                putStrLn $ "Caught " ++ show e ++ " while getting stats!")]

printWatermarkLag :: Database -> WatermarkSS -> WatermarkSS -> IO ()
printWatermarkLag db root leaf = do
  res <- runTransaction' db
           $ withSnapshot $ do r <- getCurrentWatermark root >>= await
                               l <- getCurrentWatermark leaf >>= await
                               return (r,l)
  case res of
    Left err -> putStrLn $ "Caught " ++ show err ++ " while getting watermark stats"
    Right (Just r, Just l) -> do
      printf "Watermark of root is %s\n" $ show r
      printf "Watermark of leaf is %s\n" $ show l
      printf "Watermark lag is %s seconds\n" $ show @ Int $ round $ diffUTCTime (watermarkUTCTime r) (watermarkUTCTime l)
    _ -> return ()

mainLoop :: Database -> Subspace -> Args Identity -> IO ()
mainLoop db ss Args{ generatorNumThreads
                   , generatorMsgsPerSecond
                   , generatorBatchSize
                   , generatorWatchResults
                   , streamRun
                   , printTopicStats
                   , batchSize
                   , numLeaseThreads
                   , watermark
                   , numPartitions
                   , bytesPerChunk } = do
  metricsStore <- Metrics.newStore
  latencyDist <- Metrics.createDistribution "end_to_end_latency" metricsStore
  awaitedOrders <- Metrics.createGauge "waitingOrders" metricsStore
  _ <- forkStatsd defaultStatsdOptions metricsStore
  let conf = JobConfig
             { jobConfigDB = db
             , jobConfigSS = ss
             , streamMetricsStore = Just metricsStore
             , msgsPerBatch = coerce batchSize
             , leaseDuration = 10
             , numStreamThreads = coerce numLeaseThreads
             , numPeriodicJobThreads = 1
             , defaultNumPartitions = coerce numPartitions
             , logLevel = LogDebug
             , defaultChunkSizeBytes = runIdentity $ fmap fromIntegral bytesPerChunk
             }
  let pconf = Push.PushStreamConfig
                ( if coerce watermark
                    then Just $ \_ -> liftIO getCurrentTime >>= return . Watermark
                    else Nothing)
                (Just $ fromIntegral @Word8 $ coerce numPartitions)
  let bwconf = BW.BatchWriterConfig
               { BW.desiredMaxLatencyMillis = 2000
               , BW.maxBatchBytes = 10000
               , BW.maxQueueSize = 3000
               , BW.maxBatchSize = (fromIntegral @Int $ coerce generatorBatchSize)
               , BW.minIdempotencyMemoryDurationSeconds = 600
               , BW.idempotencyCleanupPeriod = 100
               }
  (input, bws) <- Push.runPushStream conf "incoming_orders" pconf bwconf (fromIntegral @Int $ coerce generatorNumThreads)
  let table = getAggrTable conf "order_table" (coerce numPartitions)
  stats <- newTVarIO $ LatencyStats 0 1
  -- TODO: use async for all of the threads below.
  forM_ bws $ \bw ->
    forkIO $ orderGeneratorLoop db
                                bw
                                table
                                (coerce generatorMsgsPerSecond)
                                (coerce generatorBatchSize)
                                stats
                                latencyDist
                                awaitedOrders
                                (coerce generatorWatchResults)
  void $ forkIO $ latencyReportLoop stats
  _ <- forkIO $ forever $ do
    when (coerce printTopicStats) $ printStats db ss (coerce numPartitions)
    threadDelay 1000000
  _ <- forkIO $ forever $ do
    printWatermarkLag db (topicWatermarkSS $ fromJust $ streamTopic input) (AT.aggrTableWatermarkSS table)
    threadDelay 1000000
  if coerce streamRun
     then runJob conf (topology input)
     else threadDelay maxBound

cleanup :: Database -> Subspace -> IO ()
cleanup db ss = catches (do
  putStrLn "Cleaning up FDB state"
  let (delBegin, delEnd) = rangeKeys $ subspaceRange ss
  runTransactionWithConfig defaultConfig {timeout = 5000} db $ clearRange delBegin delEnd
  putStrLn "Cleanup successful")
  [Handler $ \case
     (CError err) -> putStrLn $ "Caught " ++ show err ++ "while clearing subspace"
     (Error err) -> putStrLn $ "Caught " ++ show err ++ "while clearing subspace"]

wordCount :: MonadStream m => Stream Text -> m (AT.AggrTable Text (Sum Int))
wordCount txts = aggregate "counts" (groupBy Text.words txts) (const (Sum 1))

data Args f = Args
  { subspaceName :: f ByteString
  , generatorNumThreads :: f Int
  , generatorMsgsPerSecond :: f Int
  , generatorBatchSize :: f Int
  , generatorWatchResults :: f Bool
  , streamRun :: f Bool
  , cleanupFirst :: f Bool
  , printTopicStats :: f Bool
  , batchSize :: f Word16
  , numLeaseThreads :: f Int
  , watermark :: f Bool
  , numPartitions :: f Word8
  , bytesPerChunk :: f Word16
  }
  deriving (Generic)

deriving instance (forall a . Show a => Show (f a)) => Show (Args f)

deriving instance ( forall a . ParseField a => ParseFields (f a))
                  => ParseRecord (Args f)

applyDefaults :: Args Maybe -> Args Identity
applyDefaults Args{..} = Args
  { subspaceName = dflt "streamTest" subspaceName
  , generatorNumThreads = dflt 3 generatorNumThreads
  , generatorMsgsPerSecond = dflt 1000 generatorMsgsPerSecond
  , generatorBatchSize = dflt 200 generatorBatchSize
  , generatorWatchResults = dflt True generatorWatchResults
  , streamRun = dflt True streamRun
  , cleanupFirst = dflt True cleanupFirst
  , printTopicStats = dflt True printTopicStats
  , batchSize = dflt 50 batchSize
  , numLeaseThreads = dflt 12 numLeaseThreads
  , watermark = dflt False watermark
  , numPartitions = dflt 2 numPartitions
  , bytesPerChunk = dflt 4096 bytesPerChunk
  }

  where dflt d x = Identity $ fromMaybe d x

options :: FDB.FoundationDBOptions
options = defaultOptions {
  networkOptions = [ NetOp.traceEnable "/home/connor/Dropbox/haskell/fdb-streaming"
                   , NetOp.traceRollSize 0
                   , NetOp.traceFormat "json"
                   ]
}

main :: IO ()
main = withFoundationDB defaultOptions $ \db -> do
  args@Args {subspaceName, cleanupFirst} <- applyDefaults <$> getRecord "stream test"
  let ss = FDB.subspace [FDB.Bytes (runIdentity subspaceName)]
  --TODO: cleanup can time out in some circumstances, which crashes the program
  --since there's no exception handler on this call.
  when (runIdentity cleanupFirst) $ cleanup db ss
  mainLoop db ss args
