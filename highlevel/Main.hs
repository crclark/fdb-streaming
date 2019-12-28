
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

module Main where

import           FDBStreaming
import qualified FDBStreaming.AggrTable as AT
import           FDBStreaming.Message
import           FDBStreaming.Topic
import           FDBStreaming.Watermark (Watermark(Watermark, watermarkUTCTime), WatermarkSS, getCurrentWatermark, setWatermark)

import           Control.Monad
import           Control.Monad.IO.Class (liftIO)
import           Control.Concurrent
import           Control.Concurrent.Async       ( forConcurrently )
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
import           Data.List                      ( sortOn )
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Random.MWC as BS
import           Data.Coerce                    ( coerce )
import           Data.Time.Clock                ( diffUTCTime, getCurrentTime )
import           Data.Word                      ( Word16, Word8 )
import           FoundationDB                  as FDB
import           FoundationDB.Error
import           FoundationDB.Layer.Subspace   as FDB
import           FoundationDB.Layer.Tuple      as FDB
import           Data.UnixTime                  ( UnixTime
                                                , UnixDiffTime(..)
                                                , getUnixTime
                                                , diffUnixTime
                                                )
import           Data.UUID                      ( UUID )
import           Data.UUID.V4                  as UUID
                                                ( nextRandom )
import           GHC.Generics                   ( Generic )
import           System.Random                  ( randomIO, randomRIO )
import           Data.Maybe                     ( fromMaybe )
import           Data.Store                     ( Store )
import qualified Data.Store                    as Store
import           Data.Functor.Identity (Identity(..))
import           Foreign.C.Types (CTime(..))
import           Data.Monoid (All (..))
import           System.Clock (Clock(Monotonic), getTime, toNanoSecs, diffTimeSpec)
import qualified System.Metrics as Metrics
import           System.Metrics.Distribution (Distribution)
import qualified System.Metrics.Distribution as Distribution
import           System.Metrics.Gauge (Gauge)
import qualified System.Metrics.Gauge as Gauge
import           System.Random (randomRIO)
import           System.Remote.Monitoring (forkServer, serverMetricStore)
import System.Remote.Monitoring.Statsd (defaultStatsdOptions, forkStatsd)
import           Text.Printf                    ( printf )
import Options.Generic

newtype Timestamp = Timestamp { unTimestamp :: UnixTime }
  deriving (Show, Eq, Ord, Generic)
  deriving Store via (Identity UnixTime)

newtype OrderID = OrderID { unOrderID :: UUID }
  deriving (Show, Eq, Ord, Generic)
  deriving Store via (Identity UUID)
  deriving AT.TableKey via UUID

instance Message OrderID where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

data Order = Order
  { placedAt :: Timestamp
  , orderID :: OrderID
  , isFraud :: Bool
  , isInStock :: Bool
  , orderInstructions :: Text
  } deriving (Show, Eq, Generic, Store)

instance Message Order where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

randOrder :: IO Order
randOrder = do
  placedAt  <- Timestamp <$> getUnixTime
  orderID   <- OrderID <$> UUID.nextRandom
  isFraud   <- randomIO
  isInStock <- randomIO
  let orderInstructions = "This is a bunch of bytes containing text, to bulk up the total message size. Hwæt! Wé Gárdena      in géardagum þéodcyninga      þrym gefrúnon Oft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatumOft Scyld Scéfing      sceaþena þréatum"
  return Order { .. }

data FraudResult = FraudResult
  { orderID :: OrderID
  , isFraud :: Bool
  } deriving (Show, Eq, Generic, Store)

instance Message FraudResult where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

-- TODO: use getField
fraudOrderID :: FraudResult -> OrderID
fraudOrderID FraudResult{orderID} = orderID

-- | Super-sophisticated fraud detection! Asks the order if it is fraudulent.
isFraudulent :: Order -> FraudResult
isFraudulent Order{..} = FraudResult{..}

data InStockResult = InStockResult
  { orderID ::  OrderID
  , isInStock :: Bool
  } deriving (Show, Eq, Generic, Store)

instance Message InStockResult where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

invOrderID :: InStockResult -> OrderID
invOrderID InStockResult{orderID} = orderID

inventoryCheck :: Order -> InStockResult
inventoryCheck Order { .. } = InStockResult { .. }

data OrderDetails = OrderDetails
  { orderID :: OrderID
  , details :: ByteString
  } deriving (Show, Eq, Ord, Generic, Store)

instance Message OrderDetails where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

randOrderDetails :: Order -> IO OrderDetails
randOrderDetails Order { .. } = do
  details <- BS.random 500
  return OrderDetails { .. }

goodDetails :: ByteString -> Bool
goodDetails bs = odd $ sum $ BS.unpack bs

data LatencyStats = LatencyStats
  { timeElapsed :: !UnixDiffTime
  , numFinished :: !Int
  } deriving (Show, Eq, Ord)

instance Semigroup LatencyStats where
  (LatencyStats x1 y1) <> (LatencyStats x2 y2) =
    LatencyStats (x1 + x2) (y1 + y2)

unixDiffTimeToMicroseconds :: UnixDiffTime -> Int
unixDiffTimeToMicroseconds (UnixDiffTime (CTime secs) usecs) =
  1000000 * (fromIntegral secs) + (fromIntegral usecs)

unixDiffTimeToMilliseconds :: UnixDiffTime -> Double
unixDiffTimeToMilliseconds (UnixDiffTime (CTime secs) usecs) =
  1000.0 * (fromIntegral secs) + (fromIntegral usecs / 1000.0)

withProbability :: Int -> IO () -> IO ()
withProbability p f = do
  r <- randomRIO (1,100)
  when (r <= p) f

awaitOrder
  :: TopicConfig
  -> AT.AggrTable OrderID All
  -> TVar LatencyStats
  -> Distribution
  -> Gauge
  -> Order
  -> IO ()
awaitOrder orderTopic table stats latencyDist awaitGauge order@Order{orderID} = withProbability 1 $ catches (do
  Gauge.inc awaitGauge
  _ <- AT.getBlocking (topicConfigDB orderTopic) table orderID
  endTime <- getUnixTime
  let diff = diffUnixTime endTime (unTimestamp $ placedAt order)
  let statsDiff = LatencyStats diff 1
  atomically $ modifyTVar' stats (<> statsDiff)
  let diffMillis = unixDiffTimeToMilliseconds diff
  Distribution.add latencyDist diffMillis
  Gauge.dec awaitGauge
  )
  [ Handler (\(e :: Error) ->
              putStrLn $ "Caught " ++ show e ++ " while awaiting table data")
  , Handler (\(e :: SomeException) ->
                putStrLn $ "Caught " ++ show e ++ " while awaiting table data!")
  ]

-- | Creates a random order, pushes it onto the given topic, awaits its
-- arrival in the given AggrTable, and updates the given latency statistics
-- TVar.
placeAndAwaitOrders :: TopicConfig
                    -> AT.AggrTable OrderID All
                    -> TVar LatencyStats
                    -> Distribution
                    -> Gauge
                    -> Int
                    -- ^ batch size
                    -> Bool
                    -- ^ whether to watch orders to measure end_to_end_latency
                    -> IO ()
placeAndAwaitOrders orderTopic table stats latencyDist awaitGauge batchSize shouldWatch = catches ( do
  orders <- replicateM batchSize randOrder
  writeTopicNoRetry orderTopic (map toMessage orders)
  when shouldWatch
    $ forM_ orders $ awaitOrder orderTopic table stats latencyDist awaitGauge
  )
  [ Handler (\case
               Error (MaxRetriesExceeded (CError NotCommitted)) ->
                 putStrLn "Caught NotCommitted when writing to topic!"
               e -> putStrLn $ "caught fdb error " ++ show e ++ " while writing to topic")
  , Handler (\(e :: SomeException) ->
                putStrLn $ "Caught " ++ show e ++ " while writing to topic!")
  ]

writeTopicNoRetry :: Traversable t => TopicConfig -> t ByteString -> IO ()
writeTopicNoRetry tc@TopicConfig {..} bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  p <- randPartition tc
  FDB.runTransactionWithConfig
    defaultConfig { maxRetries = 0 }
    topicConfigDB
    $ do currTime <- liftIO getCurrentTime
         setWatermark (topicWatermarkSS tc) (Watermark currTime)
         writeTopic' tc p bss

-- TODO: far too many params!
orderGeneratorLoop :: TopicConfig
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
orderGeneratorLoop topic table rps batchSize stats latencyDist awaitGauge shouldWatch = do
  let delay = 1000000 `div` (rps `div` batchSize)
  void $ forkIO $ placeAndAwaitOrders topic table stats latencyDist awaitGauge batchSize shouldWatch
  threadDelay delay
  orderGeneratorLoop topic table rps batchSize stats latencyDist awaitGauge shouldWatch

latencyReportLoop :: TVar LatencyStats -> IO ()
latencyReportLoop stats = do
  LatencyStats{..} <- readTVarIO stats
  let (UnixDiffTime (CTime secs) usecs) = timeElapsed
  let totalMicroseconds = 1000000 * (fromIntegral secs) + (fromIntegral usecs) :: Integer
  let avgMilliseconds = (totalMicroseconds `div` fromIntegral numFinished) `div` 1000
  putStrLn $ "Processed "
             ++ show numFinished
             ++ " orders with average latency of "
             ++ show avgMilliseconds
             ++ " milliseconds"
  threadDelay 1000000
  latencyReportLoop stats

instance Message Bool where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

instance Message All where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

topology :: (MonadStream m) => TopicConfig -> Bool -> m (AT.AggrTable OrderID All)
topology incoming watermark = do
  input <- (\s -> s {isWatermarked = watermark}) <$> existing incoming
  let fraudChecks = fmap isFraudulent input
  let invChecks = fmap inventoryCheck input
  let details = benignIO (fmap Just . randOrderDetails) input
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
                            details
                            fst
                            (\OrderDetails{orderID} -> orderID)
                            (\(oid, isGood) OrderDetails{details}
                              -> (oid, All $ isGood && goodDetails details))
  grouped <- groupBy (pure . fst) finalJoin
  orderStatusTable <- aggregate "order_table" grouped snd
  return orderStatusTable

printStats :: Database -> Subspace -> IO ()
printStats db ss = catches (do
  tcs <- listExistingTopics db ss
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
  (r,l) <- runTransaction db $ do r <- getCurrentWatermark root >>= await
                                  l <- getCurrentWatermark leaf >>= await
                                  return (r,l)
  printf "Watermark of root is %s\n" $ show r
  printf "Watermark of leaf is %s\n" $ show l
  printf "Watermark lag is %s\n" $ show (fmap round (diffUTCTime <$> (fmap watermarkUTCTime r) <*> (fmap watermarkUTCTime l)))

mainLoop :: Database -> Subspace -> Args Identity -> IO ()
mainLoop db ss Args{ generatorNumThreads
                   , generatorMsgsPerSecond
                   , generatorBatchSize
                   , generatorWatchResults
                   , streamRun
                   , printTopicStats
                   , batchSize
                   , numLeaseThreads
                   , watermark } = do
  metricsStore <- Metrics.newStore
  latencyDist <- Metrics.createDistribution "end_to_end_latency" metricsStore
  awaitedOrders <- Metrics.createGauge "waitingOrders" metricsStore
  forkStatsd defaultStatsdOptions metricsStore
  let conf = FDBStreamConfig { streamConfigDB = db
                             , streamConfigSS = ss
                             , streamMetricsStore = Just metricsStore
                             , msgsPerBatch = coerce batchSize
                             , leaseDuration = 10
                             , numStreamThreads = coerce numLeaseThreads
                             , numPeriodicJobThreads = 1
                             }
  let input = makeTopicConfig db ss "incoming_orders"
  let table = getAggrTable conf "order_table"
  stats <- newTVarIO $ LatencyStats 0 1
  replicateM_ (coerce generatorNumThreads)
    $ forkIO $ orderGeneratorLoop input
                                  table
                                  (coerce generatorMsgsPerSecond)
                                  (coerce generatorBatchSize)
                                  stats
                                  latencyDist
                                  awaitedOrders
                                  (coerce generatorWatchResults)
  void $ forkIO $ latencyReportLoop stats
  forkIO $ forever $ do
    when (coerce printTopicStats) $ printStats db ss
    threadDelay 1000000
  forkIO $ forever $ do
    printWatermarkLag db (topicWatermarkSS input) (AT.aggrTableWatermarkSS table)
    threadDelay 1000000
  when (coerce streamRun)
    $ runStream conf (topology input (coerce watermark))


cleanup :: Database -> Subspace -> IO ()
cleanup db ss = do
  putStrLn "Cleaning up FDB state"
  let (delBegin, delEnd) = rangeKeys $ subspaceRange ss
  runTransactionWithConfig defaultConfig {timeout = 5000} db $ clearRange delBegin delEnd
  putStrLn "Cleanup successful"

data Args f = Args
  { subspaceName :: f ByteString
  , generatorNumThreads :: f Int
  , generatorMsgsPerSecond :: f Int
  , generatorBatchSize :: f Int
  , generatorWatchResults :: f Bool
  , streamRun :: f Bool
  , cleanupFirst :: f Bool
  , printTopicStats :: f Bool
  , batchSize :: f Word8
  , numLeaseThreads :: f Int
  , watermark :: f Bool
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
  }

  where dflt d x = Identity $ fromMaybe d x

main :: IO ()
main = withFoundationDB defaultOptions $ \db -> do
  args@Args {subspaceName, cleanupFirst} <- applyDefaults <$> getRecord "stream test"
  let ss = FDB.subspace [FDB.Bytes (runIdentity subspaceName)]
  --TODO: cleanup can time out in some circumstances, which crashes the program
  --since there's no exception handler on this call.
  when (runIdentity cleanupFirst) $ cleanup db ss
  mainLoop db ss args
