{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import           FDBStreaming
import qualified FDBStreaming.AggrTable as AT
import           FDBStreaming.Message
import           FDBStreaming.Topic

import           Control.Monad
import           Control.Concurrent
import           Control.Concurrent.Async       ( forConcurrently )
import           Control.Concurrent.STM         ( TVar
                                                , readTVarIO
                                                , atomically
                                                , modifyTVar'
                                                , newTVarIO
                                                )
import           Data.List                      ( sortOn )
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Random.MWC as BS
import           FoundationDB                  as FDB
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
import           System.Random                  ( randomIO )
import           Data.Store                     ( Store )
import qualified Data.Store                    as Store
import Data.Functor.Identity (Identity(..))
import Foreign.C.Types (CTime(..))
import Data.Monoid (All (..))
import qualified System.Metrics as Metrics
import           System.Metrics.Distribution (Distribution)
import qualified System.Metrics.Distribution as Distribution
import System.Remote.Monitoring (forkServer, serverMetricStore)


newtype Timestamp = Timestamp { unTimestamp :: UnixTime }
  deriving (Show, Eq, Ord, Generic)
  deriving Store via (Identity UnixTime)

newtype OrderID = OrderID { unOrderID :: UUID }
  deriving (Show, Eq, Ord, Generic)
  deriving Store via (Identity UUID)

instance Message OrderID where
  toMessage = Store.encode
  fromMessage = Store.decodeEx

data Order = Order
  { placedAt :: Timestamp
  , orderID :: OrderID
  , isFraud :: Bool
  , isInStock :: Bool
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

-- | Creates a random order, pushes it onto the given topic, awaits its
-- arrival in the given AggrTable, and updates the given latency statistics
-- TVar.
placeAndAwaitOrder :: TopicConfig
                   -> AT.AggrTable OrderID All
                   -> TVar LatencyStats
                   -> Distribution
                   -> IO ()
placeAndAwaitOrder orderTopic table stats latencyDist = do
  order@Order{orderID} <- randOrder
  writeTopic orderTopic [(toMessage order)]
  _ <- AT.getBlocking (topicConfigDB orderTopic) table orderID
  endTime <- getUnixTime
  let diff = diffUnixTime endTime (unTimestamp $ placedAt order)
  let statsDiff = LatencyStats diff 1
  atomically $ modifyTVar' stats (<> statsDiff)
  let diffMillis = unixDiffTimeToMilliseconds diff
  Distribution.add latencyDist diffMillis

orderGeneratorLoop :: TopicConfig
                   -> AT.AggrTable OrderID All
                   -> Int
                   -- ^ requests per second
                   -> TVar LatencyStats
                   -> Distribution
                   -> IO ()
orderGeneratorLoop topic table rps stats latencyDist = do
  let delay = 1000000 `div` rps
  void $ forkIO $ placeAndAwaitOrder topic table stats latencyDist
  threadDelay delay
  orderGeneratorLoop topic table rps stats latencyDist

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

topology :: TopicConfig -> Stream (AT.AggrTable OrderID All)
topology incoming = StreamAggregate "order_table" grouped snd
  where
    input = StreamExistingTopic "existing_orders" incoming
    fraudChecks = StreamPipe "fraud_checks" input $ \order ->
      return $ Just $ isFraudulent order
    invChecks = StreamPipe "inv_checks" input $ \order ->
      return $ Just $ inventoryCheck order
    detailsPipe = StreamPipe "details" input (fmap Just . randOrderDetails)
    fraudInvJoin = Stream1to1Join "f_i_join"
                                  fraudChecks
                                  invChecks
                                  fraudOrderID
                                  invOrderID
                                  (\FraudResult{orderID, isFraud}
                                    InStockResult{isInStock}
                                    -> (orderID, not isFraud && isInStock))
    finalJoin = Stream1to1Join "final_join"
                               fraudInvJoin
                               detailsPipe
                               fst
                               (\OrderDetails{orderID} -> orderID)
                               (\(oid, isGood) OrderDetails{details}
                                 -> (oid, All $ isGood && goodDetails details))
    grouped = StreamGroupBy "groupby" finalJoin fst


topSS :: Subspace
topSS = FDB.subspace [FDB.Bytes "cool_subspace"]

printStats :: Database -> Subspace -> IO ()
printStats db ss = do
  tcs <- listExistingTopics db ss
  ts  <- forConcurrently tcs $ \tc -> do
    before <- runTransaction db $ getTopicCount tc
    threadDelay 1000000
    after <- runTransaction db $ getTopicCount tc
    return (topicName tc, fromIntegral after - fromIntegral before)
  forM_ (sortOn fst ts)
    $ \(tn, c) -> putStrLn $ show tn ++ ": " ++ show (c :: Int) ++ " msgs/sec"

mainLoop :: Database -> IO ()
mainLoop db = do
  metricsStore <- serverMetricStore <$> forkServer "localhost" 8000
  latencyDist <- Metrics.createDistribution "end_to_end_latency" metricsStore
  let conf = FDBStreamConfig db topSS (Just metricsStore) 8
  let input = makeTopicConfig db topSS "incoming_orders"
  let t = topology input
  let table = getAggrTable conf t
  stats <- newTVarIO $ LatencyStats 0 1
  void $ forkIO $ orderGeneratorLoop input table 500 stats latencyDist
  void $ forkIO $ latencyReportLoop stats
  debugTraverseStream t
  runStream conf t
  forever $ do
    printStats db topSS
    threadDelay 1000000

main :: IO ()
main = withFoundationDB defaultOptions $ \db -> do
  putStrLn "Cleaning up FDB state"
  let (delBegin, delEnd) = rangeKeys $ subspaceRange topSS
  runTransaction db $ clearRange delBegin delEnd
  putStrLn "Cleanup successful"
  mainLoop db

