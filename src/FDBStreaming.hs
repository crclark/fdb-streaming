{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE NamedFieldPuns #-}

module FDBStreaming where

import qualified FDBStreaming.AggrTable        as AT
import           FDBStreaming.Message           ( Message(..) )
import           FDBStreaming.Topic

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import qualified Control.Monad.State           as State
import           Data.ByteString                ( ByteString )
import           Data.Foldable                  ( toList )

import           Foreign.C.Types             (CTime(..))
import           Data.Map                    (Map)
import qualified Data.Map                    as Map
import           Data.Maybe                     ( catMaybes )
import           Data.Sequence                  ( Seq(..) )
import qualified Data.Sequence                 as Seq
import qualified Data.Set                      as Set
import           Data.UnixTime                  ( UnixTime
                                                , UnixDiffTime(..)
                                                , getUnixTime
                                                , diffUnixTime
                                                )
import  Data.Text.Encoding           (decodeUtf8)
import           Data.Void                      ( Void )
import           FoundationDB                  as FDB
import           FoundationDB.Layer.Subspace   as FDB
import           FoundationDB.Layer.Tuple      as FDB
import           Data.Word                      ( Word8 )
import qualified System.Metrics                as Metrics
import System.Metrics.Counter (Counter)
import System.Metrics.Distribution (Distribution)
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Distribution as Distribution

data StreamEdgeMetrics = StreamEdgeMetrics
  { messagesProcessed :: Counter
  , emptyReads :: Counter
  , batchLatency :: Distribution
  , messagesPerBatch :: Distribution
  }

type MetricsMap = Map StreamName StreamEdgeMetrics

registerTopologyMetrics :: Stream a
                        -> Metrics.Store
                        -> IO (Map StreamName StreamEdgeMetrics)
registerTopologyMetrics topo store =
  foldMStream topo $ \s -> do
    let sn = decodeUtf8 $ streamName s
    mp <- Metrics.createCounter ("stream." <> sn <> ".messagesProcessed") store
    er <- Metrics.createCounter ("stream." <> sn <> ".emptyReads") store
    bl <- Metrics.createDistribution ("stream." <> sn <> ".batchLatency") store
    mb <- Metrics.createDistribution ("stream." <> sn <> ".msgsPerBatch") store
    return (Map.singleton (streamName s) (StreamEdgeMetrics mp er bl mb))

incrEmptyBatchCount :: StreamName -> Maybe MetricsMap -> IO ()
incrEmptyBatchCount sn Nothing = return ()
incrEmptyBatchCount sn (Just m) =
  Counter.inc (emptyReads $ m Map.! sn)

recordMsgsPerBatch :: StreamName -> Maybe MetricsMap -> Int -> IO ()
recordMsgsPerBatch _ Nothing _ = return ()
recordMsgsPerBatch sn (Just m) n =
  Distribution.add (messagesPerBatch $ m Map.! sn) (fromIntegral n)

{-

High-level streaming combinators -- an experiment.

The streams need to be easy to maintain. How to change a topology over time is
an important consideration. What happens if I put a new step in the middle of
a pipeline? What happens if I remove a step? What happens if I change the logic
of a step?

After all of these modifications, what happens when I start my program again
with the existing DB? What happens if I do a rolling deployment, so some are
still running the old version of the code?

-}

type StreamName = ByteString

data GroupedBy k v

-- TODO: need to think about grouping and windowing. Windowing appears to be a
-- special case of grouping, where the groupBy function is a function of time.
-- But is a sliding window a special case of groupBy? Seems like it might not be
-- since consecutive messages can be assigned to multiple groups.
-- Come to think of it, does groupBy need to assign each item to only one group?
-- Could it ever make sense to assign a single object to a set of groups?
-- What if the set of groups is rather large (such as all sliding windows the
-- object falls within)?

-- It looks like kafka streams only allows sliding windows for joins, which
-- would certainly simplify matters.

-- For groupBy, I think we could introduce a type GroupedBy a b, representing
-- things of type b grouped by type a. Introduction would be by
-- groupBy :: (b -> a) -> Stream b -> Stream (GroupedBy a b)
-- elimination would be by
-- aggregateBy :: Monoid m => (b -> m) -> Stream (GroupedBy a b) -> Stream (a, m)
-- or something similar. Problem being that we can't produce an (a,m) until we
-- know we have seen everything with a particular a key. So perhaps it should be
-- persisted to a state m associated with each Stream -- Stream m a b?
-- Or perhaps don't parametrize, and instead require that the Stream state be a
-- k/v store (i.e. table), and give the user an interface into that?

-- Note: we don't need a separate type for nested groupby -- we can simply group
-- by a tuple. No need to support GroupedBy a (GroupedBy b c).

-- TODO: think about joins.
-- I believe we can build joins out of FDB by listening to both upstream topics,
-- writing a k/v when we receive one half of the join tuple, check if the tuple
-- has been completed (i.e., the half we just received is the last of the two
-- halves), and then emit a downstream message of the tuple.
-- tentative type:
-- joinOn :: (a -> e) -> (b -> e) -> Stream a -> Stream b -> Stream (a,b)
-- possible user errors/gotchas:
-- 1. non-unique join values
-- 2. must be the case for all x,y :: JoinValue that
--    (toMessage x == toMessage y) iff x == y. That is, they will be joined by
--    the equality of the serializations of the values, not the Haskell values
--    themselves.
-- stuff to deal with on our side:
-- 1. How do we garbage collect halves that never get paired up?
--    The intermediate data must necessarily be keyed by the projected value,
--    which is itself not ordered by anything useful.

-- TODO: consider generalizing IO to m in future
-- TODO: what about truncating old data by timestamp?
-- TODO: don't recurse infinitely on cyclical topologies.
-- TODO: If we can make a constant stream that doesn't actually hit the DB, we
-- would be able to implement 'pure' for this type.
data Stream b where
  StreamExistingTopic :: Message a
                      => StreamName
                      -> TopicConfig
                      -> Stream a
  StreamProducer :: Message a
                 => StreamName
                 -- Maybe allows for filtering
                 -> IO (Maybe a)
                 -> Stream a
  StreamConsumer :: (Message a)
                 => StreamName
                 -> Stream a
                 -- TODO: if this handler type took a batch at a time,
                 -- it would be easier to optimize -- imagine if it were to
                 -- to do a get from FDB for each item -- it could do them all
                 -- in parallel.
                 -> (a -> IO ())
                 -> Stream Void
  -- TODO: looks suspiciously similar to monadic bind
  StreamPipe :: (Message a, Message b)
             => StreamName
             -> Stream a
             -> (a -> IO (Maybe b))
             -> Stream b
  -- | Streaming one-to-one join. If the relationship is not actually one-to-one
  --   (i.e. the input join functions are not injective), some messages in the
  --   input streams could be lost.
  Stream1to1Join :: (Message a, Message b, Message c, Message d)
                 => StreamName
                 -> Stream a
                 -> Stream b
                 -> (a -> c)
                 -> (b -> c)
                 -> (a -> b -> d)
                 -> Stream d
  -- NOTE: the reason that this is a separate constructor from StreamAggregate
  -- is so that our helper functions can be combined more easily. It's easier to
  -- work with and refactor code that looks like @count . groupBy id@ rather
  -- than the less compositional @countBy id@. At least, that's what it looks
  -- like at the time of this writing. Kafka Streams does it that way. If it
  -- ends up not being worth it, simplify.
  -- TODO: problem: this has an unused StreamName which is meaningless.
  StreamGroupBy :: (Message a, Message k)
                => StreamName
                -> Stream a
                -> (a -> k)
                -> Stream (GroupedBy k a)
  -- TODO: maybe consolidate TableValue and TableSemigroup
  -- TODO: if we're exporting helpers anyway, maybe no need for classes
  -- at all.
  StreamAggregate :: (Message a, Message k, AT.TableValue v, AT.TableSemigroup v)
                  => StreamName
                  -> Stream (GroupedBy k a)
                  -> (a -> v)
                  -> Stream (AT.AggrTable k v)

-- | reads a batch of messages from a stream and checkpoints so that the same
-- value of 'ReaderName' is guaranteed to never receive the same messages again
-- in subsequent calls to this function.
readPartitionBatchExactlyOnce
  :: Message a
  => FDBStreamConfig
  -> Maybe MetricsMap
  -> ReaderName
  -> Stream a
  -> PartitionId
  -> Word8
  -> Transaction (Seq a)
readPartitionBatchExactlyOnce cfg metrics rn s pid n = case outputTopic cfg s of
  Nothing     -> return Empty
  Just outCfg -> do
    rawMsgs <- readNAndCheckpoint' outCfg pid rn n
    liftIO $ when (Seq.null rawMsgs) (incrEmptyBatchCount rn metrics)
    liftIO $ recordMsgsPerBatch rn metrics (Seq.length rawMsgs)
    return $ fmap (fromMessage . snd) rawMsgs

readBatchExactlyOnce
  :: Message a
  => FDBStreamConfig
  -> Maybe MetricsMap
  -> ReaderName
  -> Stream a
  -> Word8
  -> Transaction (Seq a)
readBatchExactlyOnce cfg metrics rn s n = case outputTopic cfg s of
  Nothing -> return Empty
  Just outCfg -> do
    pid <- liftIO $ randPartition outCfg
    readPartitionBatchExactlyOnce cfg metrics rn s pid n

getAggrTable :: FDBStreamConfig -> Stream (AT.AggrTable k v) -> AT.AggrTable k v
getAggrTable sc (StreamAggregate sn _ _) = AT.AggrTable
  $ extend (streamConfigSS sc) [Bytes "tpcs", Bytes sn, Bytes "AggrTable"]
-- TODO: find a way to avoid this. type family, I suppose
getAggrTable _ _ = error "Please don't make AggrTable an instance of Message"

streamName :: Stream a -> StreamName
streamName (StreamExistingTopic sn _   ) = sn
streamName (StreamProducer      sn _   ) = sn
streamName (StreamConsumer sn _ _      ) = sn
streamName (StreamPipe     sn _ _      ) = sn
streamName (Stream1to1Join sn _ _ _ _ _) = sn
streamName (StreamGroupBy   sn _ _     ) = sn
streamName (StreamAggregate sn _ _     ) = sn

foldMStream :: forall a m. Monoid m => Stream a -> (forall b . Stream b -> IO m) -> IO m
foldMStream s f = State.evalStateT (go s) mempty
  where
   go :: forall c . Stream c -> State.StateT (Set.Set StreamName) IO m
   go st = do
     visited <- State.get
     if Set.member (streamName st) visited
       then return mempty
       else do
         State.put (Set.insert (streamName st) visited)
         process st

   process :: forall d . Stream d -> State.StateT (Set.Set StreamName) IO m
   process st@(StreamExistingTopic _ _   ) = liftIO (f st)
   process st@(StreamProducer      _ _   ) = liftIO (f st)
   process st@(StreamConsumer _ up _     ) = mappend <$> liftIO (f st) <*> go up
   process st@(StreamPipe     _ up _     ) = mappend <$> liftIO (f st) <*> go up
   process st@(Stream1to1Join _ x y _ _ _) = do
    xs <- go x
    ys <- go y
    z <- liftIO (f st)
    return (z <> xs <> ys)
   process st@(StreamGroupBy   _ up _    ) = mappend <$> liftIO (f st) <*> go up
   process st@(StreamAggregate _ up _    ) = mappend <$> liftIO (f st) <*> go up

-- TODO: kind of surprising that this has to be run on every 'StreamConsumer' in
-- the topology, and then shared upstream elements will get traversed repeatedly
-- regardless. Maybe we should just use a graph library instead.
-- | do something to each processor in a stream. Each stream is traversed once,
-- even in the case of loops.
traverseStream :: Stream b -> (forall a . Stream a -> IO ()) -> IO ()
traverseStream s a = State.evalStateT (go s) mempty
 where
  go :: forall c . Stream c -> State.StateT (Set.Set StreamName) IO ()
  go st = do
    visited <- State.get
    if Set.member (streamName st) visited
      then return ()
      else do
        State.put (Set.insert (streamName st) visited)
        process st

  process :: forall d . Stream d -> State.StateT (Set.Set StreamName) IO ()
  process st@(StreamExistingTopic _ _   ) = liftIO (a st)
  process st@(StreamProducer      _ _   ) = liftIO (a st)
  process st@(StreamConsumer _ up _     ) = liftIO (a st) >> go up
  process st@(StreamPipe     _ up _     ) = liftIO (a st) >> go up
  process st@(Stream1to1Join _ x y _ _ _) = liftIO (a st) >> go x >> go y
  process st@(StreamGroupBy   _ up _    ) = liftIO (a st) >> go up
  process st@(StreamAggregate _ up _    ) = liftIO (a st) >> go up

debugTraverseStream :: Stream a -> IO ()
debugTraverseStream s = traverseStream s $ \step -> print (streamName step)

-- TODO: move join stuff to new namespace
subspace1to1JoinForKey
  :: Message k => FDBStreamConfig -> StreamName -> k -> Subspace
subspace1to1JoinForKey sc sn k = extend
  (streamConfigSS sc)
         -- TODO: replace these Bytes constants with named small int constants
  [Bytes "tpcs", Bytes sn, Bytes "1:1join", Bytes (toMessage k)]

get1to1JoinData
  :: (Message a, Message b, Message k)
  => FDBStreamConfig
  -> StreamName
  -> k
  -> Transaction (Future (Maybe (Either a b)))
                -- ^ Returns whichever of the two join types was processed first
                -- for the given key, or Nothing if neither has yet been
                -- seen.
get1to1JoinData c sn k = do
  xs <- getRange' (subspaceRange ss) FDB.StreamingModeWantAll
  return $ flip fmap xs $ \case
    RangeDone kvs   -> parse kvs
    RangeMore kvs _ -> parse kvs
 where
  parse Empty = Nothing
  parse ((unpack ss -> (Right [Bool True, Bytes x]), _) :<| Empty) =
    Just $ Left (fromMessage x)
  parse ((unpack ss -> (Right [Bool False, Bytes x]), _) :<| Empty) =
    Just $ Right (fromMessage x)
  parse ((unpack ss -> Left err, _) :<| Empty) =
    error $ "Deserialization error in 1-to-1 join table: " ++ show err
  parse (_       :<| Empty) = error "1-to-1 join tuple in wrong format"
  parse (_ :<| _ :<| _    ) = error "consistency violation in 1-to-1 join table"

  ss = subspace1to1JoinForKey c sn k

-- | get join data for n keys concurrently
get1to1JoinDataBatch
  :: (Message a, Message b, Message k, Traversable t)
  => FDBStreamConfig
  -> StreamName
  -> t k
  -> Transaction (t (Maybe (Either a b)))
get1to1JoinDataBatch c rn ks = do
  joinFutures <- forM ks (get1to1JoinData c rn)
  -- Now that we have sent all the gets, await them
  forM joinFutures await

delete1to1JoinData
  :: Message k => FDBStreamConfig -> StreamName -> k -> Transaction ()
delete1to1JoinData c sn k = do
  let ss     = subspace1to1JoinForKey c sn k
  let (x, y) = rangeKeys $ subspaceRange ss
  clearRange x y

write1to1JoinData
  :: (Message k, Message a, Message b)
  => FDBStreamConfig
  -> StreamName
  -> k
  -> Either a b
  -> Transaction ()
write1to1JoinData c sn k x = do
  let ss = subspace1to1JoinForKey c sn k
  case x of
    Left  y -> set (pack ss [Bool True, Bytes (toMessage y)]) ""
    Right y -> set (pack ss [Bool False, Bytes (toMessage y)]) ""

-- TODO: other persistence backends
-- TODO: should probably rename to TopologyConfig
data FDBStreamConfig = FDBStreamConfig
  { streamConfigDB :: FDB.Database
  , streamConfigSS :: FDB.Subspace
  -- ^ subspace that will contain all state for the stream topology
  , streamMetricsStore :: Maybe Metrics.Store
  }

inputTopics :: FDBStreamConfig -> Stream a -> [TopicConfig]
inputTopics _  (StreamExistingTopic _ _) = []
inputTopics _  (StreamProducer      _ _) = []
inputTopics sc (StreamConsumer _ inp _ ) = catMaybes [outputTopic sc inp]
inputTopics sc (StreamPipe     _ inp _ ) = catMaybes [outputTopic sc inp]
inputTopics sc (Stream1to1Join _ l r _ _ _) =
  catMaybes [outputTopic sc l, outputTopic sc r]
inputTopics sc (StreamGroupBy   _ inp _) = catMaybes [outputTopic sc inp]
inputTopics sc (StreamAggregate _ inp _) = inputTopics sc inp

outputTopic :: FDBStreamConfig -> Stream a -> Maybe TopicConfig
-- TODO: StreamConsumer has no output. Should it not be included in this GADT?
-- Or perhaps not exist at all?
outputTopic _ (StreamExistingTopic _ tc) = Just tc
outputTopic _ StreamConsumer{}           = Nothing
outputTopic _ StreamGroupBy{}            = Nothing
outputTopic _ StreamAggregate{}          = Nothing
outputTopic sc s =
  Just $ makeTopicConfig (streamConfigDB sc) (streamConfigSS sc) (streamName s)

unixDiffTimeToMilliseconds :: UnixDiffTime -> Double
unixDiffTimeToMilliseconds (UnixDiffTime (CTime secs) usecs) =
  1000.0 * (fromIntegral secs) + (fromIntegral usecs / 1000.0)

foreverLogErrors :: Maybe (Map StreamName StreamEdgeMetrics)
                 -> StreamName
                 -> IO ()
                 -> IO ()
foreverLogErrors metrics sn x = forever $ handle
  (\(e :: SomeException) -> putStrLn $ show sn ++ " thread caught " ++ show e)
  $ do
    -- Problem: busy looping, constantly reading for more data, is wasteful.
    -- Possible solution: create a watch after each iteration, and only loop
    -- again once the watch is delivered. Subsequent problem: we need to pass
    -- the partition id of the partition with new messages into the iteration
    -- body. However, if we do that, we could conceivably cause all the reader
    -- threads to synchronize, with all of them waking up on each write and
    -- contending the same partition, which breaks the reader scalability we
    -- created with the partitions.
    -- On the other hand, we can't watch and then read a random partition,
    -- because then we might read the wrong one and the message could be delayed
    -- in a low-write situation.
    -- On the third or fourth hand, perhaps the readers would only get synched
    -- in low-write situations where contention wouldn't matter, anyway -- if
    -- tons of messages are coming in, presumably threads will be spending more
    -- time working than waiting, and they won't be woken up together for
    -- exactly the same write.
    -- w <- x
    -- awaitTopicOrTimeout 500 w
    t1 <- getUnixTime
    x
    t2 <- getUnixTime
    let timeMillis = unixDiffTimeToMilliseconds $ diffUnixTime t2 t1
    -- TODO: helper function and probably ReaderT for recording metrics
    forM_ (metrics >>= Map.lookup sn) $ \StreamEdgeMetrics{..} -> do
      Distribution.add batchLatency timeMillis

runStream :: FDBStreamConfig -> Stream a -> IO ()
runStream cfg@FDBStreamConfig{streamMetricsStore} s = do
  metrics <- forM streamMetricsStore $ registerTopologyMetrics s
  traverseStream s $ \step -> do
    putStrLn $ "starting " ++ show (streamName step)
    void $ forkIO $ foreverLogErrors metrics (streamName step) $ runStreamStep cfg metrics step

-- | Runs a single stream step. Reads from its input topic
-- in chunks, runs the monadic action on each input, and writes the result to
-- its output topic.
-- TODO: if any of these processors reach a message they can't process because
-- of an exception, they will get stuck trying to process that message in a loop
-- forever. For example, what if we always generate a 30MiB message for one bad
-- message? We can't write a message that big.
-- Perhaps we could retry a few times, then put the message in a bad message
-- topic and skip past it. The problem is that if we skip a message, we must
-- be careful that all logic using the checkpoint knows that just because a
-- given message is checkpointed, its transformed output is not necessarily in
-- downstream topics.
runStreamStep :: FDBStreamConfig -> Maybe MetricsMap -> Stream a -> IO ()
runStreamStep _                    _ (  StreamExistingTopic _ _   ) = return ()
runStreamStep c@FDBStreamConfig {..} _ s@(StreamProducer      _ step) = do
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  let Just outCfg = outputTopic c s
  xs <- catMaybes <$> replicateM 10 step -- TODO: batch size config
  writeTopic outCfg (fmap toMessage xs)

runStreamStep c@FDBStreamConfig {..} metrics s@(StreamConsumer sn _ step) = do
  -- TODO: if parsing the message fails, should we still checkpoint?
  -- TODO: blockUntilNew
  let [inCfg] = inputTopics c s
  xs <- readNAndCheckpoint inCfg sn 10
  mapM_ step (fmap (fromMessage . snd) xs)

runStreamStep c@FDBStreamConfig {..} metrics s@(StreamPipe rn up step) = do
  -- TODO: blockUntilNew
  let Just outCfg = outputTopic c s
  runTransactionWithConfig infRetry streamConfigDB $ do
    inMsgs <- readBatchExactlyOnce c metrics rn up 10
    ys     <- catMaybes . toList <$> liftIO (mapM step inMsgs)
    let outMsgs = fmap toMessage ys
    p' <- liftIO $ randPartition outCfg
    writeTopic' outCfg p' outMsgs

runStreamStep c@FDBStreamConfig {..} metrics s@(Stream1to1Join rn (ls :: Stream a) (rs :: Stream
    b) pl pr combiner)
  = do
    let Just outCfg = outputTopic c s
    -- TODO: the below two transactions are virtually identical, except
    -- swapping the usages of Left and Right. Need to find a way to eliminate
    -- the repetition.
    -- What this does is read from one of the two join streams, and for each
    -- message read, compute the join key. Using the join key, look in the
    -- join table to see if the partner is already there. If so, write the tuple
    -- downstream. If not, write the one message we do have to the join table.
    -- TODO: think of a way to garbage collect items that never get joined.
    FDB.runTransactionWithConfig infRetry streamConfigDB $ do
      lMsgs    <- readBatchExactlyOnce c metrics rn ls 10
      -- TODO: move joinFutures, joinData into separate function
      joinData <- Seq.zip lMsgs <$> get1to1JoinDataBatch c rn (fmap pl lMsgs)
      toWrite  <- fmap (catMaybes . toList) $ forM joinData $ \(lmsg, d) -> do
        let k = pl lmsg
        case d of
          Just (Right (rmsg :: b)) -> do
            delete1to1JoinData c rn k
            return $ Just $ combiner lmsg rmsg
          Just (Left (_ :: a)) -> return Nothing -- TODO: warn (or error?) about non-one-to-one join
          Nothing              -> do
            write1to1JoinData c rn k (Left lmsg :: Either a b)
            return Nothing
      p' <- liftIO $ randPartition outCfg
      writeTopic' outCfg p' (map toMessage toWrite)
    FDB.runTransactionWithConfig infRetry streamConfigDB $ do
      rMsgs    <- readBatchExactlyOnce c metrics rn rs 10
      joinData <- Seq.zip rMsgs <$> get1to1JoinDataBatch c rn (fmap pr rMsgs)
      toWrite  <- fmap (catMaybes . toList) $ forM joinData $ \(rmsg, d) -> do
        let k = pr rmsg
        case d of
          Just (Left (lmsg :: a)) -> do
            delete1to1JoinData c rn k
            return $ Just $ combiner lmsg rmsg
          Just (Right (_ :: b)) -> do
            liftIO $ putStrLn "Got unexpected Right"
            return Nothing -- TODO: warn (or error?) about non-one-to-one join
          Nothing -> do
            write1to1JoinData c rn k (Right rmsg :: Either a b)
            return Nothing
      -- TODO: I think we could dry out the above by using a utility function to
      -- return the list of stuff to write and then call swap before writing.
      p' <- liftIO $ randPartition outCfg
      writeTopic' outCfg p' (map toMessage toWrite)

runStreamStep _ _ StreamGroupBy{} = return ()

runStreamStep c@FDBStreamConfig {..} metrics s@(StreamAggregate sn (StreamGroupBy _ up toKey) aggr)
  = do
    let table = getAggrTable c s
    FDB.runTransactionWithConfig infRetry streamConfigDB $ do
      msgs <- readBatchExactlyOnce c metrics sn up 10
      forM_ msgs $ \msg -> AT.mappendTable table (toKey msg) (aggr msg)

-- TODO: stronger types
runStreamStep _ _ StreamAggregate{} = error "tried to aggregate ungrouped stream"
