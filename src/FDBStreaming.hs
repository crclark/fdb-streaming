{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module FDBStreaming where

import FDBStreaming.Topic

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.Foldable (toList)
import Data.Maybe (catMaybes)
import Data.Sequence (Seq(..))
import Data.Void
import FoundationDB as FDB
import FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Tuple as FDB

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

-- TODO: error handling for bad parses
class Messageable a where
  toMessage :: a -> ByteString
  fromMessage :: ByteString -> a

-- TODO: argh, find a way to avoid this.
instance Messageable Void where
  toMessage = error "impossible happened: called toMessage on Void"
  fromMessage = error "impossible happened: called fromMessage on Void"

instance (Messageable a, Messageable b) => Messageable (a,b) where
  toMessage (x,y) = encodeTupleElems [Bytes (toMessage x), Bytes (toMessage y)]
  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes x, Bytes y] -> (fromMessage x, fromMessage y)
      Right xs -> error $ "unexpected decode " ++ show xs

type StreamName = ByteString

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
-- TODO: what about state and folds?
-- TODO: what about truncating old data by timestamp?
-- TODO: don't recurse infinitely on cyclical topologies.
-- TODO: If we can make a constant stream that doesn't actually hit the DB, we
-- would be able to implement 'pure' for this type.
data Stream b where
  StreamProducer :: Messageable a
                 => StreamName
                 -- Maybe allows for filtering
                 -> IO (Maybe a)
                 -> Stream a
  StreamConsumer :: (Messageable a)
                 => StreamName
                 -> Stream a
                 -- TODO: if this handler type took a batch at a time,
                 -- it would be easier to optimize -- imagine if it were to
                 -- to do a get from FDB for each item -- it could do them all
                 -- in parallel.
                 -> (a -> IO ())
                 -> Stream Void
  -- TODO: looks suspiciously similar to monadic bind
  StreamPipe :: (Messageable a, Messageable b)
             => StreamName
             -> Stream a
             -> (a -> IO (Maybe b))
             -> Stream b
  -- | Streaming one-to-one join. If the relationship is not actually one-to-one
  --   (i.e. the input join functions are not injective), some messages in the
  --   input streams could be lost.
  Stream1to1Join :: (Messageable a, Messageable b, Messageable c)
                 => StreamName
                 -> Stream a
                 -> Stream b
                 -> (a -> c)
                 -> (b -> c)
                 -> Stream (a,b)

subspace1to1JoinForKey :: Messageable k
                       => FDBStreamConfig
                       -> StreamName
                       -> k
                       -> Subspace
subspace1to1JoinForKey sc sn k =
  extend (streamConfigSS sc)
         [Bytes (sn <> "_11join"), Bytes (toMessage k)]

get1to1JoinData :: (Messageable a, Messageable b, Messageable k)
                => FDBStreamConfig
                -> StreamName
                -> k
                -> Transaction (Maybe (Either a b))
                -- ^ Returns whichever of the two join types was processed first
                -- for the given key, or Nothing if neither has yet been
                -- seen.
get1to1JoinData c sn k = do
  let ss = subspace1to1JoinForKey c sn k
  xs <- getEntireRange $ subspaceRange ss
  return $ case fmap (unpack ss . fst) xs of
    (Right [Bool True, Bytes x] :<| Empty) -> Just $ Left (fromMessage x)
    (Right [Bool False, Bytes x] :<| Empty) -> Just $ Right (fromMessage x)
    -- TODO: uncommenting this triggers https://ghc.haskell.org/trac/ghc/ticket/11822
    -- (_ :<| _ :<| Empty) -> error "consistency violation in 1-to-1 join logic"
    Empty -> Nothing
    (Left err :<| _) -> error $ "1to1 join deserialization error: " ++ show err
    _ -> error "Unexpected data in 1-to-1 join table"

delete1to1JoinData :: Messageable k
                   => FDBStreamConfig -> StreamName -> k -> Transaction ()
delete1to1JoinData c sn k = do
  let ss = subspace1to1JoinForKey c sn k
  let (x,y) = rangeKeys $ subspaceRange ss
  clearRange x y

write1to1JoinData :: (Messageable k, Messageable a, Messageable b)
                  => FDBStreamConfig
                  -> StreamName
                  -> k
                  -> Either a b
                  -> Transaction ()
write1to1JoinData c sn k x = do
  let ss = subspace1to1JoinForKey c sn k
  case x of
    Left y -> set (pack ss [Bool True, Bytes (toMessage y)]) ""
    Right y -> set (pack ss [Bool False, Bytes (toMessage y)]) ""

streamName :: Stream a -> StreamName
streamName (StreamProducer sn _) = sn
streamName (StreamConsumer sn _ _) = sn
streamName (StreamPipe sn _ _) = sn
streamName (Stream1to1Join sn _ _ _ _) = sn

-- TODO: other persistence backends
-- TODO: should probably rename to TopologyConfig
data FDBStreamConfig = FDBStreamConfig {
  streamConfigDB :: FDB.Database,
  streamConfigSS :: FDB.Subspace
}

inputTopics :: FDBStreamConfig -> Stream a -> [TopicConfig]
inputTopics _ (StreamProducer _ _) = []
inputTopics sc (StreamConsumer _ inp _) = [outputTopic sc inp]
inputTopics sc (StreamPipe _ inp _) = [outputTopic sc inp]
inputTopics sc (Stream1to1Join _ l r _ _) = [outputTopic sc l, outputTopic sc r]

outputTopic :: FDBStreamConfig -> Stream a -> TopicConfig
-- TODO: StreamConsumer has no output
outputTopic sc s =
  makeTopicConfig (streamConfigDB sc)
                  (extend (streamConfigSS sc) [Bytes (streamName s)])
                  (streamName s <> "_out")

foreverLogErrors :: StreamName -> IO () -> IO ()
foreverLogErrors sn x =
  forever $
  handle (\(e :: SomeException) -> putStrLn $ show sn ++ " thread caught " ++ show e) x

-- | Runs a stream. For each StreamPipe in the tree, reads from its input topic
-- in chunks, runs the monadic action on each input, and writes the result to
-- its output topic.
-- TODO: handle cyclical inputs
-- TODO: this function is responsible for
-- 1. traversing the structure
-- 2. executing a segment in the structure
-- 3. managing a thread that executes a segment in the structure
-- It should be broken up into multiple functions.
-- TODO: this traversal has surprising behavior if there are multiple branches
-- to the user's topology -- it only runs stream processors directly upstream
-- of the one on which it is called.
runStream :: FDBStreamConfig -> Stream a -> IO ()
runStream c@FDBStreamConfig{..} s@(StreamProducer rn step) = do
  -- TODO: what if this thread dies?
  -- TODO: this keeps spinning even if the producer is done and will never
  -- produce again.
  let outCfg = outputTopic c s
  void $ forkIO $ foreverLogErrors rn $ do
    xs <- catMaybes <$> replicateM 10 step -- TODO: batch size config
    writeTopic outCfg (fmap toMessage xs)

runStream c@FDBStreamConfig{..} s@(StreamConsumer rn inp step) = do
  runStream c inp
  let [inCfg] = inputTopics c s
  void $ forkIO $ foreverLogErrors rn $ do
    -- TODO: if parsing the message fails, should we still checkpoint?
    -- TODO: blockUntilNew
    xs <- readNAndCheckpoint inCfg rn 10
    mapM_ step (fmap (fromMessage . snd) xs)

runStream c@FDBStreamConfig{..} s@(StreamPipe rn inp step) = do
  runStream c inp
  let [inCfg] = inputTopics c s
  let outCfg = outputTopic c s
  -- TODO: blockUntilNew
  void $ forkIO $ foreverLogErrors rn $ FDB.runTransactionWithConfig infRetry (topicConfigDB inCfg) $ do
    xs <- readNAndCheckpoint' inCfg rn 10 --TODO: auto-adjust batch size
    let inMsgs = fmap (fromMessage . snd) xs
    ys <- catMaybes . toList <$> liftIO (mapM step inMsgs)
    let outMsgs = fmap toMessage ys
    writeTopic' outCfg outMsgs

runStream c@FDBStreamConfig{..} s@(Stream1to1Join rn (lstr :: Stream a) (rstr :: Stream b) pl pr) = do
  runStream c lstr
  runStream c rstr
  let [lCfg, rCfg] = inputTopics c s
  let outCfg = outputTopic c s
  void $ forkIO $ foreverLogErrors rn $ do
    -- TODO: the below two transactions are virtually identical, except
    -- swapping the usages of Left and Right. Need to find a way to eliminate
    -- the repetition.
    -- What this does is read from one of the two join streams, and for each
    -- message read, compute the join key. Using the join key, look in the
    -- join table to see if the partner is already there. If so, write the tuple
    -- downstream. If not, write the one message we do have to the join table.
    -- TODO: there's a lot of unnecessary blocking in the below code. Use
    -- futures to increase throughput.
    -- TODO: think of a way to garbage collect items that never get joined.
    FDB.runTransactionWithConfig infRetry (topicConfigDB lCfg) $ do
      lMsgs <- fmap (fromMessage.snd) <$> readNAndCheckpoint' lCfg rn 10
      toWrite <- fmap (catMaybes . toList) $ forM lMsgs $ \(lmsg :: a) -> do
        let k = pl lmsg
        get1to1JoinData c rn k >>= \case
          Just (Right (rmsg :: b)) -> do
            delete1to1JoinData c rn k
            return $ Just (lmsg, rmsg)
          Just (Left (_ :: a)) -> do
            liftIO $ putStrLn "Got unexpected Left"
            return Nothing -- TODO: warn (or error?) about non-one-to-one join
          Nothing -> do
            write1to1JoinData c rn k (Left lmsg :: Either a b)
            return Nothing
      writeTopic' outCfg (map toMessage toWrite)
    FDB.runTransactionWithConfig infRetry (topicConfigDB rCfg) $ do
      rMsgs <- fmap (fromMessage . snd) <$> readNAndCheckpoint' rCfg rn 10
      toWrite <- fmap (catMaybes . toList) $ forM rMsgs $ \rmsg -> do
        let k = pr rmsg
        get1to1JoinData c rn k >>= \case
          Just (Left (lmsg :: a)) -> do
            delete1to1JoinData c rn k
            return $ Just (lmsg, rmsg)
          Just (Right (_ :: b)) -> do
            liftIO $ putStrLn "Got unexpected Right"
            return Nothing -- TODO: warn (or error?) about non-one-to-one join
          Nothing -> do
            write1to1JoinData c rn k (Right rmsg :: Either a b)
            return Nothing
      -- TODO: I think we could dry out the above by using a utility function to
      -- return the list of stuff to write and then call swap before writing.
      writeTopic' outCfg (map toMessage toWrite)
