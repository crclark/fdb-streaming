{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Lib where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.Binary.Get ( runGet
                       , getWord64le
                       , getWord32le
                       , getWord16le
                       , getWord8)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict)
import Data.Foldable (foldlM, toList)
import Data.Maybe (catMaybes)
import Data.Sequence (Seq(..))
import Data.Word (Word8, Word16, Word64)
import Data.Void
import FoundationDB as FDB
import FoundationDB.Options as Op
import FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Tuple as FDB
-- TODO: move prefixRangeEnd out of Advanced usage section.
import FoundationDB.Transaction (prefixRangeEnd)
import FoundationDB.Versionstamp (Versionstamp
                                  (CompleteVersionstamp,
                                   IncompleteVersionstamp),
                                  encodeVersionstamp,
                                  TransactionVersionstamp(..),
                                  VersionstampCompleteness(..),
                                  decodeVersionstamp)
import System.IO (stderr, hPutStrLn)

-- TODO: something less insane
infRetry :: TransactionConfig
infRetry = FDB.defaultConfig {maxRetries = maxBound}

type TopicName = ByteString

type ReaderName = ByteString

-- TODO: consider switching to the directory layer so the subspace strings
-- are shorter
data TopicConfig = TopicConfig { topicConfigDB :: FDB.Database
                               , topicSS :: FDB.Subspace
                               , topicName :: TopicName
                               , topicCountKey :: ByteString
                               , topicMsgsSS :: FDB.Subspace
                               , topicWriteOneKey :: ByteString
                               }
                               deriving Show

makeTopicConfig :: FDB.Database -> FDB.Subspace -> TopicName -> TopicConfig
makeTopicConfig topicConfigDB topicSS topicName = TopicConfig{..} where
  topicCountKey = FDB.pack topicSS [ Bytes topicName
                                   , Bytes "meta"
                                   , Bytes "count"
                                   ]
  topicMsgsSS = FDB.extend topicSS [Bytes topicName, Bytes "msgs"]
  topicWriteOneKey = FDB.pack topicMsgsSS [FDB.IncompleteVS (IncompleteVersionstamp 0)]

-- TODO: currently not used by anything, but might be useful.
incrTopicCount :: TopicConfig
               -> Transaction ()
incrTopicCount conf = do
  let k = topicCountKey conf
  let one = "\x01"
  FDB.atomicOp k (Op.add one)

getTopicCount :: TopicConfig
              -> Transaction (Maybe Word64)
getTopicCount conf = do
  let k = topicCountKey conf
  cBytes <- FDB.get k >>= await
  -- TODO: partial
  return $ fmap (runGet parse . fromStrict) cBytes
  where parse = getWord64le
                <|> fromIntegral <$> getWord32le
                <|> fromIntegral <$> getWord16le
                <|> fromIntegral <$> getWord8

readerSS :: TopicConfig -> ReaderName -> Subspace
readerSS TopicConfig{..} rn =
  extend topicSS [Bytes "rdrs", Bytes rn]

readerCheckpointKey :: TopicConfig
                    -> ReaderName
                    -> ByteString
readerCheckpointKey tc rn =
  FDB.pack (readerSS tc rn) [Bytes "ckpt"]

-- | Danger!! It's possible to write multiple messages with the same key
-- if this is called more than once in a single transaction.
writeTopic' :: Traversable t
            => TopicConfig
            -> t ByteString
            -> Transaction ()
writeTopic' tc@TopicConfig{..} bss = do
  _ <- foldlM go 1 bss
  return ()
    where
      go !i bs = do
        let vs = IncompleteVersionstamp i
        let k = FDB.pack topicMsgsSS [FDB.IncompleteVS vs]
        FDB.atomicOp k (setVersionstampedKey bs)
        incrTopicCount tc
        return (i+1)

-- TODO: support messages larger than FDB size limit, via chunking.
-- | Transactionally write a batch of messages to the given topic. The
-- batch must be small enough to fit into a single FoundationDB transaction.
-- DANGER: can only be called once per topic per transaction.
writeTopic :: Traversable t
           => TopicConfig
           -> t ByteString
           -> IO ()
writeTopic tc@TopicConfig{..} bss = do
  -- TODO: proper error handling
  guard (fromIntegral (length bss) < (maxBound :: Word16))
  FDB.runTransaction topicConfigDB $ writeTopic' tc bss

-- | Optimized function for writing a single message to a topic. The key is
-- precomputed once, so no time needs to be spent computing it. This may be
-- faster than writing batches of keys. Profile the code to find out!
-- DANGER: can only be called once per topic per transaction. Cannot be mixed
-- with other write functions in a single transaction.
writeOneMsgTopic :: TopicConfig
                 -> ByteString
                 -> IO ()
writeOneMsgTopic tc@TopicConfig{..} bs = do
  FDB.runTransaction topicConfigDB $ do
    FDB.atomicOp topicWriteOneKey (setVersionstampedKey bs)
    incrTopicCount tc

trOutput :: TopicConfig
         -> (ByteString, ByteString)
         -> (Versionstamp 'Complete, ByteString)
trOutput TopicConfig{..} (k,v) =
  case FDB.unpack topicMsgsSS k of
    Right [CompleteVS vs] -> (vs, v)
    Right t -> error $ "unexpected tuple: " ++ show t
    Left err -> error $ "failed to decode "
                        ++ show k
                        ++ " because "
                        ++ show err

-- TODO: should actually set the watch from the same transaction that did the
-- last read, so that we are guaranteed to be woken by the next write.
blockUntilNew :: TopicConfig -> IO ()
blockUntilNew conf@TopicConfig{..} = do
  let k = topicCountKey
  f <- FDB.runTransaction topicConfigDB (FDB.watch k)
  FDB.awaitIO f >>= \case
    Right () -> return ()
    Left err -> do
      hPutStrLn stderr $ "got error while watching: " ++ show err
      blockUntilNew conf

checkpoint' :: TopicConfig
            -> ReaderName
            -> Versionstamp 'Complete
            -> Transaction ()
checkpoint' tc rn vs = do
  let k = readerCheckpointKey tc rn
  let v = encodeVersionstamp vs
  FDB.atomicOp k (Op.byteMax v)

-- | For a given reader, returns a versionstamp that is guaranteed to be less
-- than the first uncheckpointed message in the topic. If the reader hasn't
-- made a checkpoint yet, returns a versionstamp containing all zeros.
getCheckpoint' :: TopicConfig
               -> ReaderName
               -> Transaction (Versionstamp 'Complete)
getCheckpoint' tc rn = do
  let cpk = readerCheckpointKey tc rn
  bs <- get cpk >>= await
  case decodeVersionstamp <$> bs of
    Just Nothing -> error $ "Failed to decode checkpoint: " ++ show bs
    Just (Just vs) -> return vs
    Nothing -> return $ CompleteVersionstamp (TransactionVersionstamp 0 0) 0

readNPastCheckpoint :: TopicConfig
                    -> ReaderName
                    -> Word8
                    -> Transaction (Seq (Versionstamp 'Complete, ByteString))
readNPastCheckpoint tc rn n = do
  cpvs <- getCheckpoint' tc rn
  let begin = FDB.pack (topicMsgsSS tc) [CompleteVS cpvs]
  let end = prefixRangeEnd $ FDB.subspaceKey (topicMsgsSS tc)
  let r = Range { rangeBegin = FirstGreaterThan begin
                , rangeEnd = FirstGreaterOrEq end
                , rangeLimit = Just (fromIntegral n)
                , rangeReverse = False
                }
  fmap (trOutput tc) <$> FDB.getEntireRange r

-- TODO: would be useful to have a version of this that returns a watch if
-- there are no new messages.
readNAndCheckpoint' :: TopicConfig
                    -> ReaderName
                    -> Word8
                    -> Transaction (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint' tc@TopicConfig{..} rn n =
  readNPastCheckpoint tc rn n >>= \case
    (x@(_ :|> (vs,_))) -> do
      checkpoint' tc rn vs
      return x
    _ -> return mempty

readNAndCheckpoint :: TopicConfig
                   -> ReaderName
                   -> Word8
                   -> IO (Seq (Versionstamp 'Complete, ByteString))
readNAndCheckpoint tc@TopicConfig{..} rn n =
  FDB.runTransactionWithConfig infRetry topicConfigDB (readNAndCheckpoint' tc rn n)

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
