{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module FDBStreaming.Joins.OneToMany (
  oneToManyJoinSS,
  OneToManyJoinSS,
  lMessageJob,
  rMessageJob,
  flushBacklogJob,
  -- * Exported for tests only
  setLStoreMsg,
  getLStoreMsg,
  existsRBacklog,
  addRBacklogMessage,
  getAndDeleteRBacklogMessages,
  getArbitraryFlushableBackloggedKey,
  addFlushableBackloggedKey,
  removeFlushableBackloggedKey
) where

import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_)
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified Data.Sequence as Seq
import Data.Sequence (Seq ((:<|), (:|>), Empty))
import Data.Traversable (for)
import Data.Witherable.Class (catMaybes)
import Data.Word (Word16)
import FDBStreaming.Message (Message (fromMessage, toMessage))
import FDBStreaming.Topic (PartitionId)
import qualified FDBStreaming.Topic.Constants as C
import qualified FoundationDB as FDB
import FoundationDB (Future, RangeQuery (RangeQuery, rangeBegin), Transaction, atomicOp, clear, clearRange, get, getKey, set)
import qualified FoundationDB.Layer.Subspace as FDB
import FoundationDB.Layer.Subspace (Subspace, extend, pack, rawPrefix, subspaceRangeQuery)
import FoundationDB.Layer.Tuple (Elem (Bytes, CompleteVS, IncompleteVS, Int))
import qualified FoundationDB.Options.MutationType as Mut
import FoundationDB.Versionstamp (
    Versionstamp (IncompleteVersionstamp),
  )
import GHC.Exts (IsList (fromList, toList))

{-
One-to-many joins. In this case, we have streams l and r.
Each message in l must be joined to many messages in r, and each message in r
must be joined to exactly one message in l. Messages are joined by a key k. We
could even assign each message in l and r to multiple keys, so long as for a
given key, only one l message is assigned to it.

There are a few cases when we are consuming these two streams.

l cases:
1. We receive a new message and there are no r-messages waiting for it. We write
   the l-message to the l-message store, keyed by k.
2. We receive a new message and there are r-messages waiting for it. We write
   the l-message to the l-message store, and we write its key to the set of
   keys whose backlogs are ready to be flushed downstream.

r cases:
1. We receive a new r-message and there is no l-message waiting for it. We write
   the r-message to the key's backlog. We DO NOT write the key to the set of
   ready-to-flush backlogs, because we don't yet have enough information to begin
   flushing the backlog.
2. We receive a new r-message and there is an l-message waiting for it. We
   immediately write the joined pair downstream.

These two workers' tasks are straightforward. The interesting part, however, is
how to work through a large backlog of r-messages. There could be a huge number,
so it can't be done in one transaction. We have a list of l-messages with
backlogs, so what this backlog worker should do is:

1. Read the first key of the keys-with-flushable-backlogs set.
2. Read and delete a chunk of its backlog.
3. Read the stored l-message.
3. Do the join using the values read in steps 2 and 3 and write
   the result downstream.
4. If the read of the backlog chunk was empty, delete the item from the set of
   keys with backlogs to be flushed.

Open questions:

1. Should this work be done as part of the l or r transaction, or as a totally
   separate transactional job? Answer: separate job.
2. What if we keep getting more r-messages while clearing the backlog for an
   l-message? Answer: no problem. Remember that after we have seen the l-msg,
   we never write to the backlog for that key again. We immediately join and
   flush downstream.
3. If we are getting hammered by r-messages for a single key that is the first
   in the backlog, we could get stuck just emitting messages for that key for
   a really long time. Should we try to guarantee fairness? We could select
   random items from the backlog list on each iteration, but we would need
   to partition the backlog. See 4. Answer: not a problem. Backlog flush is a
   separate job.
4. How do we distribute the backlog task across multiple workers? I guess we
   can partition it by a hash of the join key, or something.

-}

newtype OneToManyJoinSS = OneToManyJoinSS Subspace
  deriving (Eq, Show)

--  Do we even need to partition by key?
-- The reason we partition in the first place is to avoid conflicts among the
-- workers. What conflicts would we avoid by partitioning?
-- Conflict 1: reading the same backlogged key and trying to do the same work.
-- Seems that we must indeed partition the backlogged keys, if nothing else.
-- So we do need to partition, but do we need to partition by key, which has
-- the potential to unbalance the work done by backlog flushers?
-- Backlog flushers need to read the l message store, the r message store, and
-- the backlogged keys store. l-message store reads are always single key point
-- reads, so no real conflict potential. r-message stores are subdivided by join
-- key, so flushers won't conflict with each other unless they are working on
-- the same key (which we assume they aren't). They also won't conflict with
-- writers to the backlog, because no one is writing to the key's backlog after
-- a backlog flusher has started flushing it. Ergo, only the backlogged key
-- store fundamentally needs to be partitioned. Since only one of the stores is
-- partitioned, we don't have to worry about not being able to find a message
-- for a key because data for key k in store A wasn't placed in the same
-- partition as data for key k in store B. Thus, we don't need to make the
-- partitioning a function of key... and we seem to have no problem with hot
-- keys! Cool.

oneToManyJoinSS :: Subspace -> OneToManyJoinSS
oneToManyJoinSS ss = OneToManyJoinSS $ extend ss [C.oneToManyJoin]

-- | Subspace for storing l-messages (the "one" side of the one-to-many join).
--   Because we don't know how many r-messages we will receive, l-messages must
--   be stored forever, and total space usage will grow without bound. At least
--   in the naive case. In practice, we will probably want to be able to delete
--   old l-messages. TODO: garbage collection of l-messages?
lStoreSS :: OneToManyJoinSS -> Subspace
lStoreSS (OneToManyJoinSS ss) = extend ss [C.oneToManyJoinLStore]

-- | Temporary storage for r-messages (the "many" side of the one-to-many join).
--   They wait here until their corresponding l-message is received.
--   These are stored as versionstamped keys to allow any job to write them
--   without conflicts.
--   TODO: because this ss is not partitioned, we can have at most one reader of
--   each key's backlog. This means that if we have a large number of messages
--   for one key, we only have one thread working on them. We may want to
--   improve that.
rBacklogSS :: Message k => OneToManyJoinSS -> k -> Subspace
rBacklogSS (OneToManyJoinSS ss) k =
  extend ss [C.oneToManyJoinBacklog, Bytes (toMessage k)]

addRBacklogMessage ::
  (Message k, Message r) =>
  OneToManyJoinSS ->
  k ->
  r ->
  Word16 ->
  Transaction ()
addRBacklogMessage ss k r n = do
  let rSS = rBacklogSS ss k
  let vs = IncompleteVersionstamp n
  let kbs = pack rSS [IncompleteVS vs]
  atomicOp kbs (Mut.setVersionstampedKey (toMessage r))

-- | Get up to n backlog r-messages for k and delete them from the subspace.
--   NOTE: this reads from the top of the subspace, so that writers can write
--   to the bottom without conflicts (hopefully).
getAndDeleteRBacklogMessages ::
  (Message k, Message r) =>
  OneToManyJoinSS ->
  k ->
  Int ->
  Transaction (Seq r)
getAndDeleteRBacklogMessages ss k n = do
  let rSS = rBacklogSS ss k
  let r = (subspaceRangeQuery rSS) {FDB.rangeLimit = Just n}
  rs <- FDB.getEntireRange r
  -- delete what we read. The case matching is used to do a more efficient
  -- clearRange command for most of the range when possible. However, clearRange
  -- is exclusive of the last key, so we need to explicitly clear it, too.
  case rs of
    Empty -> return ()
    start :<| rs' -> case rs' of
      Empty -> clear (fst start)
      _ :|> end -> clearRange (fst start) (fst end) >> clear (fst end)
  return (fmap (fromMessage . snd) rs)

-- | Returns True iff messages exist in the backlog for the given k.
existsRBacklog ::
  Message k =>
  OneToManyJoinSS ->
  k ->
  Transaction (Future Bool)
existsRBacklog ss k = do
  let rSS = rBacklogSS ss k
  -- TODO: could use getKey instead but the string comparisons might be more
  -- code.
  let
  let ks = FDB.FirstGreaterOrEq $ pack rSS [CompleteVS minBound]
  future <- FDB.getKey ks
  return $ fmap (FDB.contains rSS) future

-- | Subspace to contain the set of keys for which a backlog of messages exists,
-- and for which we have received the l-message. Invariant: if a key exists in
-- this set, then the l-message for this key is in lStoreSS and at least one
-- r-message exists for this key in rBacklogSS. This means that we have
-- everything we need to flush the backlog downstream.
-- This is partitioned in order to allow multiple backlog flush workers to
-- each select an arbitrary key without conflicting.
flushableBackloggedKeysSS ::
  OneToManyJoinSS ->
  PartitionId ->
  Subspace
flushableBackloggedKeysSS (OneToManyJoinSS ss) pid =
  extend ss [C.oneToManyJoinBackloggedKeys, Int $ fromIntegral pid]

getLStoreMsg ::
  (Message k, Message l) =>
  OneToManyJoinSS ->
  k ->
  Transaction (Future (Maybe l))
getLStoreMsg ss k = do
  let lss = lStoreSS ss
  let key = pack lss [Bytes (toMessage k)]
  fmap (fmap fromMessage) <$> get key

setLStoreMsg ::
  (Message k, Message l) =>
  OneToManyJoinSS ->
  k ->
  l ->
  Transaction ()
setLStoreMsg ss k l = do
  let lss = lStoreSS ss
  let key = pack lss [Bytes (toMessage k)]
  set key (toMessage l)

-- | Add a key to set of backlogged keys. See 'flushableBackloggedKeysSS' for the
-- invariant.
addFlushableBackloggedKey ::
  Message k =>
  OneToManyJoinSS ->
  PartitionId ->
  k ->
  Transaction ()
addFlushableBackloggedKey ss pid k =
  set (pack (flushableBackloggedKeysSS ss pid) [Bytes (toMessage k)]) ""

-- | Get an arbitrary key for which a backlog of r-messages exists, and for
-- which an l-message exists in lStoreSS. Returns
-- Nothing if no backlogged keys exist.
-- For performance and type ambiguity reasons, does not parse the key. Relies on
-- the fact that toMessage for ByteStrings is id.
getArbitraryFlushableBackloggedKey ::
  OneToManyJoinSS ->
  PartitionId ->
  Transaction (Future (Maybe ByteString))
getArbitraryFlushableBackloggedKey ss pid = do
  let backloggedSS = flushableBackloggedKeysSS ss pid
  let RangeQuery {rangeBegin} = subspaceRangeQuery backloggedSS
  -- TODO: if this is too much load on one key, we could randomly select either
  -- the begin or end of the range to get a key.
  f <- getKey rangeBegin
  let prefix = rawPrefix backloggedSS
  let f' = flip fmap f $ \bs ->
        case (BS.isPrefixOf prefix bs, FDB.unpack backloggedSS bs) of
          (False, _) -> Nothing
          (True, Left err) -> error $ "error decoding backlog key: " ++ show err
          (True, Right [Bytes kbs]) -> Just kbs
          (True, Right _) -> error "Failed to unpack backlog key in getArbitraryFlushableBackloggedKey"
  return f'

-- | Remove a key from the set of flushable backlogged keys.
removeFlushableBackloggedKey ::
  Message k =>
  OneToManyJoinSS ->
  PartitionId ->
  k ->
  Transaction ()
removeFlushableBackloggedKey ss pid k =
  clear (pack (flushableBackloggedKeysSS ss pid) [Bytes (toMessage k)])

-- | The "main" function for the job that processes l-messages. See the long
-- comment above for more details.
lMessageJob ::
  (Message k, Message l) =>
  OneToManyJoinSS ->
  PartitionId ->
  (l -> k) ->
  Seq l ->
  Transaction ()
lMessageJob ss pid f ls = do
  for_ ls $ \l -> do
    setLStoreMsg ss (f l) l
  backlogExistencePerKey <- for (fmap f ls) $ \k -> do
    b <- existsRBacklog ss k
    return (fmap (k,) b)
  for_ backlogExistencePerKey $
    \future -> FDB.await future
      >>= \case (k, exists) -> when exists (addFlushableBackloggedKey ss pid k)

-- | The "main" function for the job that processes r-messages. See the long
-- comment above for more details.
rMessageJob ::
  (Message k, Message l, Message r) =>
  OneToManyJoinSS ->
  (r -> k) ->
  Seq r ->
  (l -> r -> Transaction c) ->
  Transaction (Seq c)
rMessageJob ss toKey rs joinFn = do
  -- NOTE: this code is a bit ugly because I want to avoid an Eq constraint
  -- on k. To avoid it, I use the Message instance to use equality on the keys
  -- instead. This also saves some redundant toMessage calls by exploiting the
  -- fact that toMessage for ByteString is 'id'.
  let rsWithKeyBytes = fmap (\r -> (r, toMessage $ toKey r)) rs
  let keyBytesToFetch = fromList @ (Set.Set ByteString)
                        $ toList
                        $ fmap snd rsWithKeyBytes
  -- TODO: do we have a conflict with lMessageJob here between setLStoreMsg
  -- and getLStoreMsg?
  lStoreMsgFutures <- for (toList keyBytesToFetch)
                      $ \kbs -> (kbs,) <$> getLStoreMsg ss kbs
  lStoreMsgs <- fmap fromMessage . Map.fromList . catMaybes
                <$> for lStoreMsgFutures
                    (\(k, fl) -> fmap (k,) <$> FDB.await fl)
  let forWithIndex = flip Seq.traverseWithIndex
  fmap catMaybes $ forWithIndex rsWithKeyBytes $ \i (r, kbs) ->
    case Map.lookup kbs lStoreMsgs of
      Nothing -> addRBacklogMessage ss kbs (toMessage r) (fromIntegral i) >> return Nothing
      Just l -> Just <$> joinFn l r

-- | The main function for the job that flushes backlogged r-messages.
-- NOTE: this must be run with the same number of partitions as lMessageJob.
flushBacklogJob ::
  (Message l, Message r) =>
  OneToManyJoinSS ->
  PartitionId ->
  (l -> r -> Transaction c) ->
  Int -> --batch size
  Transaction (Seq c)
flushBacklogJob ss pid joinFn batchSize =
  getArbitraryFlushableBackloggedKey ss pid >>= FDB.await >>= \case
    Nothing -> return mempty
    Just flushableKey -> do
      lf <- getLStoreMsg ss flushableKey
      rs <- getAndDeleteRBacklogMessages ss flushableKey batchSize
      FDB.await lf >>= \case
        -- if l has not been written yet, the key is not flushable, so it
        -- shouldn't have been returned by getArbitraryFlushableBackloggedKey.
        Nothing -> error "impossible case in flushBacklogJob"
        Just l -> do
          when (Seq.null rs) (removeFlushableBackloggedKey ss pid flushableKey)
          for rs (joinFn l)
