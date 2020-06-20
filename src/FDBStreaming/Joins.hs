module FDBStreaming.Joins
  ( OneToOneJoinSS,
    oneToOneJoinSS,
    delete1to1JoinData,
    write1to1JoinData,
    get1to1JoinData,
  )
where

import FDBStreaming.Message (Message (fromMessage, toMessage))
import qualified FDBStreaming.Topic.Constants as C
import FoundationDB (Future, Transaction, clearRange, get, rangeKeys, set)
import FoundationDB.Layer.Subspace (Subspace, extend, pack, subspaceRange)
import FoundationDB.Layer.Tuple (Elem (Bytes, Int))

-- | For one-to-one joins, we have an outer subspace, and then for each join key
-- we have an inner subspace where we store the two sides of the join. In
-- practice, only one side is ever actually stored, because if one is in there
-- and we receive the other side, we can immediately flush downstream. If it is
-- an n-way join, up to n-1 messages will be stored in each key subspace.
newtype OneToOneJoinSS = OneToOneJoinSS Subspace
  deriving (Show, Eq, Ord)

oneToOneJoinSS :: Subspace -> OneToOneJoinSS
oneToOneJoinSS prefixSS = OneToOneJoinSS $ extend prefixSS [C.oneToOneJoin]

oneToOneKeySS :: Message k => OneToOneJoinSS -> k -> Subspace
oneToOneKeySS (OneToOneJoinSS ss) k = extend ss [Bytes (toMessage k)]

delete1to1JoinData ::
  Message k => OneToOneJoinSS -> k -> Transaction ()
delete1to1JoinData joinSS k = do
  let ss = oneToOneKeySS joinSS k
  let (x, y) = rangeKeys $ subspaceRange ss
  clearRange x y

write1to1JoinData ::
  (Message k, Message a) =>
  OneToOneJoinSS ->
  k ->
  Int ->
  a ->
  Transaction ()
write1to1JoinData joinSS k upstreamIx x = do
  let ss = oneToOneKeySS joinSS k
  set (pack ss [Int $ fromIntegral upstreamIx]) (toMessage x)

get1to1JoinData ::
  (Message k, Message a) =>
  OneToOneJoinSS ->
  -- | index of the stream being joined. E.g., if this is a three-way join of
  -- streams x, y, and z, 0 means the message came from x, 1 from y, etc.
  Int ->
  k ->
  Transaction (Future (Maybe a))
get1to1JoinData joinSS upstreamIx k = do
  let ss = oneToOneKeySS joinSS k
  let key = pack ss [Int $ fromIntegral upstreamIx]
  f <- get key
  return (fmap (fmap fromMessage) f)
