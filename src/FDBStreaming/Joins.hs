module FDBStreaming.Joins
  ( subspace1to1Join,
    delete1to1JoinData,
    write1to1JoinData,
    get1to1JoinData,
  )
where

import Data.ByteString (ByteString)
import FDBStreaming.Message (Message (fromMessage, toMessage))
import qualified FDBStreaming.Topic.Constants as C
import FoundationDB (Future, Transaction, clearRange, get, rangeKeys, set)
import FoundationDB.Layer.Subspace (Subspace, extend, pack, subspaceRange)
import FoundationDB.Layer.Tuple (Elem (Bytes, Int))

subspace1to1Join ::
  Message k => Subspace -> ByteString -> k -> Subspace
subspace1to1Join prefixSS joinName k =
  extend
    prefixSS
    [C.topics, Bytes joinName, C.oneToOneJoin, Bytes (toMessage k)]

delete1to1JoinData ::
  Message k => Subspace -> ByteString -> k -> Transaction ()
delete1to1JoinData prefixSS joinName k = do
  let ss = subspace1to1Join prefixSS joinName k
  let (x, y) = rangeKeys $ subspaceRange ss
  clearRange x y

write1to1JoinData ::
  (Message k, Message a) =>
  Subspace ->
  ByteString ->
  k ->
  Int ->
  a ->
  Transaction ()
write1to1JoinData prefixSS joinName k upstreamIx x = do
  let ss = subspace1to1Join prefixSS joinName k
  set (pack ss [Int $ fromIntegral upstreamIx]) (toMessage x)

get1to1JoinData ::
  (Message k, Message a) =>
  Subspace ->
  ByteString ->
  -- | index of the stream being joined. E.g., if this is a three-way join of
  -- streams x, y, and z, 0 means the message came from x, 1 from y, etc.
  Int ->
  k ->
  Transaction (Future (Maybe a))
get1to1JoinData prefixSS joinName upstreamIx k = do
  let ss = subspace1to1Join prefixSS joinName k
  let key = pack ss [Int $ fromIntegral upstreamIx]
  f <- get key
  return (fmap (fmap fromMessage) f)
