{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.Index
  ( Index (Index, indexSS, indexName),
    namedIndex,
    IndexName,
    index,
    indexCommitted,
    coordinateRangeForKey,
    coordinateRangeForKeyRange,
    coordinateRangeStream,
    countForKey,
    countForKeysRange,
    countForKeysStream,

    -- * Advanced usage
    parseIndexKey,
    parseCountKV,
  )
where

import Data.ByteString (ByteString)
import Data.Word (Word64)
import FDBStreaming.TableKey (OrdTableKey, TableKey (fromKeyBytes, toKeyBytes))
import qualified FDBStreaming.Topic as Topic
import qualified FDBStreaming.Topic.Constants as Constants
import FDBStreaming.Util (streamlyRangeResult, parseWord64le, addOneAtomic)
import qualified FoundationDB as FDB
import FoundationDB as FDB (KeySelector (FirstGreaterOrEq), RangeQuery (RangeQuery), Transaction, atomicOp, await)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Options.MutationType as Mut
import Safe (fromJustNote)
import qualified Streamly as S

indexKeys, counts :: FDB.Elem
indexKeys = FDB.Int 0
counts = FDB.Int 1

-- | A secondary index on a 'Topic'. This can be used to look up individual
-- messages within a topic.
data Index k
  = Index
      { indexSS :: FDB.Subspace,
        indexName :: IndexName
      }
  deriving (Eq, Show)

type IndexName = ByteString

-- | creates an Index for a topic. The index will be stored within the topic's
-- subspace, and will thus be deleted if the topic is deleted.
namedIndex :: Topic.Topic -> IndexName -> Index k
namedIndex t ixNm =
  Index
    { indexSS =
        FDB.extend
          (Topic.topicCustomMetadataSS t)
          [Constants.indices, FDB.Bytes ixNm],
      indexName = ixNm
    }

countKey :: TableKey k => Index k -> k -> ByteString
countKey Index {indexSS} k =
  FDB.pack indexSS [counts, FDB.Bytes (toKeyBytes k)]

-- | Parses a key returned from a range read initiated by
-- 'coordinateRangeForKey' or 'coordinateRangeForKeyRange'.
parseIndexKey :: (TableKey k) => Index k -> ByteString -> (k, Topic.Coordinate)
parseIndexKey Index {indexSS} k = case FDB.unpack indexSS k of
  Right [FDB.Int _, FDB.Bytes k', FDB.CompleteVS vs, FDB.Int pid, FDB.Int i] ->
    (fromKeyBytes k', Topic.Coordinate (fromIntegral pid) vs (fromIntegral i))
  x -> error $ "Failed to parse index key: " ++ show x

-- | Parse a key/value pair returned from a range read initiated by
-- 'countForKeysRange'.
parseCountKV ::
  (TableKey k) =>
  Index k ->
  (ByteString, ByteString) ->
  (k, Word64)
parseCountKV Index {indexSS} (k, v) =
  case (FDB.unpack indexSS k, parseWord64le v) of
    (Right [FDB.Int 1, FDB.Bytes k'], c) -> (fromKeyBytes k', c)
    _ -> error "Failed to parse count key/value pair."

-- | Index a single message in a topic by the given key. This must be called in
-- the same transaction that originally wrote the message to the topic, because
-- this function takes a 'CoordinateUncommitted' as input. In other words, this
-- function is used to implement immediately consistent indexes. For eventually
-- consistent indexes, see indexCommitted.
index ::
  TableKey k =>
  Index k ->
  k ->
  Topic.CoordinateUncommitted ->
  Transaction ()
index ix@Index {indexSS} k (Topic.CoordinateUncommitted pid vs i) = do
  let ik =
        FDB.pack
          indexSS
          [ indexKeys,
            FDB.Bytes (toKeyBytes k),
            FDB.IncompleteVS vs,
            FDB.Int $ fromIntegral pid,
            FDB.Int $ fromIntegral i
          ]
  FDB.atomicOp ik (Mut.setVersionstampedKey "")
  addOneAtomic (countKey ix k)

-- | Index a single message in a topic by the given key. This must be called in
-- a subsequent transaction to the one that originally wrote the message to
-- the topic, because this function takes a 'Coordinate' as input. This function
-- implements eventually consistent indexes.
indexCommitted ::
  TableKey k =>
  Index k ->
  k ->
  Topic.Coordinate ->
  Transaction ()
indexCommitted ix@Index {indexSS} k (Topic.Coordinate pid vs i) = do
  let ik =
        FDB.pack
          indexSS
          [ indexKeys,
            FDB.Bytes (toKeyBytes k),
            FDB.CompleteVS vs,
            FDB.Int $ fromIntegral pid,
            FDB.Int $ fromIntegral i
          ]
  FDB.set ik ""
  addOneAtomic (countKey ix k)

-- | Returns a range corresponding to all coordinates for the given key in the
-- given index. Coordinates will be returned in the order in which they were
-- committed. Reverse the range to return the most recently-committed
-- coordinates first. This can be consumed with 'FDB.getRange' or
-- 'coordinateRangeStream'.
coordinateRangeForKey :: TableKey k => Index k -> k -> RangeQuery
coordinateRangeForKey Index {indexSS} k =
  FDB.subspaceRangeQuery $ FDB.extend indexSS [indexKeys, FDB.Bytes (toKeyBytes k)]

-- | @coordinateRangeForKeyRange i k1 k2@
-- returns a range corresponding to all coordinates for the given indexed
-- range of keys between @k1@ and @k2@ (inclusive). Coordinates will be returned
-- in ascending order from k1 to k2, and in the order they were committed.
coordinateRangeForKeyRange :: OrdTableKey k => Index k -> k -> k -> RangeQuery
coordinateRangeForKeyRange Index {indexSS} k1 k2 =
  RangeQuery
    { FDB.rangeBegin =
        FDB.FirstGreaterOrEq $
          FDB.pack
            indexSS
            [ indexKeys,
              FDB.Bytes (toKeyBytes k1)
            ],
      FDB.rangeEnd =
        FDB.FirstGreaterThan
          $ fromJustNote "impossible index subspace in coordinateRangeForKey"
          $ FDB.prefixRangeEnd
          $ FDB.pack indexSS [indexKeys, FDB.Bytes (toKeyBytes k2)],
      FDB.rangeLimit = Nothing,
      FDB.rangeReverse = False
    }

-- | Given a range produced by 'coordinateRangeForKey' or
-- 'coordinateRangeForKeyRange', return a Streamly stream of key, coordinate
-- pairs from that range. Throws an exception if the range doesn't come from
-- either of the two coordinate range functions.
coordinateRangeStream ::
  ( TableKey k,
    S.IsStream t,
    -- TODO: ugly constraint will be redundant in newer
    -- versions of streamly.
    Functor (t Transaction)
  ) =>
  Index k ->
  RangeQuery ->
  FDB.Transaction (t FDB.Transaction (k, Topic.Coordinate))
coordinateRangeStream ix r = do
  rr <- FDB.getRange r >>= await
  return ((\(k, _) -> parseIndexKey ix k) <$> streamlyRangeResult rr)

-- | Returns the number of messages indexed by the given key.
countForKey :: TableKey k => Index k -> k -> Transaction (FDB.Future Word64)
countForKey ix k =
  fmap (maybe 0 parseWord64le)
    <$> FDB.get (countKey ix k)

-- | Returns a range containing the count of keys for each key between @k1@ and
-- @k2@ (inclusive). Keys will be returned in ascending order from @k1@ to @k2@.
-- If a key has a count of zero, it will not be included in the returned range.
countForKeysRange :: OrdTableKey k => Index k -> k -> k -> RangeQuery
countForKeysRange ix k1 k2 =
  RangeQuery
    { FDB.rangeBegin =
        FDB.FirstGreaterOrEq (countKey ix k1),
      FDB.rangeEnd =
        FDB.FirstGreaterThan (countKey ix k2),
      FDB.rangeLimit = Nothing,
      FDB.rangeReverse = False
    }

-- NOTE: why not define a newtype CountRange and use it to ensure
-- countForKeysStream is used correctly? Problem is that the user will want to
-- change the rangeLimit and rangeReverse fields, but to do that, they need to
-- be able to look inside the newtype. I guess we could just define a separate
-- type and define conversions to Range, but I fear that would interfere with
-- advanced use cases.

-- | Given a range produced by 'countForKeysRange', returns a Streamly stream of
-- key, count pairs from that range. Throws an exception if the range doesn't
-- come from 'countForKeysRange'.
countForKeysStream ::
  ( TableKey k,
    S.IsStream t,
    -- TODO: ugly constraint will be redundant in newer
    -- versions of streamly.
    Functor (t Transaction)
  ) =>
  Index k ->
  RangeQuery ->
  FDB.Transaction (t FDB.Transaction (k, Word64))
countForKeysStream ix r = do
  rr <- FDB.getRange r >>= await
  return (parseCountKV ix <$> streamlyRangeResult rr)
