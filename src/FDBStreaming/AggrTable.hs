{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.AggrTable (
  AggrTable(..),
  getRow,
  getRowRange,
  getBlocking,
  aggrTableWatermarkSS,
  TableKey(..),
  OrdTableKey,
  TableSemigroup(..),
  RangeAccessibleTable(..),
  -- * Helpers for defining instances
  setMessage,
  getMessage,
  mappendMessageBatch,
  setVia,
  getVia,
  mappendAtomicVia,
  getTableRangeVia,
  PutIntLE(..)
) where

-- TODO: runGet throws an exception if it fails to parse! Replace with runGetMay
-- from FDBStreaming.Util

import Control.Concurrent.Async (AsyncCancelled, async, waitAnyCancel)
import Control.Exception (catch)
import Control.Monad (forM, void)
import Data.Binary.Get (Get,
                        runGet,
                        getWord8,
                        getWord16le,
                        getWord32le,
                        getWord64le,
                        getInt8,
                        getInt16le,
                        getInt32le,
                        getInt64le)
import Data.Binary.Put (Put,
                        runPut,
                        putWord8,
                        putWord16le,
                        putWord32le,
                        putWord64le,
                        putInt8,
                        putInt16le,
                        putInt32le,
                        putInt64le)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Foldable (fold, for_, toList)
import Data.Int (Int8, Int16, Int32, Int64)
import Data.Map.Strict (Map, unionsWith)
import qualified Data.Map.Strict as Map
import Data.Monoid (Sum(Sum, getSum), All(All, getAll), Any(Any, getAny))
import Data.Semigroup (Max(Max, getMax), Min(Min, getMin))
import Data.Text (Text)
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Data.Word (Word8, Word16, Word32, Word64)

import FDBStreaming.Message(Message (fromMessage, toMessage))
import FDBStreaming.Topic (PartitionId)

import qualified FoundationDB as FDB
import qualified FoundationDB.Options as Op
import qualified FoundationDB.Layer.Subspace as SS
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Versionstamp as FDB


-- | A table of monoidal values, resulting from aggregating a grouped stream.
-- Internally, tables are partitioned -- to read a row from a table, we read
-- the value at every partition, then mappend them together. The helper
-- functions 'getRow' and 'getRowRange' do this for you.
data AggrTable k v = AggrTable {
  aggrTableSS :: SS.Subspace
  , aggrTableNumPartitions :: Integer
  } deriving (Eq, Show)

aggrTableWatermarkSS :: AggrTable k v -> SS.Subspace
aggrTableWatermarkSS = flip SS.extend [FDB.Bytes "wm"] . aggrTableSS

-- | Class of types that can be serialized as table keys. This is distinct from
-- 'Message' to enable cases where the user may want to read entire ranges of
-- a table efficiently. In such cases, the serialized representation of the
-- key must have a lexicographic ordering equivalent to the 'Ord' instance of
-- the type. For the 'Message' class, we don't require any ordering of the
-- serialized representation, which may admit for more efficient serialization
-- in some cases.
--
-- Instances are provided for some types.
class TableKey a where
  toKeyBytes :: a -> ByteString
  fromKeyBytes :: ByteString -> a

-- | An additional tag to place on types that satisfy
-- @compare x y == compare (toKeyBytes x) (toKeyBytes y)@
class TableKey a => OrdTableKey a

-- | Class for aggregate values stored in tables that were created by 'GroupBy'.
-- Laws:
--
--    * If @x@ is the value already stored in the table at @k@, then
--      @mappendTable table k y@ should have the effect of storing @mappend x y@
--      in @table@ at @k@.
--    * If no value exists in @table@ at @k@, then @mappendTable table k x@ has
--      the same effect as @set table k x@.
--    * Must be a commutative semigroup.
class (Semigroup v) => TableSemigroup v where
  -- | mappends the given key,value pairs to the existing values at each k in
  -- the table. If k has not been set,
  -- sets k to the provided value instead. Some implementations may use
  -- atomic FoundationDB operations to improve performance.
  mappendBatch :: (Traversable t, Ord k, TableKey k)
               => AggrTable k v -> PartitionId -> t (k,v) -> FDB.Transaction ()
  -- | Overwrites the value at @k@ in the table. Use with caution.
  set :: TableKey k => AggrTable k v -> PartitionId -> k -> v -> FDB.Transaction ()
  -- | Gets the value at @k@, if present.
  get :: TableKey k => AggrTable k v -> PartitionId -> k -> FDB.Transaction (FDB.Future (Maybe v))

-- | Helper function to define 'TableSemigroup.set' easily.
setVia :: TableKey k
       => (v -> ByteString)
       -> AggrTable k v
       -> PartitionId
       -> k
       -> v
       -> FDB.Transaction ()
setVia f t pid k v = do
  let kbs = SS.pack (aggrTableSS t) [FDB.Int pid, FDB.Bytes (toKeyBytes k)]
  let vbs = f v
  FDB.set kbs vbs

getVia :: TableKey k
       => (ByteString -> v)
       -> AggrTable k v
       -> PartitionId
       -> k
       -> FDB.Transaction (FDB.Future (Maybe v))
getVia f t pid k = do
  let kbs = SS.pack (aggrTableSS t) [FDB.Int pid, FDB.Bytes (toKeyBytes k)]
  fmap (fmap f) <$> FDB.get kbs

mappendAtomicVia :: TableKey k
                 => (v -> ByteString)
                 -> (ByteString -> Op.MutationType)
                 -> AggrTable k v
                 -> PartitionId
                 -> k
                 -> v
                 -> FDB.Transaction ()
mappendAtomicVia f op t pid k v = do
  let kbs = SS.pack (aggrTableSS t) [FDB.Int pid, FDB.Bytes (toKeyBytes k)]
  let vbs = f v
  FDB.atomicOp kbs (op vbs)

-- | Helper to easily define an instance of 'TableSemigroup' for types that
-- implement 'Message'. This is less efficient than using FDB atomic ops, but
-- works with more types. 'v' should be a type that is guaranteed to have a
-- bounded serialized size in your use case -- at most, a few megabytes. If this
-- is not the case, the pipeline could become permanently stuck, because
-- individual transactions could exceed FoundationDB's per-transaction limits.
-- For unbounded monoidal values such as sets, lists, etc., see TODO.
setMessage :: (TableKey k, Message v)
           => AggrTable k v
           -> PartitionId
           -> k
           -> v
           -> FDB.Transaction ()
setMessage = setVia toMessage

getMessage :: (TableKey k, Message v)
           => AggrTable k v
           -> PartitionId
           -> k
           -> FDB.Transaction (FDB.Future (Maybe v))
getMessage = getVia fromMessage

mappendMessageBatch :: (Ord k, TableKey k, Message v, Traversable t, Semigroup v)
                    => AggrTable k v
                    -> PartitionId
                    -> t (k,v)
                    -> FDB.Transaction ()
mappendMessageBatch table pid kvs = do
  let kvs' = Map.fromListWith (<>) $ toList kvs
  let forWithKey = flip Map.traverseWithKey
  toWriteFutures <- forWithKey kvs' $ \k v ->
    fmap (maybe v (<> v)) <$> getMessage table pid k
  void $ forWithKey toWriteFutures $ \k v -> do
    v' <- FDB.await v
    setMessage table pid k v'

-- Types of tables for which a range of k,v pairs can be efficiently accessed.
class RangeAccessibleTable v where
  -- | For tables with keys whose serialized representation is ordered and
  -- the state of each table row is of bounded size, get a range of k,v pairs
  -- from the table.
  getTableRange :: (Ord k, OrdTableKey k, TableSemigroup v)
                => AggrTable k v
                -> PartitionId
                -> k
                -- ^ Beginning of the range
                -> k
                -- ^ End of the range, inclusive
                -> FDB.Transaction (Map k v)

getTableRangeVia :: (Ord k, OrdTableKey k)
                 => AggrTable k v
                 -> PartitionId
                 -> k
                 -> k
                 -> (ByteString -> v)
                 -> FDB.Transaction (Map k v)
getTableRangeVia table pid start end parse = do
 let startK = SS.pack (aggrTableSS table) [FDB.Int pid, FDB.Bytes (toKeyBytes start)]
 let endK = SS.pack (aggrTableSS table) [FDB.Int pid, FDB.Bytes (toKeyBytes end)]
 let range = FDB.keyRangeInclusive startK endK
 let unwrapOuterBytes bs = case SS.unpack (aggrTableSS table) bs of
       Left err -> error $ "Error decoding table key in getTableRangeVia: " ++ show err
       Right [FDB.Int _, FDB.Bytes bs'] -> bs'
       Right _ -> error "Unexpected tuple in getTableRangeVia"
 let parser (k,v) = (fromKeyBytes (unwrapOuterBytes k), parse v)
 (Map.fromList . toList) . fmap parser <$> FDB.getEntireRange range

-- | Class of types that can be serialized to little-endian integers. These
-- types can be used with 'Min', 'Max', and 'Sum' for high performance
-- aggregation with FoundationDB atomics.
class Num a => PutIntLE a where
  putIntLE :: a -> Put
  getIntLE :: Get a

-- | Gets a value from the table. If the key is not present, blocks until it
-- is written. Uses one FoundationDB watch per table partition internally. Not
-- recommended for high-volume usage.
getBlocking :: (TableSemigroup v, TableKey k)
            => FDB.Database
            -> AggrTable k v
            -> k
            -> IO v
getBlocking db at k = do
  result <- FDB.runTransaction db $ getRow at k >>= \case
    Nothing -> fmap Left
               $ forM [0..(aggrTableNumPartitions at - 1)]
               $ \pid -> do
                 let kbs = SS.pack (aggrTableSS at)
                                   [FDB.Int pid, FDB.Bytes (toKeyBytes k)]
                 FDB.watch kbs
    Just v -> return (Right v)
  case result of
    Right v -> return v
    Left ws -> do
      asyncs <- forM ws
                $ \w ->
                  async
                  $ catch (FDB.awaitInterruptibleIO w)
                    $ \(_e :: AsyncCancelled) -> FDB.cancelFutureIO w
      waitAnyCancel asyncs >> getBlocking db at k

getRow :: (TableKey k, TableSemigroup v) => AggrTable k v -> k -> FDB.Transaction (Maybe v)
getRow table k = do
  futs <- forM [0..(aggrTableNumPartitions table - 1)] $ \pid -> get table pid k
  fold <$> traverse FDB.await futs

getRowRange :: (Ord k, OrdTableKey k, RangeAccessibleTable v, TableSemigroup v)
            => AggrTable k v
            -> k
            -- ^ Beginning of the range
            -> k
            -- ^ End of the range, inclusive
            -> FDB.Transaction (Map k v)
getRowRange table start end = do
  maps <- forM [0..(aggrTableNumPartitions table - 1)] $ \pid ->
    getTableRange table pid start end
  return $ unionsWith (<>) maps

instance TableKey ByteString where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Bytes x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Bytes TableKey: " ++ show err
    Right [FDB.Bytes x] -> x
    -- TODO: print actual type we decoded in error message, but don't print
    -- raw bytes value.
    Right _ -> error "Expected Bytes when decoding TableKey"

instance OrdTableKey ByteString

instance TableKey Text where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Text x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Text TableKey: " ++ show err
    Right [FDB.Text x] -> x
    Right _ -> error "Expected Text when decoding TableKey"

instance OrdTableKey Text

instance TableKey Integer where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Int x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Integer TableKey: " ++ show err
    Right [FDB.Int x] -> x
    Right _ -> error $ "Expected Integer when decoding TableKey"

instance OrdTableKey Integer

instance TableKey Int where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Int $ fromIntegral x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Int TableKey: " ++ show err
    Right [FDB.Int x] -> fromIntegral x
    Right _ -> error $ "Expected Int when decoding TableKey"

instance OrdTableKey Int

instance TableKey Float where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Float x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Float TableKey: " ++ show err
    Right [FDB.Float x] -> x
    Right _ -> error "Expected Float when decoding TableKey"

instance OrdTableKey Float

instance TableKey Double where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Double x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Double TableKey: " ++ show err
    Right [FDB.Double x] -> x
    Right _ -> error "Expected Double when decoding TableKey"

instance OrdTableKey Double

instance TableKey Bool where
  toKeyBytes x = FDB.encodeTupleElems [FDB.Bool x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Bool TableKey: " ++ show err
    Right [FDB.Bool x] -> x
    Right _ -> error "Expected Bool when decoding TableKey"

instance OrdTableKey Bool

instance TableKey UUID where
  toKeyBytes x = FDB.encodeTupleElems [FDB.UUID w1 w2 w3 w4]
    where (w1, w2, w3, w4) = UUID.toWords x
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode UUID TableKey: " ++ show err
    Right [FDB.UUID w1 w2 w3 w4] -> UUID.fromWords w1 w2 w3 w4
    Right _ -> error "Expected UUID when decoding TableKey"

instance OrdTableKey UUID

instance TableKey (FDB.Versionstamp 'FDB.Complete) where
  toKeyBytes x = FDB.encodeTupleElems [FDB.CompleteVS x]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode Versionstamp TableKey: " ++ show err
    Right [FDB.CompleteVS x] -> x
    Right _ -> error "Expected Versionstamp when decoding TableKey"

instance OrdTableKey (FDB.Versionstamp 'FDB.Complete)

instance TableKey () where
  toKeyBytes () = FDB.encodeTupleElems [FDB.Bytes "()"]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode () TableKey" ++ show err
    Right [FDB.Bytes "()"] -> ()
    Right _ -> error "Unexpected bytes decoding unit TableKey"

instance (TableKey a, TableKey b) => TableKey (a,b) where
  toKeyBytes (x,y) = FDB.encodeTupleElems [FDB.Bytes (toKeyBytes x),
                                           FDB.Bytes (toKeyBytes y)]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode 2-tuple TableKey: " ++ show err
    Right [FDB.Bytes x, FDB.Bytes y] -> (fromKeyBytes x, fromKeyBytes y)
    Right _ -> error "Expected 2-tuple when decoding TableKey"

instance (TableKey a, TableKey b) => OrdTableKey (a,b)

instance (TableKey a, TableKey b, TableKey c) => TableKey (a,b,c) where
  toKeyBytes (x,y,c) = FDB.encodeTupleElems [FDB.Bytes (toKeyBytes x),
                                             FDB.Bytes (toKeyBytes y),
                                             FDB.Bytes (toKeyBytes c)]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode 3-tuple TableKey: " ++ show err
    Right [FDB.Bytes x, FDB.Bytes y, FDB.Bytes c] -> (fromKeyBytes x, fromKeyBytes y, fromKeyBytes c)
    Right _ -> error "Expected 3-tuple when decoding TableKey"

instance (TableKey a, TableKey b, TableKey c) => OrdTableKey (a,b,c)

instance (TableKey a, TableKey b, TableKey c, TableKey d) => TableKey (a,b,c,d) where
  toKeyBytes (x,y,c,d) = FDB.encodeTupleElems [FDB.Bytes (toKeyBytes x),
                                               FDB.Bytes (toKeyBytes y),
                                               FDB.Bytes (toKeyBytes c),
                                               FDB.Bytes (toKeyBytes d)]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode 4-tuple TableKey: " ++ show err
    Right [FDB.Bytes x, FDB.Bytes y, FDB.Bytes c, FDB.Bytes d] ->
      (fromKeyBytes x, fromKeyBytes y, fromKeyBytes c, fromKeyBytes d)
    Right _ -> error "Expected 4-tuple when decoding TableKey"

instance (TableKey a, TableKey b, TableKey c, TableKey d) => OrdTableKey (a,b,c,d)

instance (TableKey a, TableKey b, TableKey c, TableKey d, TableKey e)
         => TableKey (a,b,c,d,e) where
  toKeyBytes (x,y,c,d,e) = FDB.encodeTupleElems [FDB.Bytes (toKeyBytes x),
                                                 FDB.Bytes (toKeyBytes y),
                                                 FDB.Bytes (toKeyBytes c),
                                                 FDB.Bytes (toKeyBytes d),
                                                 FDB.Bytes (toKeyBytes e)]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode 5-tuple TableKey: " ++ show err
    Right [FDB.Bytes x, FDB.Bytes y, FDB.Bytes c, FDB.Bytes d, FDB.Bytes e] ->
      (fromKeyBytes x, fromKeyBytes y, fromKeyBytes c, fromKeyBytes d, fromKeyBytes e)
    Right _ -> error "Expected 5-tuple when decoding TableKey"

instance (TableKey a, TableKey b, TableKey c, TableKey d, TableKey e) => OrdTableKey (a,b,c,d,e)

instance (TableKey a, TableKey b, TableKey c, TableKey d, TableKey e, TableKey f)
    => TableKey (a,b,c,d,e,f) where
  toKeyBytes (x,y,c,d,e,f) = FDB.encodeTupleElems [FDB.Bytes (toKeyBytes x),
                                                FDB.Bytes (toKeyBytes y),
                                                FDB.Bytes (toKeyBytes c),
                                                FDB.Bytes (toKeyBytes d),
                                                FDB.Bytes (toKeyBytes e),
                                                FDB.Bytes (toKeyBytes f)]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode 6-tuple TableKey: " ++ show err
    Right [FDB.Bytes x, FDB.Bytes y, FDB.Bytes c, FDB.Bytes d, FDB.Bytes e, FDB.Bytes f] ->
      (fromKeyBytes x, fromKeyBytes y, fromKeyBytes c, fromKeyBytes d, fromKeyBytes e, fromKeyBytes f)
    Right _ -> error "Expected 6-tuple when decoding TableKey"

instance (TableKey a, TableKey b, TableKey c, TableKey d, TableKey e, TableKey f)
         => OrdTableKey (a,b,c,d,e,f)

instance PutIntLE Word8 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putWord8
  getIntLE = getWord8

instance PutIntLE Word16 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putWord16le
  {-# INLINABLE getIntLE #-}
  getIntLE = getWord16le

instance PutIntLE Word32 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putWord32le
  {-# INLINABLE getIntLE #-}
  getIntLE = getWord32le

instance PutIntLE Word64 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putWord64le
  {-# INLINABLE getIntLE #-}
  getIntLE = getWord64le

instance PutIntLE Int8 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putInt8
  {-# INLINABLE getIntLE #-}
  getIntLE = getInt8

instance PutIntLE Int16 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putInt16le
  {-# INLINABLE getIntLE #-}
  getIntLE = getInt16le

instance PutIntLE Int32 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putInt32le
  {-# INLINABLE getIntLE #-}
  getIntLE = getInt32le

instance PutIntLE Int64 where
  {-# INLINABLE putIntLE #-}
  putIntLE = putInt64le
  {-# INLINABLE getIntLE #-}
  getIntLE = getInt64le

instance PutIntLE a => TableSemigroup (Sum a) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (toStrict . runPut . putIntLE . getSum) Op.add table pid
  set = setVia (toStrict . runPut . putIntLE . getSum)
  get = getVia (Sum . runGet getIntLE . fromStrict)

instance PutIntLE a => RangeAccessibleTable (Sum a) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Sum .runGet getIntLE . fromStrict)

intToTupleBytes :: Integral a => a -> ByteString
intToTupleBytes x = FDB.encodeTupleElems [FDB.Int (toInteger x)]

tupleBytesToInt :: Num a => ByteString -> a
tupleBytesToInt bs = case FDB.decodeTupleElems bs of
  Left err -> error $ "Failed to decode tuple bytes as integer: " ++ show err
  Right [FDB.Int x] -> fromInteger x
  Right _ -> error "Expected int tuple in tupleBytesToInt"

instance TableSemigroup (Min Integer) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . getMin)
  get = getVia (Min . tupleBytesToInt)

instance RangeAccessibleTable (Min Integer) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . tupleBytesToInt)

instance TableSemigroup (Max Integer) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . getMax)
  get = getVia (Max . tupleBytesToInt)

instance RangeAccessibleTable (Max Integer) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . tupleBytesToInt)

instance TableSemigroup (Min Int8) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Int8) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Int8) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Int8) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Int16) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Int16) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Int16) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Int16) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Int32) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Int32) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Int32) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Int32) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Int64) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Int64) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Int64) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Int64) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Word8) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Word8) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Word8) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Word8) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Word16) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Word16) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Word16) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Word16) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Word32) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Word32) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Word32) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Word32) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

instance TableSemigroup (Min Word64) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMin) Op.byteMin table pid
  set = setVia (intToTupleBytes . toInteger . getMin)
  get = getVia (Min . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Min Word64) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . fromInteger . tupleBytesToInt)

instance TableSemigroup (Max Word64) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (intToTupleBytes . toInteger . getMax) Op.byteMax table pid
  set = setVia (intToTupleBytes . toInteger . getMax)
  get = getVia (Max . fromInteger . tupleBytesToInt)

instance RangeAccessibleTable (Max Word64) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . fromInteger . tupleBytesToInt)

doubleToTupleBytes :: Double -> ByteString
doubleToTupleBytes x = FDB.encodeTupleElems [FDB.Double x]

tupleBytesToDouble :: ByteString -> Double
tupleBytesToDouble bs = case FDB.decodeTupleElems bs of
  Left err -> error $ "Failed to decode bytes in tupleBytesToDouble: " ++ show err
  Right [FDB.Double x] -> x
  Right _ -> error "Unexpected bytes in tupleBytesToDouble"

floatToTupleBytes :: Float -> ByteString
floatToTupleBytes x = FDB.encodeTupleElems [FDB.Float x]

tupleBytesToFloat :: ByteString -> Float
tupleBytesToFloat bs = case FDB.decodeTupleElems bs of
  Left err -> error $ "Failed to decode bytes in tupleBytesToFloat: " ++ show err
  Right [FDB.Float x] -> x
  Right _ -> error "Unexpected bytes in tupleBytesToFloat"

instance TableSemigroup (Min Double) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (doubleToTupleBytes . getMin) Op.byteMin table pid
  set = setVia (doubleToTupleBytes . getMin)
  get = getVia (Min . tupleBytesToDouble)

instance RangeAccessibleTable (Min Double) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . tupleBytesToDouble)

instance TableSemigroup (Max Double) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (doubleToTupleBytes . getMax) Op.byteMax table pid
  set = setVia (doubleToTupleBytes . getMax)
  get = getVia (Max . tupleBytesToDouble)

instance RangeAccessibleTable (Max Double) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . tupleBytesToDouble)

instance TableSemigroup (Min Float) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (floatToTupleBytes . getMin) Op.byteMin table pid
  set = setVia (floatToTupleBytes . getMin)
  get = getVia (Min . tupleBytesToFloat)

instance RangeAccessibleTable (Min Float) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Min . tupleBytesToFloat)

instance TableSemigroup (Max Float) where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia (floatToTupleBytes . getMax) Op.byteMax table pid
  set = setVia (floatToTupleBytes . getMax)
  get = getVia (Max . tupleBytesToFloat)

instance RangeAccessibleTable (Max Float) where
  getTableRange table pid start end =
    getTableRangeVia table pid start end (Max . tupleBytesToFloat)

allToByte :: All -> ByteString
allToByte = toStrict . runPut . putWord8 . fromIntegral . fromEnum . getAll

allFromBytes :: ByteString -> All
allFromBytes = All . toEnum . fromIntegral . runGet getWord8 . fromStrict

instance TableSemigroup All where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia allToByte Op.bitAnd table pid
  set = setVia allToByte
  get = getVia allFromBytes

instance RangeAccessibleTable All where
  getTableRange table pid start end =
    getTableRangeVia table pid start end allFromBytes

anyToByte :: Any -> ByteString
anyToByte = toStrict . runPut . putWord8 . fromIntegral . fromEnum . getAny

anyFromBytes :: ByteString -> Any
anyFromBytes = Any . toEnum . fromIntegral . runGet getWord8 . fromStrict

instance TableSemigroup Any where
  mappendBatch table pid kvs =
    for_ kvs $ uncurry $ mappendAtomicVia anyToByte Op.bitOr table pid
  set = setVia anyToByte
  get = getVia anyFromBytes

instance RangeAccessibleTable Any where
  getTableRange table pid start end =
    getTableRangeVia table pid start end anyFromBytes
