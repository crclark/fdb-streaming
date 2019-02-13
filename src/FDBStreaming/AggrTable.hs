{-# LANGUAGE FlexibleInstances #-}

module FDBStreaming.AggrTable where

import FDBStreaming.Message (Message(..))

import Data.Binary.Get (runGet,
                        getWord8,
                        getWord16le,
                        getWord32le,
                        getWord64le,
                        getInt32le,
                        getInt16le,
                        getInt64le)
import Data.Binary.Put (runPut,
                        putWord8,
                        putWord16le,
                        putWord32le,
                        putWord64le,
                        putInt16le,
                        putInt32le,
                        putInt64le)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Int (Int8, Int16, Int32, Int64)
import Data.Monoid (Sum(..))
import Data.Word (Word8, Word16, Word32, Word64)

import qualified FoundationDB as FDB
import qualified FoundationDB.Options as Op
import qualified FoundationDB.Layer.Subspace as SS
import FoundationDB.Layer.Tuple

newtype AggrTable k v = AggrTable {
  aggrTableSS :: SS.Subspace
}

-- TODO: get is partial
class TableValue v where
  set :: Message k => AggrTable k v -> k -> v -> FDB.Transaction ()
  get :: Message k => AggrTable k v -> k -> FDB.Transaction (FDB.Future (Maybe v))

class (Semigroup v, TableValue v) => TableSemigroup v where
  -- | mappends to the existing value at k in the table. If k has not been set,
  -- sets k to the provided value instead. Some implementations may use
  -- atomic FoundationDB operations to improve performance.
  mappendTable :: Message k => AggrTable k v -> k -> v -> FDB.Transaction ()

-- | Helper function to define 'TableValue.set' easily.
setVia :: Message k
       => (v -> ByteString) -> AggrTable k v -> k -> v -> FDB.Transaction ()
setVia f t k v = do
  let kbs = SS.pack (aggrTableSS t) [Bytes (toMessage k)]
  let vbs = f v
  FDB.set kbs vbs

getVia :: Message k
       => (ByteString -> v)
       -> AggrTable k v
       -> k
       -> FDB.Transaction (FDB.Future (Maybe v))
getVia f t k = do
  let kbs = SS.pack (aggrTableSS t) [Bytes (toMessage k)]
  fmap (fmap f) <$> FDB.get kbs

mappendAtomicVia :: Message k
                 => (v -> ByteString)
                 -> (ByteString -> Op.MutationType)
                 -> AggrTable k v
                 -> k
                 -> v
                 -> FDB.Transaction ()
mappendAtomicVia f op t k v = do
  let kbs = SS.pack (aggrTableSS t) [Bytes (toMessage k)]
  let vbs = f v
  FDB.atomicOp kbs (op vbs)

instance TableValue (Sum Int64) where
  set = setVia (toStrict . runPut . putInt64le . getSum)
  get = getVia (Sum . runGet getInt64le . fromStrict)

instance TableSemigroup (Sum Int64) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putInt64le . getSum) Op.add

instance TableValue (Sum Int32) where
  set = setVia (toStrict . runPut . putInt32le . getSum)
  get = getVia (Sum . runGet getInt32le . fromStrict)

instance TableSemigroup (Sum Int32) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putInt32le . getSum) Op.add

instance TableValue (Sum Int16) where
  set = setVia (toStrict . runPut . putInt16le . getSum)
  get = getVia (Sum . runGet getInt16le . fromStrict)

instance TableSemigroup (Sum Int16) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putInt16le . getSum) Op.add

instance TableValue (Sum Int8) where
  set = setVia (toStrict . runPut . putWord8 . fromIntegral . getSum)
  get = getVia (Sum . fromIntegral . runGet getWord8 . fromStrict)

instance TableSemigroup (Sum Int8) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putWord8 . fromIntegral . getSum)
                     Op.add

instance TableValue (Sum Word64) where
  set = setVia (toStrict . runPut . putWord64le . getSum)
  get = getVia (Sum . runGet getWord64le . fromStrict)

instance TableSemigroup (Sum Word64) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putWord64le . getSum) Op.add

instance TableValue (Sum Word32) where
  set = setVia (toStrict . runPut . putWord32le . getSum)
  get = getVia (Sum . runGet getWord32le . fromStrict)

instance TableSemigroup (Sum Word32) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putWord32le . getSum) Op.add

instance TableValue (Sum Word16) where
  set = setVia (toStrict . runPut . putWord16le . getSum)
  get = getVia (Sum . runGet getWord16le . fromStrict)

instance TableSemigroup (Sum Word16) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putWord16le . getSum) Op.add

instance TableValue (Sum Word8) where
  set = setVia (toStrict . runPut . putWord8 . getSum)
  get = getVia (Sum . runGet getWord8 . fromStrict)

instance TableSemigroup (Sum Word8) where
  mappendTable =
    mappendAtomicVia (toStrict . runPut . putWord8 . getSum) Op.add
