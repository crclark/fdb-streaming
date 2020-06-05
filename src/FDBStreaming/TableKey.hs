{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Type classes for types that can be used as keys in aggregation tables and
-- indexes.
module FDBStreaming.TableKey (
  TableKey (..),
  OrdTableKey
)

 where

import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Versionstamp as FDB


-- | Class of types that can be serialized as table keys. This is distinct from
-- 'Message' to enable cases where the user may want to read entire ranges of
-- a table efficiently. In such cases, the serialized representation of the
-- key must have a lexicographic ordering equivalent to the 'Ord' instance of
-- the type. For the 'Message' class, we don't require any ordering of the
-- serialized representation, which may admit for more efficient serialization
-- in some cases.
--
-- @fromKeyBytes . toKeyBytes = id@
class TableKey a where
  toKeyBytes :: a -> ByteString
  fromKeyBytes :: ByteString -> a

-- | An additional predicate for types that satisfy
-- @compare x y == compare (toKeyBytes x) (toKeyBytes y)@
class (Ord a, TableKey a) => OrdTableKey a


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

instance (Ord a, Ord b, TableKey a, TableKey b) => OrdTableKey (a,b)

instance (TableKey a, TableKey b, TableKey c) => TableKey (a,b,c) where
  toKeyBytes (x,y,c) = FDB.encodeTupleElems [FDB.Bytes (toKeyBytes x),
                                             FDB.Bytes (toKeyBytes y),
                                             FDB.Bytes (toKeyBytes c)]
  fromKeyBytes bs = case FDB.decodeTupleElems bs of
    Left err -> error $ "Failed to decode 3-tuple TableKey: " ++ show err
    Right [FDB.Bytes x, FDB.Bytes y, FDB.Bytes c] -> (fromKeyBytes x, fromKeyBytes y, fromKeyBytes c)
    Right _ -> error "Expected 3-tuple when decoding TableKey"

instance (Ord a, Ord b, Ord c, TableKey a, TableKey b, TableKey c) => OrdTableKey (a,b,c)

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

instance (Ord a, Ord b, Ord c, Ord d, TableKey a, TableKey b, TableKey c, TableKey d) => OrdTableKey (a,b,c,d)

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

instance (Ord a, Ord b, Ord c, Ord d, Ord e, TableKey a, TableKey b, TableKey c, TableKey d, TableKey e) => OrdTableKey (a,b,c,d,e)

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

instance (Ord a, Ord b, Ord c, Ord d, Ord e, Ord f, TableKey a, TableKey b, TableKey c, TableKey d, TableKey e, TableKey f)
         => OrdTableKey (a,b,c,d,e,f)

