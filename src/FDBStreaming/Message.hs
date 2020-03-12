{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A type class for serializing and deserializing stream messages.
module FDBStreaming.Message
  ( Message (..),
  )
where

import qualified Data.Binary as Binary
import Data.Binary.Get (getRemainingLazyByteString, getWord8, runGet)
import Data.Binary.Put (putByteString, putWord8, runPut)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Functor.Identity (Identity (Identity, runIdentity))
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Monoid (All, Any)
import Data.Semigroup (Dual (Dual, getDual), First (First, getFirst), Last (Last, getLast), Max (Max, getMax), Min (Min, getMin), Option (Option, getOption), Product (Product, getProduct), Sum (Sum, getSum))
import Data.Void (Void, absurd)
import Data.Word (Word16, Word32, Word64, Word8)
import FoundationDB.Layer.Tuple
  ( Elem (Bytes),
    decodeTupleElems,
    encodeTupleElems,
  )
import GHC.Natural (Natural)

-- TODO: error handling for bad parses? There is some benefit to throwing, in
-- that users don't need to care about it so much. Which works fine until they
-- write their own buggy instances.
-- TODO: Just use Store directly? Problem is that it literally doubles the
-- number of dependencies!!

-- | The class of all types that can be serialized and stored in FoundationDB.
-- While you may use any serialization you please, you may want to choose
-- something that allows for maximal forward and backward compatibility when
-- adding new fields to existing data types, such as protocol buffers. If that
-- is not a concern, the @store@ library is a good choice.
--
-- Remember that individual message size is limited by FoundationDB to 100 kB.
-- For this reason, we don't provide instances for collection types by default.
-- If you know that your collections will always be small, you can define your
-- own instances on newtypes.
--
-- A quick and easy way to create instances for your types is to use a library
-- like <https://hackage.haskell.org/package/store-0.7.2 store>. For example,
--
--
-- > {-# LANGUAGE DeriveGeneric #-}
-- > import qualified Data.Store as Store
-- > data User = User {name :: String, email :: String}
-- >  deriving(Show, Eq, Generic)
-- >
-- > instance Store User
--
-- > instance Message User where
-- >   toMessage = Store.encode
-- >   fromMessage = Store.decodeEx
--
-- Laws:
--
-- prop> fromMessage (toMessage x) == x
class Message a where

  -- | Serialize a message.
  toMessage :: a -> ByteString

  -- | Deserialize a message. Parse errors should just throw an exception.
  -- Exceptions are caught and handled by the loop associated with each worker
  -- thread.
  fromMessage :: ByteString -> a

instance Message Void where

  toMessage = absurd

  fromMessage = error "Can't parse message to Void"

instance Message () where

  toMessage = const ""

  fromMessage bs = if bs == "" then () else error "unexpected bytes decoding ()"

instance (Message a, Message b) => Message (a, b) where

  toMessage (x, y) = encodeTupleElems [Bytes (toMessage x), Bytes (toMessage y)]

  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes x, Bytes y] -> (fromMessage x, fromMessage y)
      Right xs -> error $ "unexpected decode " ++ show xs

instance (Message a, Message b, Message c) => Message (a, b, c) where

  toMessage (x, y, z) =
    encodeTupleElems
      [ Bytes (toMessage x),
        Bytes (toMessage y),
        Bytes (toMessage z)
      ]

  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes x, Bytes y, Bytes z] -> (fromMessage x, fromMessage y, fromMessage z)
      Right xs -> error $ "unexpected decode " ++ show xs

instance (Message a, Message b, Message c, Message d) => Message (a, b, c, d) where

  toMessage (a, b, c, d) =
    encodeTupleElems
      [ Bytes (toMessage a),
        Bytes (toMessage b),
        Bytes (toMessage c),
        Bytes (toMessage d)
      ]

  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes a, Bytes b, Bytes c, Bytes d] -> (fromMessage a, fromMessage b, fromMessage c, fromMessage d)
      Right xs -> error $ "unexpected decode " ++ show xs

instance (Message a, Message b, Message c, Message d, Message e) => Message (a, b, c, d, e) where

  toMessage (a, b, c, d, e) =
    encodeTupleElems
      [ Bytes (toMessage a),
        Bytes (toMessage b),
        Bytes (toMessage c),
        Bytes (toMessage d),
        Bytes (toMessage e)
      ]

  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes a, Bytes b, Bytes c, Bytes d, Bytes e] -> (fromMessage a, fromMessage b, fromMessage c, fromMessage d, fromMessage e)
      Right xs -> error $ "unexpected decode " ++ show xs

instance (Message a, Message b, Message c, Message d, Message e, Message f) => Message (a, b, c, d, e, f) where

  toMessage (a, b, c, d, e, f) =
    encodeTupleElems
      [ Bytes (toMessage a),
        Bytes (toMessage b),
        Bytes (toMessage c),
        Bytes (toMessage d),
        Bytes (toMessage e),
        Bytes (toMessage f)
      ]

  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes a, Bytes b, Bytes c, Bytes d, Bytes e, Bytes f] -> (fromMessage a, fromMessage b, fromMessage c, fromMessage d, fromMessage e, fromMessage f)
      Right xs -> error $ "unexpected decode " ++ show xs

instance (Message a, Message b, Message c, Message d, Message e, Message f, Message g) => Message (a, b, c, d, e, f, g) where

  toMessage (a, b, c, d, e, f, g) =
    encodeTupleElems
      [ Bytes (toMessage a),
        Bytes (toMessage b),
        Bytes (toMessage c),
        Bytes (toMessage d),
        Bytes (toMessage e),
        Bytes (toMessage f),
        Bytes (toMessage g)
      ]

  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes a, Bytes b, Bytes c, Bytes d, Bytes e, Bytes f, Bytes g] -> (fromMessage a, fromMessage b, fromMessage c, fromMessage d, fromMessage e, fromMessage f, fromMessage g)
      Right xs -> error $ "unexpected decode " ++ show xs

instance Message Bool where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Char where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Double where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Float where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Int where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Int8 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Int16 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Int32 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Int64 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Integer where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Natural where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Ordering where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Word8 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Word16 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Word32 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message Word64 where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance (Message a) => Message (Maybe a) where

  toMessage Nothing = toStrict $ runPut $ putWord8 0
  toMessage (Just x) = toStrict $ runPut (putWord8 1 >> putByteString (toMessage x))

  fromMessage x = flip runGet (fromStrict x) $
    getWord8 >>= \case
      0 -> return Nothing
      1 -> Just . fromMessage . toStrict <$> getRemainingLazyByteString
      _ -> error "Unexpected input to Maybe instance of Message"

instance Message Any where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message All where

  toMessage = toStrict . Binary.encode

  fromMessage = Binary.decode . fromStrict

instance Message a => Message (Min a) where

  toMessage = toMessage . getMin

  fromMessage = Min . fromMessage

instance Message a => Message (Max a) where

  toMessage = toMessage . getMax

  fromMessage = Max . fromMessage

instance Message a => Message (First a) where

  toMessage = toMessage . getFirst

  fromMessage = First . fromMessage

instance Message a => Message (Last a) where

  toMessage = toMessage . getLast

  fromMessage = Last . fromMessage

instance Message a => Message (Option a) where

  toMessage = toMessage . getOption

  fromMessage = Option . fromMessage

instance Message a => Message (Dual a) where

  toMessage = toMessage . getDual

  fromMessage = Dual . fromMessage

instance Message a => Message (Sum a) where

  toMessage = toMessage . getSum

  fromMessage = Sum . fromMessage

instance Message a => Message (Product a) where

  toMessage = toMessage . getProduct

  fromMessage = Product . fromMessage

instance (Message a, Message b) => Message (Either a b) where

  toMessage (Left x) = toStrict $ runPut $ putWord8 0 >> putByteString (toMessage x)
  toMessage (Right x) = toStrict $ runPut $ putWord8 1 >> putByteString (toMessage x)

  fromMessage x = flip runGet (fromStrict x) $
    getWord8 >>= \case
      0 -> Left . fromMessage . toStrict <$> getRemainingLazyByteString
      1 -> Right . fromMessage . toStrict <$> getRemainingLazyByteString
      _ -> error "Unexpected input to Either Message instance"

instance Message a => Message (Identity a) where

  toMessage = toMessage . runIdentity

  fromMessage = Identity . fromMessage
