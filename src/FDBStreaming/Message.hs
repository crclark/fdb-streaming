module FDBStreaming.Message where


import Data.ByteString (ByteString)
import Data.Void (Void)
import FoundationDB.Layer.Tuple (decodeTupleElems,
                                 encodeTupleElems,
                                 Elem(Bytes))

-- TODO: error handling for bad parses
class Message a where
  toMessage :: a -> ByteString
  fromMessage :: ByteString -> a

-- TODO: argh, find a way to avoid this.
instance Message Void where
  toMessage = error "impossible happened: called toMessage on Void"
  fromMessage = error "impossible happened: called fromMessage on Void"

instance (Message a, Message b) => Message (a,b) where
  toMessage (x,y) = encodeTupleElems [Bytes (toMessage x), Bytes (toMessage y)]
  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes x, Bytes y] -> (fromMessage x, fromMessage y)
      Right xs -> error $ "unexpected decode " ++ show xs
