{-# LANGUAGE OverloadedStrings #-}

module FDBStreaming.Message (Message(..)) where


import           Data.ByteString                ( ByteString )
import           Data.Void                      (Void, absurd)
import           FoundationDB.Layer.Tuple       ( decodeTupleElems
                                                , encodeTupleElems
                                                , Elem(Bytes)
                                                )

-- TODO: error handling for bad parses? There is some benefit to throwing, in
-- that users don't need to care about it so much. Which works fine until they
-- write their own buggy instances.
-- TODO: Just use Store directly?
class Message a where
  toMessage :: a -> ByteString
  fromMessage :: ByteString -> a

instance Message Void where
  toMessage = absurd
  fromMessage = error "Can't parse message to Void"

instance Message () where
  toMessage = const ""
  fromMessage bs = if bs == "" then () else error "unexpected bytes decoding ()"

instance (Message a, Message b) => Message (a,b) where
  toMessage (x,y) = encodeTupleElems [Bytes (toMessage x), Bytes (toMessage y)]
  fromMessage bs =
    case decodeTupleElems bs of
      Left err -> error $ "bad tuple decode " ++ show err
      Right [Bytes x, Bytes y] -> (fromMessage x, fromMessage y)
      Right xs -> error $ "unexpected decode " ++ show xs
