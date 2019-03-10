-- | Reusable constants for short tuple keys
module FDBStreaming.Topic.Constants where

import FoundationDB.Layer.Tuple

topics :: Elem
topics = Int 0

messages :: Elem
messages = Int 1

metaCount :: Elem
metaCount = Int 2

readers :: Elem
readers = Int 3

checkpoint :: Elem
checkpoint = Int 4

aggrTable :: Elem
aggrTable = Int 5

oneToOneJoin :: Elem
oneToOneJoin = Int 6
