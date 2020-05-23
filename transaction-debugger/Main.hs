{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Tools for parsing json transaction traces to try to find conflicts. Not a
-- general purpose tool because it tries to parse fdb-streaming keys into
-- readable formats.
--
-- This is a collection of horrible hacks because I intend to throw it all away
-- when FDB 6.3 is released with its new features for getting conflicting keys.
module Main where

import Control.Applicative ((<|>))
import Control.Monad (forM_, void)

import Data.Aeson (Value(..))
import qualified Data.Aeson as A
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Function ((&))
import Data.Hashable (Hashable)
import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromJust, isJust)
import Data.Word (Word8)

import Data.Attoparsec.ByteString.Char8 (Parser, parseOnly)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Encoding as T

import System.IO (stdin)

import qualified Streamly.Prelude as S

import qualified Text.Parser.Char as P
import qualified Text.Parser.Combinators as P
import qualified Text.Parser.LookAhead as P
import Text.Regex.Posix ((=~))

import qualified FoundationDB.Layer.Tuple as FDB

import Data.Either

import Debug.Trace (trace)

import Safe (readMay)
import Data.Ranged.RangedSet (RSet)
import qualified Data.Ranged.RangedSet as RS
import qualified Data.Ranged.Boundaries as RS
import qualified Data.Ranged.Ranges as RS

import Text.Pretty.Simple (pShow)

instance RS.DiscreteOrdered ByteString where
  adjacent x y = RS.adjacent (B.unpack x) (B.unpack y)
  adjacentBelow x = Just $  predBS x

-- | Returns the lexicographically prior bytestring. Probably slow.
predBS :: ByteString -> ByteString
predBS "" = ""
predBS xs = if B.all (== toEnum 0x00) xs
               then B.init xs
               else B.init xs <> B.pack [pred (B.last xs)]

-- | Returns the lexicographically next bytestring. Probably slow.
succBS :: ByteString -> ByteString
succBS "" = "\x00"
succBS xs = if B.all (== toEnum 0xff) xs
               then xs <> "\x00"
               else B.init xs <> B.pack [succ (B.last xs)]

inclusiveRange :: ByteString -> ByteString -> RS.Range ByteString
inclusiveRange x y =
  RS.Range (RS.BoundaryBelow x)
           (RS.BoundaryAbove y)

-- | The raw strings in the logs use double backspaces and hex for bytes, so
-- they often look like "\\xff\\xfffoobarbaz". This hopefully parses these into
-- Haskell ByteStrings correctly. Expect horrible hacks.
--parseToByteString :: (P.LookAheadParsing m, P.CharParsing m) => m ByteString
parseToByteString' :: Parser ByteString
parseToByteString' = do
  chunks <- P.many (hexByte <|> otherString)
  P.eof
  return $ BL.toStrict $ BB.toLazyByteString (mconcat chunks)

toBS :: Text -> ByteString
toBS t = case parseOnly parseToByteString' (T.encodeUtf8 t) of
  Left err -> error ("parse failed: " ++ show err)
  Right x -> x

hexByte :: (P.CharParsing m, Monad m) => m BB.Builder
hexByte = do
  _ <- P.text "\\\\x"
  digit1 <- P.hexDigit
  digit2 <- P.hexDigit
  let w = read @Word8 ['0', 'x', digit1, digit2]
  return $ BB.word8 w

otherString ::(P.LookAheadParsing m, P.CharParsing m, Monad m) => m BB.Builder
otherString = do
  cs <- P.manyTill P.anyChar (P.lookAhead (void hexByte) <|> (P.lookAhead P.eof))
  return $ BB.byteString (B.pack cs)

-- Above parser combinator implementation loops forever and I can't find why.
-- Below uses regex instead.

splitOnHexByte :: String -> (String, String, String)
splitOnHexByte t = t =~ ("\\\\x.." :: String)

toBSRegex :: Text -> ByteString
toBSRegex t =
  {-
  trace ("running toBSRegex on " ++ show (T.unpack t)
                     ++ " first char is " ++ show (fromEnum (head $ T.unpack t))
                     ++ " first four chars are " ++ show (map fromEnum (take 4 $ T.unpack t))) $
  trace ("got toBSRegex output: " ++ show result)
  -}
  result
  where
    result = BL.toStrict $ BB.toLazyByteString $ go (T.unpack t) mempty

    go [] bb = bb
    go s bb = case splitOnHexByte s of
      (str, [], []) -> bb <> pack str
      (s1, hex, s2) -> go s2 (bb <> pack s1 <> toHex hex)

    pack = BB.byteString . B.pack
    toHex s = case readMay @Word8 ("0x" ++ (drop 2 s)) of
      Just w -> BB.word8 w
      -- TODO: trace log something
      Nothing -> error $ "failed to hexify: " ++ show s ++ " input was: " ++ show (drop 2 s) --BB.byteString $ B.pack s



data Keys = Key ByteString | KeyRange ByteString ByteString
  deriving (Show, Eq)

{-
toRSet :: Keys -> RSet ByteString
toRSet (Key bs) = RS.rSingleton bs
toRSet (KeyRange b c) = RSet.singletonRange (b,c)
-}
-- TODO: this errors out for some reason. Extra bytes at the end of keys.
toTuples :: Keys -> [[FDB.Elem]]
toTuples (Key bs) =case FDB.decodeTupleElems bs of
  Left err -> (error $ "toTuples failed on: " ++ show bs ++ " with error " ++ show err)
  Right x -> [x]
toTuples (KeyRange bs1 bs2) = toTuples (Key bs1) ++ toTuples (Key bs2)

data AtomicOpType = SetVersionstamp
  | Add
  | Max
  | SetValue
  -- ^ I think this is a blind write, but not sure
  | Other T.Text
  deriving (Show, Eq)

data TransactionType = Get | Set | AtomicOp AtomicOpType [ByteString]
  deriving (Show, Eq)

-- | Extremely simplified transaction trace type
data TransactionTrace = TransactionTrace {
  keys :: Keys,
  transactionId :: ByteString,
  transactionType :: TransactionType,
  failed :: Bool
  -- ^ True if we got CommitError transaction types for this transactiontrace
} deriving (Show, Eq)

data TransactionSummary = TransactionSummary {
  transactionSummaryId :: ByteString,
  transactionSummaryFailed :: Bool,
  writeKeys :: RSet ByteString,
  readKeys :: RSet ByteString,
  atomicWriteKeys :: RSet ByteString
} deriving (Show, Eq)

toRSet :: Keys -> RSet ByteString
toRSet (Key k) = RS.makeRangedSet [inclusiveRange k k]
toRSet (KeyRange k1 k2) = RS.makeRangedSet [inclusiveRange k1 k2]

instance Semigroup TransactionSummary where
  x <> y = TransactionSummary {
    transactionSummaryId = transactionSummaryId x
    , transactionSummaryFailed = transactionSummaryFailed x || transactionSummaryFailed y
    , writeKeys = writeKeys x <> writeKeys y
    , readKeys = readKeys x <> readKeys y
    , atomicWriteKeys = atomicWriteKeys x <> atomicWriteKeys y
  }

summarize :: TransactionTrace -> TransactionSummary
summarize tt = case transactionType tt of
  Get -> TransactionSummary {
    transactionSummaryId = transactionId tt
    , transactionSummaryFailed = failed tt
    , writeKeys = mempty
    , readKeys = toRSet $ keys tt
    , atomicWriteKeys = mempty
  }

  Set -> TransactionSummary {
    transactionSummaryId = transactionId tt
    , transactionSummaryFailed = failed tt
    , writeKeys = toRSet $ keys tt
    , readKeys = mempty
    , atomicWriteKeys = mempty
  }

  AtomicOp _ [k] -> TransactionSummary {
    transactionSummaryId = transactionId tt
    , transactionSummaryFailed = failed tt
    , writeKeys = mempty
    , readKeys = mempty
    , atomicWriteKeys = toRSet (Key k)
  }

  AtomicOp _ [k1, k2] -> TransactionSummary {
    transactionSummaryId = transactionId tt
    , transactionSummaryFailed = failed tt
    , writeKeys = mempty
    , readKeys = mempty
    , atomicWriteKeys = toRSet (KeyRange k1 k2)
  }

  x -> error $ "unexpected summarize input: " ++ show x

-- | Not symmetric. Returns keys that would cause the writes of ts1 to cause
-- ts2 to fail to commit. Returns nothing if no conflict
conflictKeys :: TransactionSummary -> TransactionSummary -> Maybe (RSet ByteString)
conflictKeys ts1 ts2 =
  let writes = writeKeys ts1 <> atomicWriteKeys ts1
      conflicts = RS.rSetIntersection writes (readKeys ts2)
      in if RS.rSetIsEmpty conflicts then Nothing else Just conflicts

toAtomicOpType :: Text -> AtomicOpType
toAtomicOpType "SetValue" = SetValue
toAtomicOpType "AddValue" = Add
toAtomicOpType "ByteMax" = Max
toAtomicOpType x = Other x

getBS :: HM.HashMap Text Value -> Text -> ByteString
getBS obj k = case obj !? k of
   (A.String t) -> toBSRegex t
   x -> error $ "getBS error: " ++ show x

parseAtomicOp :: Value -> TransactionType
parseAtomicOp (A.String mutation) =
  case parseOnly p (T.encodeUtf8 mutation) of
    Left err -> error ("parseAtomicOp': " ++ show err)
    Right x -> x

  where
    p = do
      void $ P.string "code: "
      code <- P.manyTill P.anyChar (P.try (P.string " param1: "))
      param1 <- P.manyTill P.anyChar (P.try (P.string " param2: "))
      param2 <- P.many P.anyChar
      return $ AtomicOp (toAtomicOpType (T.pack code))
                        [ toBSRegex (T.pack param1)
                        , toBSRegex (T.pack param2)]

parseAtomicOp x = error $ "unexpected input to parseAtomicOp': " ++ show x

atomicOpKey :: TransactionType -> ByteString
atomicOpKey (AtomicOp _ (k:_)) = k
atomicOpKey x = error $ "gibberish passed to atomicOpKey " ++ show x

(!?) :: (Show k, Show v, Eq k, Hashable k) => HM.HashMap k v -> k -> v
(!?) hm k = case HM.lookup k hm of
  Nothing -> error $ "key " ++ show k ++ " not found in " ++ show hm
  Just v -> v

toTrace :: Value -> Maybe TransactionTrace
toTrace (Object obj) = case obj !? "Type" of
  -- The only ones we care about are the transactions that errored out, and
  -- those trace documents have "CommitError" in the type.
  -- TODO: we will probably want the timestamps ("Time" field), too, so that we
  -- can compare transactions that occurred near each other. We can use the
  -- TransactionTrace_GetVersion to get the approximate start time of the
  -- transaction and TransactionTrace_CommitError to get the approximate stop
  -- time.
  A.String "TransactionTrace_CommitError_Mutation" -> Just $
    let op = parseAtomicOp (obj !? "Mutation")
        k = atomicOpKey op
        in TransactionTrace (Key k)
                            (getBS obj "TransactionID")
                            op
                            True
  A.String "TransactionTrace_CommitError_ReadConflictRange" -> Just $
    TransactionTrace (KeyRange (getBS obj "Begin") (getBS obj "End"))
                     (getBS obj "TransactionID")
                     Get
                     True
  A.String "TransactionTrace_CommitError_WriteConflictRange" -> Just $
    TransactionTrace (KeyRange (getBS obj "Begin") (getBS obj "End"))
                     (getBS obj "TransactionID")
                     Set
                     True
  A.String "TransactionTrace_Mutation" -> Just $
    let op = parseAtomicOp (obj !? "Mutation")
        k = atomicOpKey op
        in TransactionTrace (Key k)
                            (getBS obj "TransactionID")
                            op
                            False
  A.String "TransactionTrace_ReadConflictRange" -> Just $
    TransactionTrace (KeyRange (getBS obj "Begin") (getBS obj "End"))
                     (getBS obj "TransactionID")
                     Get
                     False
  A.String "TransactionTrace_WriteConflictRange" -> Just $
    TransactionTrace (KeyRange (getBS obj "Begin") (getBS obj "End"))
                     (getBS obj "TransactionID")
                     Set
                     False
  A.String _ -> Nothing
  x -> error $ "unexpected type of Type: " ++ show x

toTrace _ = Nothing

main :: IO ()
main = do
  Just traces <- S.fromHandle stdin -- String lines
                  & fmap BL.pack -- to lazy bytestrings
                  & S.mapMaybe A.decode -- to untyped JSON
                  & S.mapMaybe toTrace -- to TransactionTraces
                  & fmap summarize -- to TransactionSummaries
                  & fmap (\ts -> HM.singleton (transactionSummaryId ts) ts)
                  & S.foldl1' (HM.unionWith (<>))
  let summaries = fmap snd $ HM.toList traces
  let potentialConflicts = [( id1
                            , id2
                            , fromJust cKeys)

                            | t1 <- summaries
                            , t2 <- summaries
                            , let cKeys = conflictKeys t1 t2
                            , isJust cKeys
                            , let id1 = transactionSummaryId t1
                            , let id2 = transactionSummaryId t2
                            , id1 /= id2
                            , transactionSummaryFailed t2]
  forM_ potentialConflicts $ \c -> putStrLn (show c)
  putStrLn "### debug miscellaneous stuff"
  let fijointr = traces HM.! "f_i_join1_cff85bdf"
  putStrLn $ show fijointr
  putStrLn $ show $ RS.rSetHas (writeKeys fijointr) ("\SOHss1356\NUL\DC4\SOHincoming_orders\NUL\NAK\ETX\SOHfinal_join1\NUL\NAK\EOT\NAK\SOH" :: ByteString)
