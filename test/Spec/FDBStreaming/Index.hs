{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Spec.FDBStreaming.Index
  ( indexTests,
  )
where

import Control.Monad ((>=>), void)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Foldable (for_)
import Data.Maybe (catMaybes, fromJust)
import Data.Sequence (Seq)
import Data.Traversable (for)
import Data.Word (Word64)
import qualified FDBStreaming.Index as Index
import FDBStreaming.TableKey (OrdTableKey, TableKey)
import FDBStreaming.Topic (makeTopic, writeTopic)
import FDBStreaming.Topic (Topic)
import qualified FDBStreaming.Topic as Topic
import qualified FoundationDB as FDB
import FoundationDB (Database, await, runTransaction)
import FoundationDB.Layer.Subspace (Subspace)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import qualified FoundationDB.Versionstamp as FDB
import Spec.FDBStreaming.Util (extendRand)
import qualified Streamly.Prelude as S
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

getRawKeyRange ::
  (Eq k, TableKey k) =>
  Index.Index k ->
  k ->
  FDB.Transaction (Seq (ByteString, ByteString))
getRawKeyRange ix k =
  FDB.getEntireRange (Index.coordinateRangeForKey ix k)

getOne ::
  (Eq k, TableKey k) =>
  Index.Index k ->
  k ->
  FDB.Transaction (Maybe (k, Topic.Coordinate))
getOne ix k = do
  let r = (Index.coordinateRangeForKey ix k) {FDB.rangeLimit = Just 1}
  s <- Index.coordinateRangeStream ix r
  S.the s

getOneOrCrash ::
  (Eq k, TableKey k) =>
  Index.Index k ->
  k ->
  FDB.Transaction Topic.Coordinate
getOneOrCrash ix k = do
  x <- getOne ix k
  return $ fromJust (fmap snd x)

getAll ::
  (Eq k, TableKey k) =>
  Index.Index k ->
  k ->
  FDB.Transaction [Topic.Coordinate]
getAll ix k = do
  let r = Index.coordinateRangeForKey ix k
  s <- Index.coordinateRangeStream ix r
  fmap snd <$> S.toList s

getAllBetween ::
  (OrdTableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  k ->
  IO [(k, Topic.Coordinate)]
getAllBetween db ix k1 k2 = runTransaction db $ do
  let r = Index.coordinateRangeForKeyRange ix k1 k2
  s <- Index.coordinateRangeStream ix r
  S.toList s

getCount ::
  (Eq k, TableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  IO Word64
getCount db ix k = runTransaction db $ Index.countForKey ix k >>= await

getAllCounts ::
  (OrdTableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  k ->
  IO [(k, Word64)]
getAllCounts db ix k1 k2 = runTransaction db $ do
  let r = Index.countForKeysRange ix k1 k2
  s <- Index.countForKeysStream ix r
  S.toList s

getAllTopic ::
  (Eq k, TableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  Topic ->
  IO [ByteString]
getAllTopic db ix k t = runTransaction db $ do
  coords <- getAll ix k
  catMaybes <$> for coords (Topic.get t >=> await)

indexTest :: Subspace -> Database -> TestTree
indexTest testSS db = testCase "index" $ do
  ss <- extendRand testSS
  let coord =
        Topic.CoordinateUncommitted
          minBound
          (FDB.IncompleteVersionstamp minBound)
          minBound
  let (ix :: Index.Index ByteString) =
        Index.Index (FDB.extend ss [FDB.Bytes "index"]) "index"
  runTransaction db $ Index.index ix "test" coord
  coordCommitted <- runTransaction db $ getOne ix "test"
  let Just (k, Topic.Coordinate pid (FDB.CompleteVersionstamp _ u) i) = coordCommitted
  k @?= "test"
  pid @?= minBound
  u @?= minBound
  i @?= minBound

indexTopic :: Subspace -> Database -> TestTree
indexTopic testSS db = testCase "index" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2 0
  let (ix :: Index.Index BS.ByteString) = Index.namedIndex topic "testIndex"
  let writeAndIndex pid xs =
        runTransaction db $ do
          coords <- writeTopic topic pid xs
          for_ (zip coords xs) $ \(coord, x) -> Index.index ix x coord
  writeAndIndex 0 ["1", "2", "3"]
  writeAndIndex 1 ["4", "5", "6"]
  x1 <- runTransaction db $ getOneOrCrash ix "1" >>= Topic.get topic >>= await
  x2 <- runTransaction db $ getOneOrCrash ix "2" >>= Topic.get topic >>= await
  x3 <- runTransaction db $ getOneOrCrash ix "3" >>= Topic.get topic >>= await
  x4 <- runTransaction db $ getOneOrCrash ix "4" >>= Topic.get topic >>= await
  x5 <- runTransaction db $ getOneOrCrash ix "5" >>= Topic.get topic >>= await
  x6 <- runTransaction db $ getOneOrCrash ix "6" >>= Topic.get topic >>= await
  Just "1" @?= x1
  Just "2" @?= x2
  Just "3" @?= x3
  Just "4" @?= x4
  Just "5" @?= x5
  Just "6" @?= x6

data Event = Event {payload :: ByteString, index :: ByteString}
  deriving (Eq, Show)

indexMany :: Subspace -> Database -> TestTree
indexMany testSS db = testCase "indexMany" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2 2048
  let (ix :: Index.Index BS.ByteString) = Index.namedIndex topic "testIndex"
  let writeAndIndex pid xs =
        runTransaction db $ do
          coords <- writeTopic topic pid (map payload xs)
          for_ (zip coords xs) $ \(coord, x) -> Index.index ix (index x) coord
  writeAndIndex
    0
    [ Event "hello" "1",
      Event "world" "1",
      Event "abc" "2"
    ]
  writeAndIndex
    1
    [ Event "def" "2",
      Event "foo" "1"
    ]
  writeAndIndex
    0
    [ Event "bar" "3",
      Event "baz" "2"
    ]
  xs1 <- getAllTopic db ix "1" topic
  xs1 @?= ["hello", "world", "foo"]
  xs2 <- getAllTopic db ix "2" topic
  xs2 @?= ["abc", "def", "baz"]
  xs3 <- getAllTopic db ix "3" topic
  xs3 @?= ["bar"]
  c1 <- getCount db ix "1"
  c1 @?= 3
  c2 <- getCount db ix "2"
  c2 @?= 3
  c3 <- getCount db ix "3"
  c3 @?= 1
  c4 <- getCount db ix "4"
  c4 @?= 0
  cs <- getAllCounts db ix "1" "2"
  cs @?= [("1", 3), ("2", 3)]
  cs2 <- getAllCounts db ix "1" "4"
  cs2 @?= [("1", 3), ("2", 3), ("3", 1)]
  kcs <- getAllBetween db ix "1" "2"
  -- We wrote three "1" keys and three "2" keys
  fmap fst kcs @?= ["1", "1", "1", "2", "2", "2"]

indexCommitted :: Subspace -> Database -> TestTree
indexCommitted testSS db = testCase "indexCommitted" $ do
  ss <- extendRand testSS
  let topic = makeTopic ss "test" 2 2048
  let (ix :: Index.Index BS.ByteString) = Index.namedIndex topic "testIndex"
  let write pid xs = void $ runTransaction db $ writeTopic topic pid xs
  let readPid pid =
        fmap (\(c, x) -> (Topic.checkpointToCoordinate pid c, x))
          <$> Topic.peekNPastCheckpoint topic pid "x" 100
  let writeIndices cks = runTransaction db $ for_ cks $
        \(c, k) -> Index.indexCommitted ix k c
  write 0 ["1", "2", "3"]
  write 1 ["4", "5", "6"]
  p0 <- runTransaction db $ readPid 0
  p1 <- runTransaction db $ readPid 1
  writeIndices p0
  writeIndices p1
  kcs <- fmap fst <$> getAllBetween db ix "1" "7"
  kcs @?= ["1", "2", "3", "4", "5", "6"]

indexTests :: Subspace -> Database -> TestTree
indexTests testSS db =
  testGroup
    "Index"
    [ indexTest testSS db,
      indexTopic testSS db,
      indexMany testSS db,
      indexCommitted testSS db
    ]
