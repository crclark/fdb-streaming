{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Spec.FDBStreaming
  ( jobTests,
  )
where

import Control.Monad ((>=>))
import Data.ByteString (ByteString)
import Data.Either (fromRight)
import Data.Maybe (catMaybes)
import Data.Persist (Persist)
import qualified Data.Persist as Persist
import qualified Data.Set as Set
import Data.Traversable (for)
import FDBStreaming (Index, Message (fromMessage, toMessage), MonadStream, Stream, StreamPersisted (FDB), indexBy, pipe', run, streamTopic, oneToOneJoin)
import qualified FDBStreaming.Index as Index
import qualified FDBStreaming.JobConfig as JC
import FDBStreaming.TableKey (TableKey)
import FDBStreaming.Testing (testJobConfig, testOnInput, testOnInput2, dumpStream)
import FDBStreaming.Topic (Topic)
import qualified FDBStreaming.Topic as Topic
import qualified FoundationDB as FDB
import FoundationDB (Database, await, runTransaction)
import FoundationDB.Layer.Subspace (Subspace)
import GHC.Generics
import Spec.FDBStreaming.Util (extendRand)
import qualified Streamly.Prelude as S
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

data TestMsg = TestMsg {payload :: ByteString, k1 :: ByteString, k2 :: ByteString}
  deriving (Eq, Show, Ord, Generic, Persist)

instance Message TestMsg where

  toMessage = Persist.encode

  fromMessage = fromRight (error "Failed to decode TestMsg") . Persist.decode

indexGetAll ::
  (TableKey k) =>
  Index.Index k ->
  k ->
  FDB.Transaction [Topic.Coordinate]
indexGetAll ix k = do
  let r = Index.coordinateRangeForKey ix k
  s <- Index.coordinateRangeStream ix r
  fmap snd <$> S.toList s

indexGetAllTopic ::
  (TableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  Topic ->
  IO [ByteString]
indexGetAllTopic db ix k t = runTransaction db $ do
  coords <- indexGetAll ix k
  catMaybes <$> for coords (Topic.get t >=> await)

ixJob ::
  forall m.
  MonadStream m =>
  Stream 'FDB TestMsg ->
  m (Index ByteString, (Index ByteString, Stream 'FDB TestMsg))
ixJob input =
  do
    run "out"
    $ indexBy "outix1" (return . k1)
    $ indexBy "outix2" (return . k2)
    $ pipe' input (\msg -> return (Just msg {payload = payload msg <> "1"}))

indexTest :: Subspace -> Database -> TestTree
indexTest testSS db = testCase "index job" $ do
  ss <- extendRand testSS
  let testInputs =
        [ TestMsg "hi" "1" "2",
          TestMsg "bye" "1" "3",
          TestMsg "hihi" "2" "2"
        ]
  (ix1, (ix2, outStream)) <- testOnInput (testJobConfig db ss) testInputs ixJob
  let outTopic = streamTopic outStream
  msgs11 <- fmap fromMessage <$> indexGetAllTopic db ix1 "1" outTopic
  fmap payload msgs11
    @?= [ "hi1" :: ByteString,
          "bye1"
        ]
  msgs12 <- fmap fromMessage <$> indexGetAllTopic db ix1 "2" outTopic
  fmap payload msgs12 @?= ["hihi1" :: ByteString]
  msgs13 <- fmap fromMessage <$> indexGetAllTopic db ix1 "3" outTopic
  fmap payload msgs13 @?= []
  msgs22 <- fmap fromMessage <$> indexGetAllTopic db ix2 "2" outTopic
  fmap payload msgs22
    @?= [ "hi1" :: ByteString,
          "hihi1"
        ]
  msgs23 <- fmap fromMessage <$> indexGetAllTopic db ix2 "3" outTopic
  fmap payload msgs23 @?= ["bye1" :: ByteString]

oneToOneSelfJoinJob ::
  forall m. MonadStream m =>
  Stream 'FDB TestMsg ->
  m (Stream 'FDB (TestMsg, TestMsg))
oneToOneSelfJoinJob input =
  oneToOneJoin "out" input input payload payload (,)

oneToOneSelfJoinTest :: Subspace -> Database -> TestTree
oneToOneSelfJoinTest testSS db = testCase "oneToOneSelfJoin" $ do
  ss <- extendRand testSS
  let testInputs = [ TestMsg "joinKey1" "1" "2"
                   , TestMsg "joinKey2" "5" "6"
                   , TestMsg "joinKey3" "5" "6"]
  outStream <- testOnInput (testJobConfig db ss) testInputs oneToOneSelfJoinJob
  results <- Set.fromList <$> dumpStream db outStream
  length results @?= 3
  Set.map (payload . fst) results @?= ["joinKey1", "joinKey2", "joinKey3"]

oneToOneJoinJob ::
  forall m. MonadStream m =>
  Stream 'FDB TestMsg ->
  Stream 'FDB TestMsg ->
  m (Stream 'FDB (TestMsg, TestMsg))
oneToOneJoinJob l r = do
  oneToOneJoin "out" l r payload payload (,)

oneToOneJoinTest :: Subspace -> Database -> TestTree
oneToOneJoinTest testSS db = testCase "oneToOneJoin" $ do
  ss <- extendRand testSS
  let in1 = [ TestMsg "k1" "1" "2"
            , TestMsg "k2" "5" "6"
            , TestMsg "k3" "7" "8"]
  let in2 = [ TestMsg "k1" "1" "3"
            , TestMsg "k2" "5" "7"
            , TestMsg "k3" "7" "9"]
  outStream <- testOnInput2 ((testJobConfig db ss) {JC.msgsPerBatch = 1}) in1 in2 oneToOneJoinJob
  results <- Set.fromList <$> dumpStream db outStream
  results @?= Set.fromList (zip in1 in2)

jobTests :: Subspace -> Database -> TestTree
jobTests testSS db = testGroup "Jobs" [indexTest testSS db,
                                       oneToOneSelfJoinTest testSS db,
                                       oneToOneJoinTest testSS db]
