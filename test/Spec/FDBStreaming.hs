{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Spec.FDBStreaming where

import Control.Monad ((>=>), void)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Either (fromRight)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromJust)
import Data.Persist (Persist)
import qualified Data.Persist as Persist
import Data.Traversable (for)
import Data.Word (Word64)
import Debug.Trace
import FDBStreaming (Index, Message (fromMessage, toMessage), MonadStream, Stream, indexBy, jobConfigSS, pipe', run, streamTopic)
import qualified FDBStreaming.Index as Index
import FDBStreaming.TableKey (OrdTableKey, TableKey (fromKeyBytes, toKeyBytes))
import FDBStreaming.Testing (testJobConfig, testOnInput)
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
  deriving (Eq, Show, Generic, Persist)

instance Message TestMsg where

  toMessage = Persist.encode

  fromMessage = fromRight (error "Failed to decode TestMsg") . Persist.decode

indexGetCount ::
  (Eq k, TableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  IO Word64
indexGetCount db ix k = runTransaction db $ Index.countForKey ix k >>= await

indexGetAll ::
  (Eq k, TableKey k) =>
  Index.Index k ->
  k ->
  FDB.Transaction [Topic.Coordinate]
indexGetAll ix k = do
  let r = Index.coordinateRangeForKey ix k
  s <- Index.coordinateRangeStream ix r
  fmap snd <$> S.toList s

indexGetAllTopic ::
  (Eq k, TableKey k) =>
  Database ->
  Index.Index k ->
  k ->
  Topic ->
  IO [ByteString]
indexGetAllTopic db ix k t = runTransaction db $ do
  coords <- indexGetAll ix k
  catMaybes <$> for coords (Topic.get t >=> await)

ixJob :: forall m. MonadStream m => Stream TestMsg -> m (Index ByteString, (Index ByteString, Stream TestMsg))
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
  let Just outTopic = streamTopic outStream
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

jobTests :: Subspace -> Database -> TestTree
jobTests testSS db = testGroup "Jobs" [indexTest testSS db]
