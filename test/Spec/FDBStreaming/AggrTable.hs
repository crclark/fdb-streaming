{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -fno-warn-missing-import-lists #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -fno-warn-unused-top-binds #-}

module Spec.FDBStreaming.AggrTable (tableProps) where

import FDBStreaming.Message (Message (fromMessage, toMessage))

import Control.Monad (forM_, when)
import FDBStreaming.AggrTable as AT
import FoundationDB (Database, runTransaction)
import qualified FoundationDB as FDB
import FoundationDB.Layer.Subspace (Subspace, extend)
import  qualified FoundationDB.Layer.Tuple as FDB
import Test.Tasty
import qualified Test.Tasty.QuickCheck as QC
import Test.QuickCheck ((===), Arbitrary, Property, property)
import Test.QuickCheck.Monadic (monadicIO, run)
import Test.QuickCheck.Monadic as Monadic
import Test.QuickCheck.Instances.ByteString ()
import Test.QuickCheck.Instances.Text ()
import Test.QuickCheck.Instances.UUID ()
import Data.ByteString (ByteString)
import Data.Data (Data)
import Data.Either (fromRight)
import GHC.Generics (Generic)
import Data.Text (Text)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Monoid (Sum, All, Any)
import Data.Persist (Persist)
import qualified Data.Persist as Persist
import Statistics.Monoid (MeanKBN, CalcMean, StatMonoid(..))
import Data.Semigroup (Min(Min), Max(Max))
import Data.Word
import Data.Int
import Numeric.Sum (KBNSum(..))
import Spec.FDBStreaming.Util (extendRand)

deriving via Double instance Arbitrary (Max Double)

deriving via Float instance Arbitrary (Min Float)

deriving via Int32 instance Arbitrary (Max Int32)

deriving via Int32 instance Arbitrary (Min Int32)

deriving via Int64 instance Arbitrary (Max Int64)

deriving via Int64 instance Arbitrary (Min Int64)

deriving via Word32 instance Arbitrary (Max Word32)

deriving via Word32 instance Arbitrary (Min Word32)

binaryProperty :: (Show a, Arbitrary a, Show b, Arbitrary b) => (a -> b -> Property) -> Property
binaryProperty x = property $ property x

ternaryProperty :: (Show a, Arbitrary a,
                    Show b, Arbitrary b,
                    Show c, Arbitrary c)
                => (a -> b -> c -> Property)
                -> Property
ternaryProperty x = property $ property $ property x

propOrdTableKey :: (Ord k, OrdTableKey k) => k -> k -> Property
propOrdTableKey k l =
  compare k l === compare (toKeyBytes k) (toKeyBytes l)

ordTableKeyProps :: TestTree
ordTableKeyProps = testGroup "OrdTableKey"
  [ QC.testProperty "works for ByteString" $ binaryProperty $ propOrdTableKey @ByteString
  , QC.testProperty "works for Text" $ binaryProperty $ propOrdTableKey @Text
  , QC.testProperty "works for Integer" $ binaryProperty $ propOrdTableKey @Integer
  , QC.testProperty "works for Float" $ binaryProperty $ propOrdTableKey @Float
  , QC.testProperty "works for Double" $ binaryProperty $ propOrdTableKey @Double
  , QC.testProperty "works for Bool" $ binaryProperty $ propOrdTableKey @Bool
  , QC.testProperty "works for 2-tuples" $ binaryProperty $ propOrdTableKey @(Bool, Double)
  ]

propMappendTable :: forall v. (Eq v, TableSemigroup v)
                 => Subspace
                 -> Database
                 -> ByteString
                 -> v
                 -> v
                 -> Property
propMappendTable ss db tableName v1 v2 = monadicIO $ do
  ss' <- run $ extendRand ss
  let table = AggrTable (extend ss' [FDB.Bytes tableName]) 2 :: AggrTable Bool v
  let k = True
  run $ runTransaction db $ AT.set table 0 k v1
  run $ runTransaction db $ mappendBatch table 0 [(k, v2)]
  (Just v3) <- run $ runTransaction db $ AT.get table 0 k >>= FDB.await
  Monadic.assert (v3 == v1 <> v2)
  -- Test that we combine across partitions correctly when reading. TODO:
  -- refactor into separate test.
  run $ runTransaction db $ mappendBatch table 1 [(k, v1)]
  (Just v4) <- run $ runTransaction db $ AT.getRow table k
  Monadic.assert (v4 == v1 <> v2 <> v1)

mappendTableProps :: Subspace -> Database -> TestTree
mappendTableProps testSS db = testGroup "mappendTable"
  [ QC.testProperty "Works for (Sum Word32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Sum Word32 -> Sum Word32 -> Property)
  , QC.testProperty "Works for (Sum Int64)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Sum Int64 -> Sum Int64 -> Property)
  , QC.testProperty "Works for (Max Double)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Double -> Max Double -> Property)
  , QC.testProperty "Works for (Max Int32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Int32 -> Max Int32 -> Property)
  , QC.testProperty "Works for (Max Int64)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Int64 -> Max Int64 -> Property)
  , QC.testProperty "Works for (Max Word32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Word32 -> Max Word32 -> Property)
  , QC.testProperty "Works for (Min Float)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Min Float -> Min Float -> Property)
  , QC.testProperty "Works for (Min Int32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Min Int32 -> Min Int32 -> Property)
  , QC.testProperty "Works for (Min Word32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Min Word32 -> Min Word32 -> Property)
  , QC.testProperty "Works for Any" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Any -> Any -> Property)
  , QC.testProperty "Works for All" $ ternaryProperty (propMappendTable testSS db :: ByteString -> All -> All -> Property)
  ]

propTableRange :: forall v . (TableSemigroup v, RangeAccessibleTable v, Eq v)
               => Subspace
               -> Database
               -> ByteString
               -> v
               -> Property
propTableRange ss db tableName v = monadicIO $ do
  ss' <- run $ extendRand ss
  let table = AggrTable (extend ss' [FDB.Bytes tableName]) 2 :: AggrTable Integer v
  forM_ [0..10] $ \i -> run $ runTransaction db $ AT.set table 0 i v
  result <- run $ runTransaction db $ AT.getTableRange table 0 0 10
  let expected = Map.fromList [(i,v) | i <- [0..10]]
  Monadic.assert (result == expected)
  -- Test that we combine across partitions correctly when reading. TODO:
  -- refactor into separate test.
  forM_ [0..10] $ \i -> run $ runTransaction db $ AT.mappendBatch table 1 [(i,v)]
  result2 <- run $ runTransaction db $ AT.getRowRange table 0 10
  let expected2 = Map.fromList [(i, v <> v) | i <- [0..10]]
  Monadic.assert (result2 == expected2)

tableRangeProps :: Subspace -> Database -> TestTree
tableRangeProps testSS db = testGroup "getTableRange"
  [ QC.testProperty "Works for (Sum Word32)" $ binaryProperty (propTableRange testSS db :: ByteString -> Sum Word32 -> Property)
  , QC.testProperty "Works for (Sum Int32)" $ binaryProperty (propTableRange testSS db :: ByteString -> Sum Int32 -> Property)
  , QC.testProperty "Works for Any" $ binaryProperty (propTableRange testSS db :: ByteString -> Any -> Property)
  , QC.testProperty "Works for All" $ binaryProperty (propTableRange testSS db :: ByteString -> All -> Property)
  , QC.testProperty "Works for (Max Double)" $ binaryProperty (propTableRange testSS db :: ByteString -> Max Double -> Property)
  , QC.testProperty "Works for (Min Int64)" $ binaryProperty (propTableRange testSS db :: ByteString -> Min Int64 -> Property)
  ]

newtype Mean = Mean { unMean :: MeanKBN }
  deriving (Eq, Data, Show, Generic)
  deriving Semigroup via MeanKBN
  deriving Monoid via MeanKBN
  deriving CalcMean via MeanKBN

deriving instance Generic KBNSum
deriving instance Persist KBNSum
deriving instance Persist MeanKBN
deriving instance Persist Mean

instance Real a => StatMonoid Mean a where
  addValue (Mean m) a = Mean (addValue m a)
  singletonMonoid a = Mean (singletonMonoid a)

instance Message Mean where
  toMessage = Persist.encode
  fromMessage = fromRight (error "Failed to decode Mean") . Persist.decode

instance TableSemigroup Mean where
  mappendBatch = mappendMessageBatch
  set = setMessage
  get = getMessage

propNonAtomicSemigroup :: Real v => Subspace -> Database -> ByteString -> [v] -> Property
propNonAtomicSemigroup ss db tableName vs = monadicIO $ do
  ss' <- run $ extendRand ss
  let table = AggrTable (extend ss' [FDB.Bytes tableName]) 2 :: AggrTable () Mean
  let kvs = map (\v -> ((), singletonMonoid v)) vs
  run $ runTransaction db $ mappendBatch table 0 kvs
  run $ runTransaction db $ mappendBatch table 1 kvs
  let expectedResult = foldMap singletonMonoid (vs <> vs)
  result <- fromMaybe mempty <$> (run $ runTransaction db $ AT.getRow table ())
  when (expectedResult /= result) $ do
    run $ putStrLn $ "expected: " ++ show expectedResult
    run $ putStrLn $ "got: " ++ show result
  Monadic.assert (expectedResult == (result :: Mean))

nonAtomicSemigroupProps :: Subspace -> Database -> TestTree
nonAtomicSemigroupProps testSS db = testGroup "non-atomic table helper functions"
  [QC.testProperty "Works for Kahan Mean" $ binaryProperty (propNonAtomicSemigroup testSS db :: ByteString -> [Integer] -> Property)
  ]

tableProps :: Subspace -> Database -> TestTree
tableProps testSS db = testGroup "AggrTable"
  [ nonAtomicSemigroupProps testSS db
  , ordTableKeyProps
  , mappendTableProps testSS db
  , tableRangeProps testSS db
  ]
