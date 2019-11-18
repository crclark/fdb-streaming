{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -fno-warn-missing-import-lists #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -fno-warn-unused-top-binds #-}

module Spec.FDBStreaming.AggrTable (tableProps) where

import Control.Monad (forM_)
import FDBStreaming.AggrTable as AT
import FoundationDB (Database, runTransaction)
import qualified FoundationDB as FDB
import FoundationDB.Layer.Subspace (Subspace, extend)
import  qualified FoundationDB.Layer.Tuple as FDB
import Test.Hspec (SpecWith, describe, it)
import Test.QuickCheck ((===), Arbitrary, Property, property)
import Test.QuickCheck.Monadic (monadicIO, run)
import Test.QuickCheck.Monadic as Monadic
import Test.QuickCheck.Instances.ByteString ()
import Test.QuickCheck.Instances.Text ()
import Test.QuickCheck.Instances.UUID ()
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.UUID (UUID)
import Data.Monoid (Sum, All, Any)
import Data.Semigroup (Min(Min), Max(Max))
import Data.Word
import Data.Int
import qualified Data.Sequence as Seq

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

ordTableKeyProps :: SpecWith ()
ordTableKeyProps = describe "OrdTableKey" $ do
  it "works for ByteString" $ binaryProperty $ propOrdTableKey @ByteString
  it "works for Text" $ binaryProperty $ propOrdTableKey @Text
  it "works for Integer" $ binaryProperty $ propOrdTableKey @Integer
  it "works for Float" $ binaryProperty $ propOrdTableKey @Float
  it "works for Double" $ binaryProperty $ propOrdTableKey @Double
  it "works for Bool" $ binaryProperty $ propOrdTableKey @Bool
  it "works for UUID" $ binaryProperty $ propOrdTableKey @UUID
  it "works for 2-tuples" $ binaryProperty $ propOrdTableKey @(UUID, Double)

propMappendTable :: forall v. (Eq v, TableSemigroup v)
                 => Subspace
                 -> Database
                 -> ByteString
                 -> v
                 -> v
                 -> Property
propMappendTable ss db tableName v1 v2 = monadicIO $ do
  let table = AggrTable (extend ss [FDB.Bytes tableName]) :: AggrTable Bool v
  run $ runTransaction db $ AT.set table True v1
  run $ runTransaction db $ mappendTable table True v2
  (Just v3) <- run $ runTransaction db $ AT.get table True >>= FDB.await
  Monadic.assert (v3 == v1 <> v2)

mappendTableProps :: Subspace -> Database -> SpecWith ()
mappendTableProps testSS db = describe "mappendTable" $ do
  it "Works for (Sum Word32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Sum Word32 -> Sum Word32 -> Property)
  it "Works for (Sum Int64)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Sum Int64 -> Sum Int64 -> Property)
  it "Works for (Max Double)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Double -> Max Double -> Property)
  it "Works for (Max Int32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Int32 -> Max Int32 -> Property)
  it "Works for (Max Int64)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Int64 -> Max Int64 -> Property)
  it "Works for (Max Word32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Max Word32 -> Max Word32 -> Property)
  it "Works for (Min Float)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Min Float -> Min Float -> Property)
  it "Works for (Min Int32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Min Int32 -> Min Int32 -> Property)
  it "Works for (Min Word32)" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Min Word32 -> Min Word32 -> Property)
  it "Works for Any" $ ternaryProperty (propMappendTable testSS db :: ByteString -> Any -> Any -> Property)
  it "Works for All" $ ternaryProperty (propMappendTable testSS db :: ByteString -> All -> All -> Property)

propTableRange :: forall v . (TableSemigroup v, RangeAccessibleTable v, Eq v)
               => Subspace
               -> Database
               -> ByteString
               -> v
               -> Property
propTableRange ss db tableName v = monadicIO $ do
  let table = AggrTable (extend ss [FDB.Bytes tableName]) :: AggrTable Integer v
  forM_ [0..10] $ \i -> run $ runTransaction db $ AT.set table i v
  result <- run $ runTransaction db $ AT.getTableRange table 0 10
  let expected = Seq.fromList [(i,v) | i <- [0..10]]
  Monadic.assert (result == expected)

tableRangeProps :: Subspace -> Database -> SpecWith ()
tableRangeProps testSS db = describe "getTableRange" $ do
  it "Works for (Sum Word32)" $ binaryProperty (propTableRange testSS db :: ByteString -> Sum Word32 -> Property)
  it "Works for (Sum Int32)" $ binaryProperty (propTableRange testSS db :: ByteString -> Sum Int32 -> Property)
  it "Works for Any" $ binaryProperty (propTableRange testSS db :: ByteString -> Any -> Property)
  it "Works for All" $ binaryProperty (propTableRange testSS db :: ByteString -> All -> Property)
  it "Works for (Max Double)" $ binaryProperty (propTableRange testSS db :: ByteString -> Max Double -> Property)
  it "Works for (Min Int64)" $ binaryProperty (propTableRange testSS db :: ByteString -> Min Int64 -> Property)

tableProps :: Subspace -> Database -> SpecWith ()
tableProps testSS db = do
  ordTableKeyProps
  mappendTableProps testSS db
  tableRangeProps testSS db
