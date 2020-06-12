{-# LANGUAGE ScopedTypeVariables #-}

module Spec.FDBStreaming.Util
  ( utilProps,
    extendRand,
  )
where

import Data.UUID (toWords)
import qualified Data.UUID.V4 as UUID
import FDBStreaming.Util (chunksOfSize)
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB
import Test.QuickCheck ((===), Arbitrary, Property, property)
import Test.Tasty (TestTree, testGroup)
import qualified Test.Tasty.QuickCheck as QC

-- | Randomly extends a subspace. This allows us to run tests in parallel
-- without collisions.
extendRand :: FDB.Subspace -> IO FDB.Subspace
extendRand ss = do
  u <- UUID.nextRandom
  let (w1, w2, w3, w4) = toWords u
  return $ FDB.extend ss [FDB.UUID w1 w2 w3 w4]

binaryProperty :: (Show a, Arbitrary a, Show b, Arbitrary b) => (a -> b -> Property) -> Property
binaryProperty x = property $ property x

chunksOfSizeLengthPreserved :: Property
chunksOfSizeLengthPreserved = binaryProperty $ \n (xs :: [()]) ->
  let chunks = chunksOfSize n (const 1) xs
   in length xs === length (concat chunks)

chunksOfSizeRespectsSize :: Property
chunksOfSizeRespectsSize = binaryProperty $ \n (xs :: [()]) ->
  let sz = const 1
      chunks = chunksOfSize n sz xs
   in all (\chunk -> length chunk == 1 || sum (map (fromIntegral . sz) chunk) <= n) chunks === True

chunksOfSizeProps :: TestTree
chunksOfSizeProps =
  testGroup
    "chunksOfSize"
    [ QC.testProperty "Preserves length" chunksOfSizeLengthPreserved,
      QC.testProperty "chunk size respected" chunksOfSizeRespectsSize
    ]

utilProps :: TestTree
utilProps =
  testGroup
    "Utility functions"
    [chunksOfSizeProps]
