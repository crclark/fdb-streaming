{-# LANGUAGE ScopedTypeVariables #-}

module Spec.FDBStreaming.Util
  ( utilProps,
    extendRand,
  )
where

import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Test.QuickCheck.Instances.ByteString ()
import Data.Int (Int64)
import qualified Data.Persist as Persist
import Data.UUID (toWords)
import qualified Data.UUID.V4 as UUID
import FDBStreaming.Util (chunksOfSize, millisSinceEpochToUTC, millisSinceEpoch, spanInclusive)
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
chunksOfSizeLengthPreserved = binaryProperty $ \n (xs :: [ByteString]) ->
  let chunks = chunksOfSize n xs
   in length xs === length (concat chunks)

chunksOfSizeRespectsSize :: Property
chunksOfSizeRespectsSize = binaryProperty $ \(n :: Word) (xs :: [ByteString]) ->
  let chunks = chunksOfSize n xs
   in all (\chunk -> length chunk == 1
                     || BS.length (Persist.encode chunk) <= fromIntegral n)
          chunks
      === True

chunksOfSizeProps :: TestTree
chunksOfSizeProps =
  testGroup
    "chunksOfSize"
    [ QC.testProperty "Preserves length" chunksOfSizeLengthPreserved,
      QC.testProperty "chunk size respected" chunksOfSizeRespectsSize
    ]

millisSinceEpochRoundTrip :: Property
millisSinceEpochRoundTrip = property $ \(millis :: Int64) ->
  millis === millisSinceEpoch (millisSinceEpochToUTC millis)

spanInclusiveSlow :: (a -> Bool) -> [a] -> ([a], [a])
spanInclusiveSlow f xs =
  case span f xs of
    ([], []) -> ([], [])
    ([], y:ys) -> ([y], ys)
    (y:ys, []) -> (y:ys, [])
    (y:ys, z:zs) -> ((y:ys) ++ [z], zs)

spanInclusiveProps :: TestTree
spanInclusiveProps =
  testGroup
    "spanInclusive"
    [QC.testProperty
      "spanInclusive is span with an off-by-one error"
      (property (\(xs :: [Int]) ->
        spanInclusive odd xs
        === spanInclusiveSlow odd xs))]

utilProps :: TestTree
utilProps =
  testGroup
    "Utility functions"
    [chunksOfSizeProps,
     spanInclusiveProps,
     QC.testProperty "millisSinceEpoch roundtrips" millisSinceEpochRoundTrip]
