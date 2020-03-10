module Spec.FDBStreaming.Util (extendRand) where

import Data.UUID (toWords)
import qualified Data.UUID.V4 as UUID
import qualified FoundationDB.Layer.Subspace as FDB
import qualified FoundationDB.Layer.Tuple as FDB

-- | Randomly extends a subspace. This allows us to run tests in parallel
-- without collisions.
extendRand :: FDB.Subspace -> IO FDB.Subspace
extendRand ss = do
  u <- UUID.nextRandom
  let (w1, w2, w3, w4) = toWords u
  return $ FDB.extend ss [FDB.UUID w1 w2 w3 w4]
