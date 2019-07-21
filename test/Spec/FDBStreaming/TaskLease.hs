{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Spec.FDBStreaming.TaskLease where

import Control.Concurrent
import Control.Monad.IO.Class (liftIO)
import FDBStreaming.TaskLease
import FoundationDB (runTransaction, Database, rangeKeys, clearRange)
import FoundationDB.Layer.Subspace
import FoundationDB.Layer.Tuple
import Data.Functor.Classes (Eq1)
import Data.Kind (Type)
import Safe (headMay)
import Test.QuickCheck (Property, (===))
import Test.QuickCheck.Gen
import Test.QuickCheck.Monadic (monadicIO)
import Test.StateMachine as QSM
import GHC.Generics
import Data.TreeDiff.Class (ToExpr)

import qualified Test.StateMachine.Types.Rank2 as Rank2

update :: Eq a => a -> b -> [(a, b)] -> [(a, b)]
update ref i m = (ref, i) : filter ((/= ref) . fst) m

testSS :: Subspace
testSS = subspace [Bytes "fdbstreaming-test"]

testTaskSpace :: TaskSpace
testTaskSpace = taskSpace (extend testSS [Bytes "leases-state-machine"])

cleanup :: Database -> IO ()
cleanup db = do
  let (begin, end) = rangeKeys $ subspaceRange testSS
  runTransaction db $ clearRange begin end

-- NOTE: since the TaskName type is so easy to create (just a string), we don't
-- actually need the Reference utilities that QSM provides. The r is simply a
-- phantom type to satisfy QSM.
data Command (r :: Type -> Type)
  = TryAcquireFor Int TaskName
  -- ^ Try to acquire the lock for n seconds. Returns 'AcquiredLease' on success.
  | Deliver AcquiredLease TaskName
  -- ^ Release the lock, using the provided AcquiredLease to see if we still have
  -- the lock (i.e., it hasn't timed out). If the timeout has not expired and we
  --still have the lock, returns success. Otherwise, returns 'Stolen'.
  | PassTime Int
  -- ^ Simulates the passage of n seconds.
  | EnsureTask TaskName
  -- ^ Create task if it doesn't already exist.
  | AcquireRandom Int
  -- ^ Acquire a random task for n seconds. Returns 'AcquiredRandom' on success.
  --   Returns 'AlreadyLocked' if no tasks are available.
  deriving (Show, Eq, Ord, Generic1, CommandNames, Rank2.Functor, Rank2.Foldable, Rank2.Traversable)

data Response (r :: Type -> Type)
  = Acquired AcquiredLease
  | AcquiredRandom TaskName AcquiredLease
  | AlreadyLocked
  | Expired
  | Success
  | LeaseDNE
  | TaskEnsured EnsureTaskResult
  deriving (Show, Eq, Ord, Generic1, Rank2.Foldable)

semantics :: Database -> Command Concrete -> IO (Response Concrete)
semantics db (TryAcquireFor n ref) =
  runTransaction db $ tryAcquire testTaskSpace ref n >>= \case
    Nothing    -> return AlreadyLocked
    Just lease -> return (Acquired lease)

semantics db (Deliver lease ref) =
  runTransaction db $ release testTaskSpace ref lease >>= \case
    ReleaseSuccess -> return Success
    AlreadyExpired -> return Expired
    InvalidLease   -> return LeaseDNE

semantics _ (PassTime n) = do
  threadDelay (n * 1000_000)
  return Success

semantics db (EnsureTask taskName) = do
  result <- runTransaction db $ ensureTask testTaskSpace taskName
  return $ TaskEnsured result

semantics db (AcquireRandom n) = do
  result <- runTransaction db $ acquireRandom testTaskSpace n
  case result of
    Nothing -> return AlreadyLocked
    Just (taskName, lease) -> return (AcquiredRandom taskName lease)

data LeaseState =
  IsLocked AcquiredLease
  | LastLeaseWas AcquiredLease
  | NeverLocked
  deriving (Show, Eq, Generic)

deriving instance ToExpr LeaseState

isLockedState :: LeaseState -> Bool
isLockedState (IsLocked _) = True
isLockedState _ = False

isAvailable :: LeaseState -> Bool
isAvailable = not . isLockedState

-- | Models a set of locks. Each is either locked (True) or not, and has a
--   lockversion. NOTE: because TaskNames are just strings, we don't even need
--   the Reference machinery. r is just a phantom type to get things in the
--   shape QSM expects.
newtype Model r = Model [(TaskName, LeaseState)]
  deriving (Generic, Show)

deriving instance ToExpr AcquiredLease
deriving instance ToExpr TaskName
deriving instance ToExpr (Model Concrete)

initModel :: Model r
initModel = Model []

--NOTE: preconditions are used by qsm to constrain what commands are generated,
--not to do test assertions! Unfortunately, this is only documented in the
--github readme, not the haddock docs.
precondition :: Model Symbolic -> Command Symbolic -> Logic
precondition (Model []) (EnsureTask _) = Top
precondition (Model []) _ = Bot .// "When no tasks exist, can only run EnsureTask"
precondition (Model refs) (TryAcquireFor _ ref) =
  case lookup ref refs of
    Nothing -> Bot .// ("Can't acquire non-existent taskName: " ++ show ref)
    _       -> Top
precondition _ (Deliver _ _)       = Top
precondition _ (PassTime _)        = Top
precondition _ (EnsureTask _)      = Top
precondition _ (AcquireRandom _)   = Top

taskResultID :: EnsureTaskResult -> TaskID
taskResultID (AlreadyExists x) = x
taskResultID (NewlyCreated x)  = x

transition :: Eq1 r => Model r -> Command r -> Response r -> Model r
transition (Model refs) (TryAcquireFor _ ref) (Acquired acquiredLease) =
  Model (update ref (IsLocked acquiredLease) refs)
transition m (TryAcquireFor _ _) _ = m
transition (Model refs) (Deliver acquiredLease ref) Success =
  Model (update ref (LastLeaseWas acquiredLease) refs)
transition m (Deliver _ _) _ = m
transition m (PassTime _) _  = m
transition m@(Model refs) (EnsureTask tn) (TaskEnsured result) =
  case (lookup tn refs, result) of
    (Nothing, AlreadyExists _) -> error "existing task untracked by model!"
    (Nothing, NewlyCreated _)  -> Model ((tn, NeverLocked):refs)
    (Just _, NewlyCreated _)   -> error "task already existed in model but not in DB!"
    (Just _, AlreadyExists _)  -> m
transition _ (EnsureTask _) x = error $ "impossible EnsureTask transition: " ++ show x
transition
  (Model refs) (AcquireRandom _) (AcquiredRandom taskName acquiredLease) =
    Model (update taskName (IsLocked acquiredLease) refs)
transition m (AcquireRandom _) _ = m

postcondition :: Model Concrete -> Command Concrete -> Response Concrete -> Logic
postcondition (Model refs) cmd resp = case (cmd, resp) of
  (TryAcquireFor _ ref, Acquired _) ->
    let (Just oldLockStatus) = lookup ref refs
      in isLockedState oldLockStatus .== False .// "was unlocked before acquisition"
  (TryAcquireFor _ ref, AlreadyLocked) ->
    let (Just oldLockStatus) = lookup ref refs
      in isLockedState oldLockStatus .== True .// "AlreadyLocked consistent"
  (TryAcquireFor _ _, _) -> Bot .// "Unexpected output for TryAcquireFor"
  (Deliver _ ref, Success) ->
    let (Just oldLockStatus) = lookup ref refs
      in isLockedState oldLockStatus .== True .// "locked before delivery"
  (Deliver _ ref, Expired) ->
    let (Just oldLockStatus) = lookup ref refs
      in isLockedState oldLockStatus .== True .// "models expired locks as IsLocked"
  (Deliver lease ref, LeaseDNE) ->
    case lookup ref refs of
      Just (IsLocked lease') -> lease' ./= lease
      Just (LastLeaseWas _)  -> Top
      Just NeverLocked       -> Top
      Nothing                -> Top
  (AcquireRandom _, AcquiredRandom taskName _acquiredLease) ->
    case lookup taskName refs of
      -- TODO: this is wrong. The model doesn't get updated by the passage of
      -- time, so this can be legal if enough time has passed. As it is, the
      -- QSM tests aren't testing lock expiry unless we get really lucky with
      -- the sequence of commands that get generated.
      Just oldLockStatus -> isAvailable oldLockStatus .== True .// "can only acquire available tasks"
      Nothing -> error $ "acquired lock that DNE in Model: " ++ show taskName
  (AcquireRandom _, AlreadyLocked) ->
    let allStates = map snd refs
        allLocked = foldr (&&) True (map isLockedState allStates)
        in allLocked .== True .// "AcquireRandom fails only if all are locked"
  (_, _) -> Top

generator :: Model Symbolic -> Maybe (Gen (Command Symbolic))
generator (Model refs) = Just $
  if null refs
    -- TODO: for reasons that remain mysterious, qsm is somehow generating
    -- command sequences that don't start with EnsureTask. To see this happen,
    -- remove the preconditions above that prevent it.
    then frequency
           [ (1, pure $ EnsureTask "task1")
           , (1, pure $ EnsureTask "task2")
           ]
    else do
      (ref, mLease) <- elements refs
      frequency $
        [ (10, pure $ TryAcquireFor 5 ref)
        , (2, pure $ PassTime 1)
        , (10, pure $ AcquireRandom 5)
        ]
        ++
        case mLease of
          LastLeaseWas lease -> [(3, pure $ Deliver lease ref)]
          IsLocked lease -> [ (10, pure $ Deliver lease ref)
                            , (1, pure $ Deliver (lease - 1) ref)
                            ]
          NeverLocked    -> []
        ++
        [ (1, pure $ EnsureTask x)
        | x <- ["task1", "task2"]
        , x `Prelude.notElem` names
        ]
  where names = map fst refs

shrinker :: Model Symbolic -> Command Symbolic -> [Command Symbolic]
shrinker _ _ = []

mock :: Model Symbolic -> Command Symbolic -> GenSym (Response Symbolic)
mock (Model refs) cmd = case cmd of
  ta@(TryAcquireFor _n ref) -> do
    let mLease = lookup ref refs
    case mLease of
      Just (LastLeaseWas lease) -> return $ Acquired (lease + 1)
      Just (IsLocked _)         -> return AlreadyLocked
      Just NeverLocked          -> return $ Acquired 0
      Nothing                   -> error $ "tried to acquire nonexistent ref: " ++ show ta ++ " model state: " ++ show refs
  (Deliver _lease _ref) -> return Success
  (PassTime _n)         -> return Success
  (EnsureTask taskName) -> case lookup taskName refs of
    -- NOTE: TaskIDs aren't used in the test logic, so we return TaskID 0
    Just _ -> return (TaskEnsured (AlreadyExists (TaskID 0)))
    Nothing -> return (TaskEnsured (NewlyCreated (TaskID 0)))
  (AcquireRandom _) -> case headMay $ filter (isAvailable . snd) refs of
    Nothing             -> return AlreadyLocked
    (Just (taskName, LastLeaseWas lease)) ->
      return (AcquiredRandom taskName (lease + 1))
    (Just (taskName, NeverLocked)) -> return (AcquiredRandom taskName 0)
    (Just (_,IsLocked _)) -> error "impossible case in mock"

sm :: Database -> StateMachine Model Command IO Response
sm db = StateMachine initModel transition precondition postcondition
          Nothing generator Nothing shrinker (semantics db) mock

smProp :: Database -> Property
smProp db = forAllCommands (sm db) Nothing $ \cmds -> monadicIO $ do
  liftIO $ cleanup db
  (hist, _model, res) <- runCommands (sm db) cmds
  --NOTE: It's not incredibly documented, but checkCommandNames is printing
  --a histogram of constructors, not actual command sequences.
  prettyCommands (sm db) hist (checkCommandNames cmds (res === Ok))

