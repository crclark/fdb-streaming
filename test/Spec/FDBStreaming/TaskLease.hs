{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}

{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -fno-warn-missing-import-lists #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -fno-warn-unused-top-binds #-}

module Spec.FDBStreaming.TaskLease
 ( leaseProps, smProp
 )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, wait)
import Control.Monad (forM_, replicateM, void)
import Control.Monad.IO.Class (liftIO)
import Data.IORef (atomicModifyIORef, newIORef, readIORef)
import qualified Data.Map as Map
import Data.Kind (Type)
import qualified Data.Sequence as Seq
import FDBStreaming.TaskLease
    ( AcquiredLease,
      EnsureTaskResult(AlreadyExists, NewlyCreated),
      TryAcquireResult(TryAcquireSuccess, TryAcquireIsLocked, TryAcquireDoesNotExist),
      HowAcquired(Available, RandomExpired),
      ReleaseResult(ReleaseSuccess, AlreadyExpired, InvalidLease, LeaseTaskMismatch),
      TaskSpace(TaskSpace),
      TaskSpace,
      TaskName,
      TaskID(TaskID),
      acquireRandom,
      ensureTask,
      removeTask,
      release,
      tryAcquire,
      listAllTasks )
import FDBStreaming.TaskLease.Internal (AcquiredLease(..))
import qualified FDBStreaming.TaskRegistry as TR
import FoundationDB (Database, runTransaction, rangeKeys, clearRange)
import FoundationDB.Layer.Subspace (Subspace, subspaceRangeQuery)
import Spec.FDBStreaming.Util (extendRand)
import GHC.Generics
import Safe (headMay)

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)
import Test.QuickCheck ((===), Property)
import Test.QuickCheck.Gen
import Test.QuickCheck.Monadic (monadicIO)
import Test.StateMachine as QSM
import qualified Test.StateMachine.Types.Rank2 as Rank2
import qualified Test.Tasty.QuickCheck as QC
import System.IO.Unsafe (unsafePerformIO)
import qualified Data.Map.Strict as M

cleanup :: Database -> Subspace -> IO ()
cleanup db testSS = do
  let (begin, end) = rangeKeys $ subspaceRangeQuery testSS
  runTransaction db $ clearRange begin end

-- NOTE: since the TaskName type is so easy to create (just a string), we don't
-- actually need the Reference utilities that QSM provides. The r is simply a
-- phantom type to satisfy QSM.
data Command (r :: Type -> Type)
  = -- | Try to acquire the lock for n seconds. Returns 'AcquiredLease' on success.
    TryAcquireFor Int TaskName
  | -- | Release the lock, using the provided AcquiredLease to see if we still have
    -- the lock (i.e., it hasn't timed out). If the timeout has not expired and we
    -- still have the lock, returns success. Otherwise, returns 'Stolen'.
    Deliver AcquiredLease TaskName
  | -- | Simulates the passage of n seconds.
    PassTime Int
  | -- | Create task if it doesn't already exist.
    EnsureTask TaskName
  | -- | Acquire a random task for n seconds. Returns 'AcquiredRandom' on success.
    --   Returns 'AlreadyLocked' if no tasks are available.
    AcquireRandom Int
  deriving (Show, Eq, Ord, Generic1, CommandNames, Rank2.Functor, Rank2.Foldable, Rank2.Traversable)

data Response (r :: Type -> Type)
  = Acquired AcquiredLease
  | AcquiredRandom TaskName AcquiredLease
  | AlreadyLocked
  | Expired
  | Success
  | TaskDNE
  | LeaseDNE
  | TaskEnsured EnsureTaskResult
  deriving (Show, Eq, Ord, Generic1, Rank2.Foldable)

semantics :: Database -> TaskSpace -> Command Concrete -> IO (Response Concrete)
semantics db testTaskSpace (TryAcquireFor n ref) =
  runTransaction db $
    tryAcquire testTaskSpace ref n >>= \case
      TryAcquireIsLocked -> return AlreadyLocked
      TryAcquireSuccess lease -> return (Acquired lease)
      TryAcquireDoesNotExist -> return TaskDNE
semantics db testTaskSpace (Deliver lease ref) =
  runTransaction db $
    release testTaskSpace ref lease >>= \case
      ReleaseSuccess -> return Success
      AlreadyExpired -> return Expired
      InvalidLease -> return LeaseDNE
      LeaseTaskMismatch -> return TaskDNE
semantics _ _ (PassTime n) = do
  threadDelay (n * 1000_000)
  return Success
semantics db testTaskSpace (EnsureTask taskName) = do
  result <- runTransaction db $ ensureTask testTaskSpace taskName
  return $ TaskEnsured result
semantics db testTaskSpace (AcquireRandom n) = do
  result <- runTransaction db $ acquireRandom testTaskSpace (const n)
  case result of
    Nothing -> return AlreadyLocked
    Just (taskName, lease, _) -> return (AcquiredRandom taskName lease)

data LeaseState
  = IsLocked AcquiredLease
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
newtype Model r = Model (M.Map TaskName (TaskID, LeaseState))
  deriving (Generic, Show)

deriving instance ToExpr TaskID

deriving instance ToExpr AcquiredLease

deriving instance ToExpr TaskName

deriving instance ToExpr (Model Concrete)

initModel :: Model r
initModel = Model M.empty

nextTaskID :: Model r -> TaskID
nextTaskID (Model m) = fromIntegral $ M.size m

update :: TaskName
       -> LeaseState
       -> M.Map TaskName (TaskID, LeaseState)
       -> M.Map TaskName (TaskID, LeaseState)
update k ls m =
  case M.lookup k m of
    Nothing -> error $ "update on non-existent key: " ++ show k
    Just (taskID, _) -> M.insert k (taskID, ls) m

getState :: TaskName -> M.Map TaskName (TaskID, LeaseState) -> Maybe LeaseState
getState k m = fmap snd $ M.lookup k m

allStates :: Model r -> [LeaseState]
allStates (Model m) = map snd $ M.elems m

--NOTE: preconditions are used by qsm to constrain what commands are generated,
--not to do test assertions! Unfortunately, this is only documented in the
--github readme, not the haddock docs.
precondition :: Model Symbolic -> Command Symbolic -> Logic
precondition (Model []) (EnsureTask _) = Top
precondition (Model []) _ = Bot .// "When no tasks exist, can only run EnsureTask"
precondition (Model refs) (TryAcquireFor _ ref) =
  case M.lookup ref refs of
    Nothing -> Bot .// ("Can't acquire non-existent taskName: " ++ show ref)
    _ -> Top
precondition _ (Deliver _ _) = Top
precondition _ (PassTime _) = Top
precondition _ (EnsureTask _) = Top
precondition _ (AcquireRandom _) = Top

taskResultID :: EnsureTaskResult -> TaskID
taskResultID (AlreadyExists x) = x
taskResultID (NewlyCreated x) = x

transition :: Model r -> Command r -> Response r -> Model r
transition (Model refs) (TryAcquireFor _ ref) (Acquired acquiredLease) =
  Model (update ref (IsLocked acquiredLease) refs)
transition m (TryAcquireFor _ _) _ = m
transition (Model refs) (Deliver acquiredLease ref) Success =
  Model (update ref (LastLeaseWas acquiredLease) refs)
transition m (Deliver _ _) _ = m
transition m (PassTime _) _ = m
transition m@(Model refs) (EnsureTask tn) (TaskEnsured result) =
  case (getState tn refs, result) of
    (Nothing, AlreadyExists _) -> error "existing task untracked by model!"
    (Nothing, NewlyCreated _) -> Model (M.insert tn (nextTaskID m, NeverLocked) refs)
    (Just _, NewlyCreated _) -> error "task already existed in model but not in DB!"
    (Just _, AlreadyExists _) -> m
transition _ (EnsureTask _) x = error $ "impossible EnsureTask transition: " ++ show x
transition (Model refs) (AcquireRandom _) (AcquiredRandom taskName acquiredLease) =
  Model (update taskName (IsLocked acquiredLease) refs)
transition m (AcquireRandom _) _ = m

-- NOTE: postcondition, despite the name, is passed the state of the model
-- *before* the command is executed and the response is returned. We need to
-- call transition on it ourselves to get the state after command execution.
postcondition :: Model Concrete -> Command Concrete -> Response Concrete -> Logic
postcondition m@(Model oldRefs) cmd resp = case (cmd, resp) of
  (TryAcquireFor _ ref, acquireResult) ->
    case (getState ref newRefs, acquireResult) of
      (Nothing, Acquired _) -> Bot .// "acquired nonexistent task"
      (Nothing, TaskDNE) -> Top .// "can't acquire nonexistent task"
      (Nothing, AlreadyLocked) -> Bot .// "inconsistent state: task DNE but we got AlreadyLocked"
      (Just (IsLocked x), Acquired y) -> x .== y
      (Just (IsLocked _), TaskDNE) -> Bot .// "model thinks task is locked but we got a TaskDNE response"
      (Just (IsLocked _), AlreadyLocked) -> Top
      (leaseState, _) -> Bot .// ("unexpected acquire response. leaseState: "
                                  ++ show leaseState
                                  ++ " response: " ++ show acquireResult)
  (Deliver _ ref, Success) ->
    let (Just oldLockStatus) = getState ref oldRefs
     in isLockedState oldLockStatus .== True .// "locked before delivery"
  (Deliver _ ref, Expired) ->
    let (Just oldLockStatus) = getState ref oldRefs
     in isLockedState oldLockStatus .== True .// "models expired locks as IsLocked"
  (Deliver lease ref, LeaseDNE) ->
    case getState ref oldRefs of
      Just (IsLocked lease') -> lease' ./= lease
      Just (LastLeaseWas _) -> Top
      Just NeverLocked -> Top
      Nothing -> Top
  (Deliver _lease ref, TaskDNE) ->
    case M.lookup ref oldRefs of
      Nothing -> Top
      Just _ -> Bot .// "Got TaskDNE from Deliver, but task exists in model"
  (AcquireRandom _, AcquiredRandom taskName _acquiredLease) ->
    case getState taskName oldRefs of
      -- TODO: this is wrong. The model doesn't get updated by the passage of
      -- time, so this can be legal if enough time has passed. As it is, the
      -- QSM tests aren't testing lock expiry unless we get really lucky with
      -- the sequence of commands that get generated.
      Just oldLockStatus -> isAvailable oldLockStatus .== True .// "can only acquire available tasks"
      Nothing -> error $ "acquired lock that DNE in Model: " ++ show taskName
  (AcquireRandom _, AlreadyLocked) ->
    let allLocked = all isLockedState (allStates m)
     in allLocked .== True .// "AcquireRandom fails only if all are locked"
  (_, _) -> Top

  where (Model newRefs) = transition m cmd resp

generator :: Model Symbolic -> Maybe (Gen (Command Symbolic))
generator (Model refs) =
  Just $
    if null refs
      then-- TODO: for reasons that remain mysterious, qsm is somehow generating
      -- command sequences that don't start with EnsureTask. To see this happen,
      -- remove the preconditions above that prevent it.

        frequency
          [ (1, pure $ EnsureTask "task1"),
            (1, pure $ EnsureTask "task2")
          ]
      else do
        (taskName, (_taskID, leaseState)) <- elements (M.assocs refs)
        let allNames = M.keys refs
        frequency $
          [ (10, pure $ TryAcquireFor 5 taskName),
            (2, pure $ PassTime 1),
            (10, pure $ AcquireRandom 5)
          ]
            ++ case leaseState of
              LastLeaseWas lease -> [(3, pure $ Deliver lease taskName)]
              IsLocked lease ->
                [ (10, pure $ Deliver lease taskName),
                  (1, pure $ Deliver (lease{leaseNumber = leaseNumber lease - 1}) taskName)
                ]
              NeverLocked -> []
            ++ [ (1, pure $ EnsureTask x)
                 | x <- ["task1", "task2"],
                   x `Prelude.notElem` allNames
               ]

shrinker :: Model Symbolic -> Command Symbolic -> [Command Symbolic]
shrinker _ _ = []

mock :: Model Symbolic -> Command Symbolic -> GenSym (Response Symbolic)
mock m@(Model refs) cmd = case cmd of
  ta@(TryAcquireFor _n ref) -> do
    case M.lookup ref refs of
      Just (_, LastLeaseWas AcquiredLease{leaseTaskID, leaseNumber}) ->
        return $ Acquired $ AcquiredLease leaseTaskID (leaseNumber + 1)
      Just (_, IsLocked _) -> return AlreadyLocked
      Just (taskID, NeverLocked) -> return $ Acquired (AcquiredLease taskID 0)
      Nothing -> error $ "tried to acquire nonexistent ref: " ++ show ta ++ " model state: " ++ show refs
  (Deliver _lease _ref) -> return Success
  (PassTime _n) -> return Success
  (EnsureTask taskName) -> case M.lookup taskName refs of
    Just (taskID, _) -> return (TaskEnsured (AlreadyExists taskID))
    Nothing -> return (TaskEnsured (NewlyCreated (nextTaskID m)))
  (AcquireRandom _) -> case headMay $ filter (isAvailable . snd . snd) (M.assocs refs) of
    Nothing -> return AlreadyLocked
    (Just (taskName, (taskID, LastLeaseWas lease))) ->
      return (AcquiredRandom taskName (AcquiredLease taskID (leaseNumber lease + 1)))
    (Just (taskName, (taskID, NeverLocked))) ->
      return (AcquiredRandom taskName (AcquiredLease taskID 1))
    (Just (_, (_taskID, IsLocked _))) -> error "impossible case in mock"

sm :: Database -> TaskSpace -> StateMachine Model Command IO Response
sm db testTaskSpace@(TaskSpace testSS) =
  StateMachine
    initModel
    transition
    precondition
    postcondition
    Nothing
    generator
    shrinker
    (semantics db testTaskSpace)
    mock
    (const $ cleanup db testSS)

smProp :: Database -> Subspace -> Property
smProp db testSS =
  -- TODO: thread an RNG down here so that we don't need to do this.
  let qsmSS = unsafePerformIO (TaskSpace <$> extendRand testSS)
  in
    forAllCommands (sm db qsmSS) Nothing $ \cmds -> monadicIO $ do
      (hist, _model, res) <- runCommands (sm db qsmSS) cmds
      --NOTE: It's not incredibly well documented, but checkCommandNames is printing
      --a histogram of constructors, not actual command sequences.
      prettyCommands (sm db qsmSS) hist (checkCommandNames cmds (res === Ok))

leaseProps :: Subspace -> Database -> TestTree
leaseProps testSS db = do
  testGroup
    "Lease properties"
    [ mutualExclusion testSS db,
      uniformRandomness testSS db,
      mutualExclusionRandom testSS db,
      taskDeletion testSS db
      , QC.testProperty "state machine tests" (smProp db testSS)
    ]

isNewlyCreated :: EnsureTaskResult -> Bool
isNewlyCreated (NewlyCreated _) = True
isNewlyCreated _ = False

isAlreadyExists :: EnsureTaskResult -> Bool
isAlreadyExists (AlreadyExists _) = True
isAlreadyExists _ = False

isTryAcquireSuccess :: TryAcquireResult -> Bool
isTryAcquireSuccess (TryAcquireSuccess _) = True
isTryAcquireSuccess _ = False

isTryAcquireIsLocked :: TryAcquireResult -> Bool
isTryAcquireIsLocked TryAcquireIsLocked = True
isTryAcquireIsLocked _ = False

taskDeletion :: Subspace -> Database -> TestTree
taskDeletion testSS db =
  testCase "removing tasks" $ do
    ss <- TaskSpace <$> extendRand testSS
    let t1 = "testTask1"
    let t2 = "testTask2"
    runTransaction db $ removeTask ss t1
    create1 <- runTransaction db $ ensureTask ss t1
    assertBool "t1 not newly created" $ isNewlyCreated create1
    create2 <- runTransaction db $ ensureTask ss t2
    assertBool "t2 not newly created" $ isNewlyCreated create2
    runTransaction db $ removeTask ss t1
    create12 <- runTransaction db $ ensureTask ss t1
    assertBool "t1 not newly created after delete and create" $ isNewlyCreated create12
    create22 <- runTransaction db $ ensureTask ss t2
    assertBool "t2 didn't already exist after removing t1" $ isAlreadyExists create22
    runTransaction db $ removeTask ss t1
    allTasks <- runTransaction db $ listAllTasks ss
    assertBool "t1 still listed in all tasks" (allTasks == Seq.fromList [t2])
    acquireResult <- runTransaction db $ tryAcquire ss t2 5
    assertBool "couldn't acquire t2" (isTryAcquireSuccess acquireResult)
    runTransaction db $ removeTask ss t2
    allTasks2 <- runTransaction db $ listAllTasks ss
    assertBool "t2 still listed in all tasks" (allTasks2 == Seq.empty)

mutualExclusion :: Subspace -> Database -> TestTree
mutualExclusion testSS db =
  testCase "Shouldn't be acquirable when already acquired" $ do
    ss <- TaskSpace <$> extendRand testSS
    let taskName = "testTask"
    res <- runTransaction db $ ensureTask ss taskName
    assertBool "created" $ isNewlyCreated res
    res2 <- runTransaction db $ ensureTask ss taskName
    assertBool "creating again: already exists" $ isAlreadyExists res2
    acq <- runTransaction db $ tryAcquire ss taskName 5
    assertBool "Failed to acquire a new lock" (isTryAcquireSuccess acq)
    acq2 <- runTransaction db $ tryAcquire ss taskName 5
    assertBool "Acquired a lock that should be locked already" (isTryAcquireIsLocked acq2)
    threadDelay 7000000
    acq3 <- runTransaction db $ tryAcquire ss taskName 5
    assertBool "Failed to acquire a lock that should be expired" (isTryAcquireSuccess acq3)
    assertBool "Acquired same lease for same lock twice" (acq /= acq3)

mutualExclusionRandom :: Subspace -> Database -> TestTree
mutualExclusionRandom testSS db =
  testCase "With only one task, acquireRandom should be equivalent to acquire" $ do
    ss <- TaskSpace <$> extendRand testSS
    let taskName = "testTask2"
    res <- runTransaction db $ ensureTask ss taskName
    assertBool "created" $ isNewlyCreated res
    acq1 <- runTransaction db $ acquireRandom ss (const 5)
    acq1 @?= Just (taskName, AcquiredLease (TaskID 0) 1, Available)
    acq2 <- runTransaction db $ acquireRandom ss (const 5)
    acq2 @?= Nothing
    threadDelay 7000000
    acq3 <- runTransaction db $ acquireRandom ss (const 5)
    acq3 @?= Just (taskName, AcquiredLease (TaskID 0) 2, RandomExpired)
    acq4 <- runTransaction db $ acquireRandom ss (const 5)
    acq4 @?= Nothing

uniformRandomness :: Subspace -> Database -> TestTree
uniformRandomness testSS db =
  testCase "Should acquire each task once when they each get locked" $ do
    ss <- extendRand testSS
    taskReg <- TR.empty ss
    let tasks =
          [ "task1",
            "task2",
            "task3",
            "task4",
            "task5",
            "task6",
            "task7",
            "task8",
            "task9",
            "task10"
          ]
    taskRunCounts <- liftIO $ newIORef (Map.fromList (zip tasks (repeat (0 :: Int))))
    let task taskName _ _ =
          liftIO
            $ void
            $ atomicModifyIORef
              taskRunCounts
              (\n -> (Map.adjust succ taskName n, ()))
    let addTask' tr taskName = TR.addTask tr taskName 100 (task taskName)
    runTransaction db $ forM_ tasks $ addTask' taskReg
    asyncs <- replicateM 50 (async $ TR.runRandomTask db taskReg)
    forM_ asyncs wait
    finalCounts <- readIORef taskRunCounts
    finalCounts @?= Map.fromList (zip tasks (repeat 1))
    oneMore <- TR.runRandomTask db taskReg
    oneMore @?= False
