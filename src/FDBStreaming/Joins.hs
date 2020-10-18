module FDBStreaming.Joins
  ( OneToOneJoinSS,
    oneToOneJoinSS,
    delete1to1JoinData,
    write1to1JoinData,
    get1to1JoinData,
  )
where

import FDBStreaming.Message (Message (fromMessage, toMessage))
import qualified FDBStreaming.Topic.Constants as C
import FoundationDB (Future, Transaction, clearRange, get, rangeKeys, set)
import FoundationDB.Layer.Subspace (Subspace, extend, pack, subspaceRange)
import FoundationDB.Layer.Tuple (Elem (Bytes, Int))

-- | For one-to-one joins, we have an outer subspace, and then for each join key
-- we have an inner subspace where we store the two sides of the join. In
-- practice, only one side is ever actually stored, because if one is in there
-- and we receive the other side, we can immediately flush downstream. If it is
-- an n-way join, up to n-1 messages will be stored in each key subspace.
newtype OneToOneJoinSS = OneToOneJoinSS Subspace
  deriving (Show, Eq, Ord)

{-
There is a potential for conflicts in the one-to-one join design. The current
algorithm is:

1. Read a batch of messages
2. Compute the join key of each message
3. Look in FDB to see if the other message(s) for this key is already in the
   join storage (key: join key, join side id (0 left, 1 right); value: message).
   a. If it is, join the two messages and write downstream.
   b. If it is not, write the one message we do have to join storage.

This works nicely to minimize reads, but it can conflict. Imagine that both
messages for a key k are processed at the same time by threads A and B. Then
they both perform the following operations from the algorithm above.

1.
2.
3.b.

But if they both do that, then the joined message will never be written
downstream! Luckily, serializability saves us. One of the two transactions will
conflict, because the read in step 3. is reading a key that the other
transaction is writing.

So is there some other algorithm that is conflict-free and always makes
progress? One method I have thought of is to separate the "staging" step (where
we write to join storage) from the "flushing" step (where we write downstream).

How could this work?

Staging algorithm:

1. Read a batch of messages
2. Compute the join key of each message
3. Write the message into join storage. Same key/value format as above.
4. Write another key that signals to the flusher that this join key needs to be
   checked for completeness. This is basically a topic all over again. Key
   format would be (versionstamp, join key).

Flushing algorithm:

1. Read a checkpoint of how far into the flush queue we have already read.
2. Read n flush signals past that checkpoint.
3. For each flush signal's join key, range read the join storage for the join
   key. If all n messages (or message pointers if we want to save space) are
   present, we can flush. If not, we will check it again the next time a flush
   signal is received for that key.
4. Update the checkpoint.

This has pros and cons:

pros: no conflicts, potential to partition the joining work by join key,
      join key computation and joining computation scale separately.
cons: more serial reads, more workers (meaning more threads needed).

How does the number of reads for each algorithm compare? Use + to mean
potentially concurrent reads, and ⊞ to mean we must block before proceeding to
the next read. Let k be the batch size. Let p be the probability of conflict.
Let n be the arity of the join. We further subscript a variable with _random if
the reads are separate random-access reads rather than a contiguous range read.

Current algorithm:

E(reads_curr) = k ⊞ n*k_random ⊞ p*reads_curr

the last term is meant to signify that we must retry the entire transaction if
a conflict occurs. It's not entirely precise, because we will never need to
retry more than k times (because the other join worker will eventually have
written all the corresponding messages on the other side of the join). I've
never seen more than one retry in practice.

So we can simplify with the assumption that there will be only one retry and
say

E(reads_curr) = k ⊞ n*k_random ⊞ p*(k ⊞ n*k_random)

Proposed algorithm:

reads_staging_proposed = k
reads_flushing_proposed = 1 ⊞ k ⊞ n*k_random

So, from a latency perspective, we have an additional blocking read to get the
checkpoint, and an additional range read of length k.

From a practical point of view, we can also think of this as adding an entire
extra stream processing step to the pipeline compared to the old implementation.
Is guaranteed progress worth the extra read/write and thread overhead?

Given the apparently low probability of conflict, I currently think the answer
is no. If the probability were 50% or higher, then it would be worth switching
to the new algorithm. Perhaps we will find a workload in the future where the
probability is that high. At that time, we should implement the new algorithm.
-}

oneToOneJoinSS :: Subspace -> OneToOneJoinSS
oneToOneJoinSS prefixSS = OneToOneJoinSS $ extend prefixSS [C.oneToOneJoin]

oneToOneKeySS :: Message k => OneToOneJoinSS -> k -> Subspace
oneToOneKeySS (OneToOneJoinSS ss) k = extend ss [Bytes (toMessage k)]

delete1to1JoinData ::
  Message k => OneToOneJoinSS -> k -> Transaction ()
delete1to1JoinData joinSS k = do
  let ss = oneToOneKeySS joinSS k
  let (x, y) = rangeKeys $ subspaceRange ss
  clearRange x y

write1to1JoinData ::
  (Message k, Message a) =>
  OneToOneJoinSS ->
  k ->
  Int ->
  a ->
  Transaction ()
write1to1JoinData joinSS k upstreamIx x = do
  let ss = oneToOneKeySS joinSS k
  set (pack ss [Int $ fromIntegral upstreamIx]) (toMessage x)

get1to1JoinData ::
  (Message k, Message a) =>
  OneToOneJoinSS ->
  -- | index of the stream being joined. E.g., if this is a three-way join of
  -- streams x, y, and z, 0 means the message came from x, 1 from y, etc.
  Int ->
  k ->
  Transaction (Future (Maybe a))
get1to1JoinData joinSS upstreamIx k = do
  let ss = oneToOneKeySS joinSS k
  let key = pack ss [Int $ fromIntegral upstreamIx]
  f <- get key
  return (fmap (fmap fromMessage) f)

