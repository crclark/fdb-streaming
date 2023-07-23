# fdb-streaming
Playground for me to try building something interesting on top of FoundationDB. Not intended to be used for anything serious.

# Tests

Test with `stack test --test-arguments "-j 2"`.


# FDBStreaming

FDBStreaming is a poorly-named, work-in-progress, proof-of-concept library for large-scale processing of bounded or unbounded data sets and sources. It is inspired by both Kafka Streams and the Dataflow Model. It uses [FoundationDB](https://www.foundationdb.org/) for all data storage and worker coordination. It began as a project to try out FoundationDB. FoundationDB enables us to support several features which are not present in similar systems, and allowed FDBStreaming to be written with surprisingly little code.

## Features

- Fault-tolerant, stateless workers.
- Durability provided by FoundationDB.
- Exactly-once semantics.
- Simple deployment like Kafka Streams -- deploy as many instances of a binary executable, wherever and however you want, and they will automatically distribute work amongst themselves.
- Simple infrastructure dependency: requires only FoundationDB, which is used for both worker coordination and data storage.
- Dataflow-style watermarks and triggers (triggers not yet implemented).
- Distributed monoidal aggregations.
- Distributed joins.

## (Ostensibly) differentiating features

- Serve aggregation results directly from worker instances and FoundationDB -- no separate output step to another database is required.
- Secondary indices can be defined on data streams, allowing you to serve and look up individual records within the stream.
- Unlike Kafka Streams, no tricky locality limitations on joins.
- Easy extensibility to support additional distributed data structures, thanks to FoundationDB. Insert incoming data into priority queues, stacks, spatial indices, hierarchical documents, tables, etc., with end-to-end exactly-once semantics provided transparently.
- Insert data into the stream directly with HTTP requests to workers; no separate data store required.
- Small codebase.
- Group and window by anything -- event time, processing time, color, etc. Unlike the Dataflow Model, event time is not a built-in concept, so complex use cases can be easier to express.

## Current Limitations

- Individual messages must be less than 100 kB in size.
- At each processing step, processing an individual batch of messages must take less than five seconds.
- Because the FoundationDB client library is single-threaded, running many instances of your job executable with a few worker threads per process often gives better performance than running a few instances with many threads per process.
- End-to-end pipeline latency (time from when a message enters the pipeline to the time it flows to the end of the pipeline) is generally on the order of seconds.
- When storing unbounded stream data in FoundationDB, FDBStreaming performance is bound by FoundationDB. Expect tens of thousands to low hundreds of thousands of messages per second for the 'Topic' data structure, and perhaps millions for the 'AggrTable' structure. See [FoundationDB's docs](https://apple.github.io/foundationdb/performance.html) for more information about how performance scales. However, if you are only storing monoidal aggregations in FoundationDB and reading data from another data source (Kafka, S3, etc.), or are otherwise aggressively filtering or shrinking the input data before it gets written to FoundationDB, you will probably be bound by the speed at which you can read from the data source.
- No autoscaling -- the number of threads per 'StreamStep' must be specified by the user. Autoscaling will be implemented in the future.
- No facilities for upgrading jobs with new code -- understanding what changes are safe currently requires some understanding of FDBStreaming internals. Guidelines and tools for identifying breaking changes will be provided in the future.

## Core concepts

Each FDBStreaming job consists of unbounded `Stream`s of data, which serve as input and output to `StreamStep`s, which map, filter, and otherwise transform their inputs. Lastly, `StreamStep`s can also group and window their inputs to perform aggregations, which are written to `AggrTable`s. For scalability reasons, aggregations must be commutative monoids. Many aggregations included out-of-the-box in FDBStreaming are implemented in terms of FoundationDB's atomic operations, which further improves performance.

`Stream`s are abstract representations of unbounded data sources. Internally, they are represented as instructions for how to ask for the next `n` items in the stream, and how to checkpoint where we left off. Input `Stream`s can pull data from external data sources, while intermediate streams created by 'StreamStep's are persisted in FoundationDB in a `Topic` data structure which provides an API roughly similar to Kafka's.

`StreamStep`s read data from `Stream`s in small batches, transform the batch, and write the results to another `Stream` or `AT.AggrTable`. Each batch is processed in a single FoundationDB transaction, which is also used to checkpoint our position in the stream. Furthermore, for advanced use cases, the user can add any additional FoundationDB operations to the transaction. Transactions trivially enable end-to-end exactly-once semantics. The user may also perform arbitrary IO on each batch (the `Transaction` monad is a `MonadIO`), but must take care that such IO actions are idempotent.

`AT.AggrTable`s can be thought of conceptually as `Monoid m => Map k m`. These tables are stored in FoundationDB. Many data processing and analytics questions can be framed in terms of monoidal aggregations, which these tables represent. The API for these tables provides the ability to look up individual values, as well as ranges of keys if the key type is ordered.

## Writing jobs

A stream processing job is expressed as a`MonadStream` action. You should always write a polymorphic action; this is required because your action will be statically analyzed by multiple "interpreters" internally when you call `runJob` on the action.

Here is an example of a simple word count action:

```haskell
{-# LANGUAGE OverloadedStrings #-}
import Data.Text (Text)
import qualified Data.Text as Text
import FDBStreaming

wordCount :: MonadStream m => Stream Text -> m (AggrTable Text (Sum Int))
wordCount txts = aggregate "counts" (groupBy Text.words txts) (const (Sum 1))
```
