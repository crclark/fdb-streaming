module FDBStreaming.Stream
  ( Stream,
    StreamPersisted (External, FDB),
    StreamName,
    isStreamWatermarked,
    getStreamWatermark,
    customStream,
    streamTopic,
    maybeStreamTopic,
    streamName,
    streamWatermarkSS,
    streamFromTopic,
  )
where

import FDBStreaming.Stream.Internal
  ( Stream,
    StreamName,
    StreamPersisted (External, FDB),
    customStream,
    getStreamWatermark,
    isStreamWatermarked,
    maybeStreamTopic,
    streamFromTopic,
    streamName,
    streamTopic,
    streamWatermarkSS,
  )
