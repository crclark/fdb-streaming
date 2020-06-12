module FDBStreaming.Stream (
  Stream,
  StreamPersisted(External, FDB),
  StreamName,
  isStreamWatermarked,
  getStreamWatermark,
  customStream,
  streamTopic, maybeStreamTopic, streamName, streamWatermarkSS,
  streamFromTopic
) where

import FDBStreaming.Stream.Internal
  (
    Stream,
    StreamPersisted(External, FDB),
    streamTopic, maybeStreamTopic, streamName, streamWatermarkSS,
    StreamName,
    isStreamWatermarked,
    getStreamWatermark,
    customStream,
    streamFromTopic
  )
