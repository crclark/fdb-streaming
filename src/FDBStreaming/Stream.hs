module FDBStreaming.Stream (
  Stream,
  StreamName,
  isStreamWatermarked,
  getStreamWatermark,
  customStream
) where

import FDBStreaming.Stream.Internal
  (
    Stream(streamTopic, streamName, streamWatermarkSS),
    StreamName,
    isStreamWatermarked,
    getStreamWatermark,
    customStream
  )
