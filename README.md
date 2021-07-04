# fdb-streaming
Playground for me to try building something interesting on top of FoundationDB. Not intended to be used for anything serious.

# Tests

Test with `stack test --test-arguments "-j 2"`. Using more concurrency sometimes causes weird behavior, possibly because we start too many transactions simultaneously without robust backoff and retry mechanisms.
