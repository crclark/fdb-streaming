stack exec fdb-streaming-writer -- --numWriters $1 --numMsgs $2 +RTS -A64m &
stack exec fdb-streaming-reader -- --numReaders $1 --numMsgs $2 +RTS -A64m
