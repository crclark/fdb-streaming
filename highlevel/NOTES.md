This messy program is used as a benchmark of throughput. Since joins are currently
the worst case for performance (each incoming message incurs at least one read to find the other side of the join), we do two successive joins.

After the message packing update, we can handle 8k messages per second, with write batches of 500, read batches of 500, 4096 byte target value size, and 2 partitions per topic. End-to-end latency is stable at around 3 to 5 seconds.

increasing number of partitions to 3, we can do 10k messages per second, using 3 partitions per topic. Two generator processes each generating 5k per second, two worker processes with 12 threads each (over-provisioned). End-to-end latency is stable at 4 to 5 seconds.

So far, attempts to increase to 12k/s result in sporadic hot storage processes and ever-increasing end-to-end latency. At this point, it seems we need to scale up to more than a single computer.
