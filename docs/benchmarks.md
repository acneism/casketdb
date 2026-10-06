# Benchmarks

All numbers below were measured on one machine: AMD Ryzen 5 3500U (4 cores, 8 threads), Windows 10 and Linux under WSL2 on the same laptop. The comparison with Redis and etcd comes first. The sections after it come from `go test -bench` in-process benchmarks and show the effect of CasketDB's own changes.

## Comparison with Redis and etcd

Measured on 2026-10-06 under WSL2. The builds were:

- CasketDB at commit bbe90eb: v0.15.0 with the changes listed as Unreleased in the [changelog](../CHANGELOG.md);
- Redis 7.2.16, built with its default jemalloc;
- etcd 3.7.2.

Clients and servers ran on the same 4-core laptop, so the client takes CPU from the server, and the numbers are lower than a server would give. The ratios say more than the numbers.

How it was measured:

- CasketDB ran with its defaults: four logs and `appendfsync everysec`. Redis ran in three ways: without persistence; with AOF and `appendfsync everysec`, the closest durability; and with AOF plus `io-threads 4`.
- The load came from `redis-benchmark`: 50 connections per process, 3-byte values, keys drawn from a million (`-r 1000000`). Its list, set, hash and sorted-set tests each use one key.
- There were three rounds, with the configurations alternating within each round. Tables give the median of the rounds.
- The ratio columns are the median of the ratios within each round, so they can differ from the quotient of the medians shown.
- Throughput was timed with the monotonic clock. The wall clock under WSL2 jumps by two hours every few seconds, which now and then corrupts the figure that redis-benchmark computes itself. Latencies are redis-benchmark's.

### One client, pipeline of 16

Thousands of commands per second, one `redis-benchmark -P 16` process:

| Command | Redis, no persistence | Redis, AOF everysec | CasketDB | CasketDB / Redis AOF |
| --- | --- | --- | --- | --- |
| SET | 358 | 236 | 344 | 1.21 |
| GET | 409 | 402 | 426 | 1.08 |
| INCR | 404 | 280 | 247 | 0.95 |
| PING | 649 | 625 | 464 | 0.73 |
| MSET of 10 keys | 85 | 55 | 20 | 0.36 |
| LPUSH | 510 | 336 | 130 | 0.39 |
| LPOP | 471 | 310 | 144 | 0.46 |
| SADD | 429 | 251 | 114 | 0.39 |
| SPOP | 438 | 231 | 95 | 0.37 |
| HSET | 383 | 191 | 101 | 0.51 |
| ZADD | 133 | 97 | 27 | 0.24 |
| ZPOPMIN | 418 | 366 | 88 | 0.24 |

### Several clients

Thousands of commands per second, four `redis-benchmark -P 16` processes at once (200 connections):

| Command | Redis, no persistence | Redis, AOF everysec | Redis, AOF and `io-threads 4` | CasketDB |
| --- | --- | --- | --- | --- |
| SET | 282 | 239 | 246 | 260 |
| GET | 334 | 343 | 400 | 475 |

Against Redis with AOF, that is 1.3 times the SETs and 1.5 times the GETs; against Redis with I/O threads, 1.2 and 1.3 times.

### Without pipelining

With one command per round trip and 50 connections, every server here ran at 20–55 thousand commands a second, and the benchmark client was the limit. CasketDB reached 0.65–0.85 of Redis with AOF on strings and 0.4–0.7 on collections.

### Latency

Milliseconds, p50 / p99:

| Load | Redis, AOF everysec | CasketDB |
| --- | --- | --- |
| SET, one client, pipeline of 16 | 2.8 / 9.8 | 1.2 / 8.4 |
| SET, four clients, pipeline of 16 | p99 49 | p99 83 |
| MSET of 10 keys, one client, pipeline of 16 | 13.6 / 30 | 11.9 / 365 |

### A cluster against etcd

Three nodes of each on the same laptop. Both acknowledge a write after the Raft log is fsynced on a majority, and the fsync of the WSL2 virtual disk, about 2.5 ms, keeps the write numbers low for both. etcd was driven by its own `benchmark` tool against the leader, with 8-byte keys and 256-byte values. CasketDB was driven by `redis-benchmark` with 256-byte values, in four processes from 50 clients up.

| Load | etcd | CasketDB | CasketDB / etcd |
| --- | --- | --- | --- |
| Writes, 1 client | 279/s, 3.2 / 8.2 ms | 439/s, 2.1 / 5.2 ms | 1.6 |
| Writes, 50 clients | 2,833/s, 17 / 36 ms | 4,994/s, 9.1 / 19 ms | 1.7 |
| Writes, 500 clients | 6,194/s, 73 / 158 ms | 19,569/s, 22 / 77 ms | 3.1 |
| Reads, serializable / `-raft-reads local`, 500 clients | 17,624/s, 21 / 92 ms | 99,010/s, 2.4 / 13 ms | 5.6 |
| Reads, linearizable, 500 clients | 13,699/s, 28 / 102 ms | 51,724/s, 7.9 / 20 ms | 3.7 |

Latencies are p50 / p99. etcd does more for each write than CasketDB: it keeps every revision of a key for MVCC and watches. The comparison shows what each costs on the same hardware, not that one replaces the other.

### What the numbers show

- Strings on one node keep up with Redis with AOF and pass it when several clients load the node, because CasketDB runs commands on every core.
- Commands on one hot key are 2–4 times slower than Redis. Every command on a key takes the lock of its shard, and a large collection keeps each element in a record of its own, with a log write and an index update. Redis changes memory in place on one thread.
- MSET, and MULTI/EXEC over keys in several logs, commit in two phases while they hold their locks, so under load their tail latency is long. Pipelines of commands that each touch one key skip the two-phase commit, see [how a write reaches disk](persistence.md#how-a-write-reaches-disk).
- Without pipelining, CasketDB serves about three quarters of what Redis does on strings. Its CPU goes to waking a goroutine in Go's network poller for each request, where Redis serves every ready connection in one loop.

### Reproducing

Start the server, then run, for example:

```bash
redis-benchmark -p 6379 -n 2000000 -r 1000000 -c 50 -P 16 -t set,get,incr,lpush,lpop,sadd,spop,hset,zadd,zpopmin,mset
benchmark --endpoints=127.0.0.1:2379 --conns=100 --clients=500 put --key-size=8 --val-size=256 --key-space-size=1000000 --total=200000
```

Run the configurations in alternating rounds and compare within a round: a laptop's speed changes with its temperature by tens of percent.

## Throughput, single node

Thousands of operations per second, 8 threads unless stated otherwise, median of 3 runs, v0.2 → v0.3 → v0.4. v0.3 replaced the global lock with 1024 shards; v0.4 added parallel logs.

| Scenario | Windows | Linux (WSL2) |
| --- | --- | --- |
| 90% GET / 10% SET | 104 → 177 → 395 | 206 → 791 → 1,060 |
| SET, distinct keys, 1 / 4 / 8 logs (v0.4) | 224 / 314 / 272 | 251 / 365 / 240 |
| INCR | 55 → 196 → 205 | 126 → 176 → 186 |
| MSET of two keys in different logs (v0.4) | 104 | 50–160 (high variance) |
| Engine PUT with `appendfsync always`, 128 writers | 59 → 67 → 58 | 19 → 18 → 16 |
| SET over TCP, 8 connections, pipeline of 64 | 121 → 136 → 142 | 127 → 155 → 153 |

Four logs are the best choice on 4 cores: eight add system calls and context switches. With `always`, several logs are slightly slower, because each log fsyncs separately and each fsync covers fewer writes.

v0.6 executes consecutive pipelined writes of one connection as one transaction. SET over TCP with a pipeline of 64, without replication, went from 298k to 472k ops/s on Windows.

## Read latency

Nanoseconds per operation, v0.7 → v0.8. v0.8 serves reads from memory-mapped files and an in-memory copy of the active file instead of a system call per read.

| Scenario | Windows | Linux (WSL2) |
| --- | --- | --- |
| GET, 1 thread | 7,000 → 1,000 | 2,540 → 1,220 |
| GET, 8 threads | 2,250 → 230 | 830 → 380 |
| 90% GET / 10% SET, 8 threads | 2,430 → 700 | 1,130 → 920 |
| GET over TCP, pipelined | 6,620 → 2,800 | 3,800 → 2,490 |

On Windows the `ReadFile` system call took about 70% of a GET, so the gain is larger there than on Linux, where `pread` is cheaper.

## Allocations

v0.9 cut allocations per command, test client included: SET 10 → 6, GET 12 → 7, INCR 9 → 6. Time per operation stayed within noise; the gain is less garbage-collection work.

## Comparing two builds in CI

The [Benchmarks workflow](../.github/workflows/bench.yml) compares a branch with a base on one GitHub runner. It builds the benchmarks of both, runs them in interleaved rounds, ten by default, and puts the `benchstat` table in the run's summary; the raw results are an artifact.

```bash
gh workflow run bench.yml --ref my-branch -f base=main
gh workflow run bench.yml --ref my-branch -f base=v0.11.1 -f bench=BenchmarkServer -f rounds=20
```

A full run takes about an hour. Shared runners are noisy, so trust only the changes `benchstat` marks as significant, and confirm a surprising one on your own machine. There is no history across runs: different runners make the numbers incomparable.

## Methodology notes

- Fsync time on this machine is bimodal on Windows and varies by 10–30% between runs. Compare two builds with interleaved runs, never runs taken hours apart.
- WSL2 fsyncs a virtual disk, about 2.5 ms per call, so fsync-bound numbers on Linux here are pessimistic compared to a server with NVMe.
- Latency percentiles are only meaningful on Linux: the Windows timer is too coarse.
