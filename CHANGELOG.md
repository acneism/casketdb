# Changelog

Versions are listed newest first. CasketDB was called BitKV up to and including v0.9.

## Unreleased

### Added

- `OBJECT ENCODING` answers `int`, `embstr` or `raw` for a string, as Redis does.
- String commands GETSET, GETEX, GETRANGE, SETRANGE, INCRBYFLOAT, MSETNX and LCS. INCRBYFLOAT uses 64-bit floating point, so its last digits can differ from Redis; see [numbers and string sizes](docs/commands.md#numbers-and-string-sizes).
- APPEND refuses to grow a string beyond 512 MB, as Redis does.
- Bitmap commands SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD and BITFIELD_RO, with BYTE and BIT ranges and the overflow modes of Redis, and the ACL category `@bitmap`.
- Hashes: HSET, HMSET, HSETNX, HGET, HMGET, HDEL, HLEN, HEXISTS, HSTRLEN, HGETALL, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSCAN and HRANDFIELD, and the ACL category `@hash`. Like Redis, a small hash is stored as one value and a hash of more than 128 fields, or with a field or value longer than 64 bytes, keeps each field in a record of its own, so a change writes only what it touches; see [hashes](docs/commands.md#hashes) and [ADR 11](docs/adr/0011-collection-members.md).
- Sets: SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SINTERSTORE, SINTERCARD, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE and SSCAN, with the two encodings of hashes and the ACL category `@set`. See [sets](docs/commands.md#sets).
- Lists: LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LREM, LTRIM, LINSERT, LPOS, LMOVE and RPOPLPUSH, and the ACL category `@list`. A list of more than 128 elements keeps each element in a record of its own, so pushes and pops write one element. See [lists](docs/commands.md#lists).
- Sorted sets: ZADD with NX, XX, GT, LT, CH and INCR, ZINCRBY, ZREM, ZSCORE, ZMSCORE, ZCARD, ZCOUNT, ZLEXCOUNT, ZRANK and ZREVRANK with WITHSCORE, ZRANGE with BYSCORE, BYLEX, REV and LIMIT, ZRANGESTORE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZPOPMIN, ZPOPMAX, ZMPOP, ZRANDMEMBER, ZSCAN, ZUNION, ZINTER, ZDIFF, ZINTERCARD, ZUNIONSTORE, ZINTERSTORE and ZDIFFSTORE, and the ACL category `@sortedset`. A sorted set of more than 128 members keeps them in records of their own and in a skiplist in memory, so ranks and ranges take O(log n). See [sorted sets](docs/commands.md#sorted-sets) and [ADR 12](docs/adr/0012-ordered-members.md).
- Blocking commands BLPOP, BRPOP, BLMPOP, BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX and BZMPOP, LMPOP, the ACL category `@blocking` and `blocked_clients` in `INFO`. Waiting clients are served in the order they started to wait, a client that disconnects stops waiting, and in a cluster a client waiting on a node that stops being the leader gets `UNBLOCKED`. See [blocking commands](docs/commands.md#blocking-commands).
- HyperLogLog: PFADD, PFCOUNT and PFMERGE on strings in the format of Redis, sparse and dense, with the same hash and estimator, and the ACL category `@hyperloglog`. See [HyperLogLog](docs/commands.md#hyperloglog).
- Geo: GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE, GEORADIUS, GEORADIUSBYMEMBER and their `_RO` forms on sorted sets, with the geohash, search areas and reply formats of Redis, and the ACL category `@geo`. See [geo](docs/commands.md#geo).
- Streams: XADD with NOMKSTREAM, MAXLEN, MINID and LIMIT, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM, XREAD with BLOCK, XSETID, consumer groups with XGROUP, XREADGROUP with BLOCK and NOACK, XACK, XPENDING, XCLAIM and XAUTOCLAIM, XINFO STREAM, GROUPS and CONSUMERS, and the ACL category `@stream`. Each entry, consumer and pending entry is a record of its own, ordered in memory. See [streams](docs/commands.md#streams).
- Pub/Sub: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PUBLISH, SPUBLISH and PUBSUB, and the ACL category `@pubsub`. Each subscriber has its own output queue, limited to 32 MB. In a cluster, messages travel through the Raft log and reach subscribers on every node. See [pub/sub](docs/commands.md#pubsub).
- RESP3: `HELLO 3` switches a connection to RESP3, with the maps, sets, doubles, pushes and verbatim strings Redis returns there; pub/sub messages arrive as pushes and a subscribed RESP3 client may run any command. See [connection](docs/commands.md#connection).
- Client-side caching: `CLIENT TRACKING` with REDIRECT, BCAST, PREFIX, OPTIN and OPTOUT, `CLIENT CACHING`, `CLIENT TRACKINGINFO` and `CLIENT GETREDIR`; invalidations go as RESP3 pushes or to a RESP2 client subscribed to `__redis__:invalidate`, also from followers. See [client-side caching](docs/commands.md#client-side-caching).
- The fault-injection tests also check hashes, sorted sets, sets, lists and streams. Hashes, sorted sets, sets and lists start with 200 elements, so every element is a record of its own. See [CONTRIBUTING](CONTRIBUTING.md#fault-injection-tests).
- RENAME, RENAMENX and COPY with REPLACE and `DB 0`, for every type; the TTL goes along. See [data types](docs/commands.md#data-types).
- A differential test sends the same random commands to CasketDB and to Redis 7.2 and compares the replies; CI runs it against a `redis:7.2` container. See [CONTRIBUTING](CONTRIBUTING.md#differential-tests).
- `-relog-to` copies a stopped database into a new directory with another number of logs. See [changing the number of logs](docs/persistence.md#changing-the-number-of-logs).
- `NOTICE` credits Redis for the skiplist, HyperLogLog and geo algorithms that CasketDB follows.
- Value types in records: `TYPE`, `SCAN … TYPE` and `WRONGTYPE` follow the type of a key. See [ADR 10](docs/adr/0010-value-types.md).

### Changed

- `RAFT ADDLEARNER` replies with the `-raft-peers` value for the new node instead of `OK`, so a node can be added first and started with that value. See [adding a node](docs/replication.md#adding-a-node).

### Fixed

- WATCH did not notice a key that was created and deleted again between WATCH and EXEC. See [transactions](docs/commands.md#transactions).
- With `-appendfsync no`, a node could refuse to start after a power loss, because the Raft log had been compacted past data that was written but not fsynced. The durable index now counts only fsynced data, and the `no` policy fsyncs every 30 seconds so that the log can still be compacted. See [persistence](docs/persistence.md).

### Upgrading from v0.13

- Data directories open unchanged, but their `META` becomes version 2 at the first start, and v0.13 and older refuse the directory after that. Back it up first if you may need to go back.
- In a cluster, upgrade every node before the first write to a hash and before the first PUBLISH: a v0.13 node stops at the new kinds of Raft entry they carry.

## v0.13 — 2026-10-01

### Added

- TLS for clients: `-tls-addr` with `-tls-cert` and `-tls-key` opens a listener for TLS 1.2 and 1.3 next to `-addr`, and `-tls-ca` makes clients present a certificate. The certificate is read again when its file changes. `-addr ""` turns plain text off. See [TLS for clients](docs/configuration.md#tls-for-clients).
- A warning when clients on a non-loopback `-addr` send the password in plain text.
- ACL users: `ACL SETUSER`, `GETUSER`, `DELUSER`, `LIST`, `USERS`, `WHOAMI`, `CAT` and `LOG`, `AUTH <user> <password>`, permissions by command, category and key pattern, `NOPERM` replies. Users are stored in the data directory and, in a cluster, replicated through Raft; `CONFIG SET requirepass` is now stored and replicated too. User names and key patterns must be valid UTF-8. See [access control](docs/commands.md#access-control).
- Protected mode, on by default as in Redis: while the `default` user has no password, clients from other hosts get `DENIED` and are disconnected. `-protected-mode=false` turns it off. See [protected mode](docs/configuration.md#protected-mode).
- Client limits: `-maxclients`, 10,000 by default as in Redis, and `-timeout` for idle clients, off by default. After 10 failed `AUTH` attempts from one address within a second, the rest of that second's attempts from it are refused. `INFO` shows `maxclients` and `rejected_connections`, and the metric `casketdb_rejected_connections_total` counts refused connections. See [client limits](docs/configuration.md#client-limits).
- Audit records in the server log, marked `component=audit`: successful and failed AUTH, `ACL SETUSER` and `DELUSER`, `CONFIG SET`, FLUSHDB and FLUSHALL, and the RAFT commands that change the cluster, each with the user and the client's address. See [audit records](docs/monitoring.md#audit-records).

### Security

- A client that has not authenticated may send commands of at most 10 arguments of 16 KB each, as in Redis; a larger command closes the connection with `ERR Protocol error: unauthenticated multibulk length` or `bulk length`. Before, a client without the password could make the server hold up to `-proto-max-bulk-len` per argument.

### Fixed

- A Raft entry or a snapshot's file list that declared a field longer than the data that followed made a node allocate up to 4 GB before it failed. Found while adding fuzz tests of the decoders, which now run in CI and nightly.

### Known issues

- Still open from v0.12: a learner added after the leader took its latest snapshot cannot catch up until the log is compacted past that snapshot, so `RAFT PROMOTE` times out. See [limitations](docs/limitations.md).

### Upgrading from v0.12

- Data directories open unchanged, and a cluster can be upgraded one node at a time.
- A server that listens on a non-loopback address without a password now refuses clients from other hosts. Set a password, or start it with `-protected-mode=false` (`CASKETDB_PROTECTED_MODE=false`) where every client that can reach it is trusted.
- At most 10,000 clients connect at once; raise `-maxclients` if a node serves more.
- Once users are stored, `-requirepass` is ignored at start; the stored `default` user decides.
- In a cluster, upgrade every node before the first `ACL SETUSER`, `ACL DELUSER` or `CONFIG SET requirepass`: a v0.12 node stops at the new kind of Raft entry they write.

## v0.12 — 2026-10-01

### Added

- Every flag can be set through an environment variable, `CASKETDB_` plus the flag name in upper case with underscores: `CASKETDB_RAFT_PEERS`, `CASKETDB_APPENDFSYNC`. The command line wins, with a warning naming the ignored variable. Before, only `CASKETDB_REQUIREPASS` existed.
- `CONFIG SET` changes `requirepass`, `appendfsync` and `proto-max-bulk-len` at runtime, on one node and until restart, as in Redis. `CONFIG GET` also answers `requirepass`.
- Prometheus metrics at `-metrics-addr`: clients, commands and a command latency histogram, keys, data files, fsync count and time, and on cluster nodes the Raft term, indexes, leadership and membership. See [monitoring](docs/monitoring.md).
- `-log-level` and `-log-format json`. Raft library messages now follow the server's level, so its info messages appear by default.

### Fixed

- The server reports a failure to close the database or the Raft node when it cannot start.

### Known issues

- Once the Raft log has been compacted, a learner added after the leader took its latest snapshot cannot catch up until the log is compacted past that snapshot, so `RAFT PROMOTE` times out. The fix belongs in the Raft library; see [limitations](docs/limitations.md).

### Upgrading from v0.11

- The data format and the wire protocol between nodes are unchanged: upgrade nodes one at a time.

## v0.11.1 — 2026-09-30

### Changed

- Internal cleanup: duplicated code is gone and several code paths are simpler. Behavior, the data format and the wire protocol between nodes are unchanged, and nodes of v0.11.0 and v0.11.1 can run in one cluster.

## v0.11 — 2026-09-28

### Added

- Consistent reads: `-raft-reads linearizable` confirms every read with the leader, so it sees every write acknowledged before it started, on any node. `-raft-reads lease` lets the leader answer from its lease without a network round, assuming clock rates differ by at most `-raft-max-clock-drift`. A read that no leader confirms returns `TRYAGAIN`.
- `RAFT TRANSFER [id]` hands leadership over to another voter before the leader is stopped for maintenance.
- Membership changes at runtime: start a node with `-raft-join`, then `RAFT ADDLEARNER`, `RAFT PROMOTE` and `RAFT REMOVE` on the leader. `RAFT MEMBERS` lists the members; `INFO replication` shows `raft_membership`, `raft_voters` and `raft_learners`.
- Mutual TLS between nodes with `-raft-tls-cert`, `-raft-tls-key` and `-raft-tls-ca`. A node must present a certificate for its own id; the server checks its certificate at start and warns when Raft runs without TLS on a non-loopback address.
- `-raft-listen` sets the Raft listen address when it differs from the node's address in `-raft-peers`; `-raft-election-timeout` sets the election timeout, 1 s by default.
- Fault-injection tests: real server processes under `kill -9`, network partitions, leadership transfers and membership changes, with the RESP client history checked for linearizability by Porcupine. See [CONTRIBUTING](CONTRIBUTING.md#fault-injection-tests).

### Changed

- Replication runs on github.com/acneism/raft v0.3.1. Nodes now negotiate the wire protocol version, so later upgrades can roll through the cluster one node at a time.
- `raft_leader_addr` in `INFO` comes from the cluster configuration stored in the Raft log instead of `-raft-peers`.

### Fixed

- A write command that changed nothing, such as DEL of a missing key or SETNX of an existing one, could answer on the strength of another client's write that was proposed but not yet committed. Had that write been lost in a leader change, the answer would have been wrong; a consistent read right after it could also contradict it. Such commands now wait until the proposed writes they read are committed. Found by the new fault-injection tests.

### Upgrading from v0.10

- The wire format between nodes changed: a cluster cannot mix v0.10 nodes with newer ones. Stop all nodes, upgrade them, and start them again. Data and Raft directories open unchanged.

## v0.10 — 2026-09-28

### Changed

- The project is renamed from BitKV to CasketDB. The Go module is `github.com/acneism/casketdb`, the binary `casketdb`, the password variable `CASKETDB_REQUIREPASS`, and the INFO field `casketdb_version`.
- Replication runs on the Raft library [github.com/acneism/raft](https://github.com/acneism/raft) v0.2.1. hashicorp/raft and raft-wal are gone; CasketDB has no other external dependency.
- Snapshots are taken only when a follower falls behind the start of the leader's log, instead of every 2^20 entries.
- New data directories get the `META` signature `casketdb-meta 1`. Directories with `bitkv-meta 1` still open.

### Added

- Durable index: every log stores Raft index marks, so a node knows exactly which Raft entries are on disk in all logs. The mark rides on the existing fsync; no fsync is added to the write path.
- The Raft log is compacted without snapshots, 65,536 entries behind the durable index. A restart replays only the entries after the durable index instead of up to about a million.
- Project documentation: README, [docs/](docs/README.md), CONTRIBUTING, SECURITY and this changelog, and a logo.

### Security

- Directories are created with mode `0700` and files with `0600`. They used to be `0755` and `0644`, so on Linux every local user could read the data. The server logs a warning at start if an existing `-dir` or Raft directory is open to other users; run `chmod 700` on it.

### Upgrading from v0.9

- **Single node:** the data directory opens as is.
- **Cluster:** the Raft directory of v0.9 is rejected at start (`ErrOldRaftLog`), and a node whose data directory has keys but no Raft state refuses to start (`ErrNotEmpty`). Start a new cluster from empty directories and load the data through a client.

## v0.9 — 2026-09-26

- Speculative writes on the leader: a write releases its key locks as soon as it is proposed, and the next write to the same key builds on the proposed value. INCR of one hot key in a 3-node cluster went from 0.72k to 18.8k ops/s on Windows with fsync.
- Snapshots hard-link data files instead of copying them: 200k keys take about 29 ms instead of 217–260 ms, and 70 KB of memory instead of 59 MB.
- Fixed: a node that crashed while restoring a snapshot kept a partial database and diverged from the cluster. The restore is now marked and repeated on the next start.
- Fewer allocations per command: SET 10 → 6, GET 12 → 7, INCR 9 → 6.

## v0.8 — 2026-09-26

- Reads come from memory: sealed data files are memory-mapped, and the active file is mirrored in memory. A single-threaded GET went from 7 µs to 1 µs on Windows.
- Followers apply Raft entries in batches, up to 2.7× faster.

## v0.7 — 2026-09-26

- `-raft-unsafe-no-fsync` skips the fsync of the Raft log for speed, at the cost of safety on power loss.

## v0.6 — 2026-09-26

- Consecutive pipelined writes of one connection run as one transaction: one group commit, and one Raft entry in a cluster.
- The Raft log moved from raft-boltdb to raft-wal: one fsync per batch instead of at least two. Raft directories of v0.5 are rejected at start.

## v0.5 — 2026-09-26

- Replication through Raft (hashicorp/raft) for clusters of 3 or 5 nodes: a write is acknowledged after a majority has it in the Raft log.
- Followers answer `READONLY`; `INFO replication` shows the Raft state and the leader.
- Flags `-raft-id`, `-raft-peers` and `-raft-dir`.

## v0.4 — 2026-09-26

- Data is written to several independent logs, 4 by default (`-logs`), each with its own group commit, fsync and merge.
- Batches that span logs use two-phase commit and stay atomic after a crash.
- `FLUSHDB` is atomic through a marker file.
- Single-log directories from earlier versions still open.

## v0.3 — 2026-09-26

- No global data lock: the key index is split into 1024 shards with their own locks, and transactions declare the keys they touch.
- Active expiry walks the shards with a cursor, as Dragonfly does. Time comes from an injectable clock.

## v0.2 — 2026-09-25

- Group commit with an in-memory overlay: writers no longer queue behind each other's fsync on Windows.
- Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
- Password protection: `AUTH` and `-requirepass`.
- The server listens on `127.0.0.1` by default.

## v0.1 — 2026-09-25

- First version: Bitcask storage (append-only data files, an in-memory key index, merge with hint files, CRC on every record), a RESP2 server, string, key and expiry commands, `appendfsync` policies and crash recovery.
