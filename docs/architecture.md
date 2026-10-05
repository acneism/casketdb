# Architecture

This document is for contributors. It explains how CasketDB is built and what it writes to disk. User-facing behavior is described in [persistence](persistence.md) and [replication](replication.md).

## Packages

The storage engine knows nothing about the network or replication, and the protocol layer knows nothing about commands. Only `internal/replica` has an external dependency, the Raft library; everything else uses the Go standard library. The fault-injection tests in `cmd/casketdb` also use Porcupine.

| Package | Responsibility |
| --- | --- |
| `cmd/casketdb` | Flags, startup, clean shutdown on SIGINT/SIGTERM |
| `internal/server` | TCP server, one goroutine per connection, command table, MULTI/EXEC/WATCH, AUTH, glob matching. A data command is a function `(tx, args) → reply`; the reply is written after the locks are released |
| `internal/resp` | RESP2 reader (arrays and inline commands) and writer, protocol limits |
| `internal/bitcask` | Data files, key index, transactions, group commit, expiry, merge, directory lock |
| `internal/replica` | Raft node on top of the engine: state machine, snapshots, leadership tracking |
| `internal/clock` | Manual clock for tests |

## Key index

The key index (keydir) is 1024 shards of `map[string]entry`, chosen by FNV-1a of the key. An entry is `{fileID, offset, valueSize, expireAt}` with no pointers, so the garbage collector does not scan it. Each shard has its own RWMutex, key and TTL counters, and an overlay of values that are committed but not yet written to the file. Shards also give SCAN a stable cursor and the active expiry cycle a place to resume.

## Transactions

Every command, and every EXEC, is one transaction. A transaction declares its scope up front:

- `Keys(k1, k2, …)` locks only the shards of those keys;
- `All()` locks every shard;
- `Shardwise()` is read-only and locks one shard at a time, for SCAN and DBSIZE outside MULTI.

Touching a key outside the scope fails with `ErrNotLocked`. Shards are locked in ascending order, so transactions cannot deadlock. The server takes a command's keys from its key specification, as Redis does: first, last and step (GET is 1,1,1; MGET is 1,−1,1; MSET is 1,−1,2). EXEC locks the union of the queued keys and the watched keys.

Lock order: log `mergeMu` → shards → `txGate` → log `logMu` → `filesMu` → `wmu`; the queue mutex and the overlay are leaves.

## Logs and group commit

Data is written to N independent logs, 4 by default. Shard i belongs to log i mod N. N is fixed at creation in `META`, otherwise keys would move between logs.

Each log has its own active file, write queue, group-commit leader, fsync and merge. A commit only reserves space under the tiny `logMu`, updates the key index and queues the batch. One waiting writer becomes the group-commit leader and writes the whole queue with one `WriteAt`, outside every data lock. This avoids a convoy on Windows, where `FlushFileBuffers` blocks a concurrent `WriteFile`.

A batch that touches several logs uses two-phase commit. Each part starts with a header carrying a transaction id and the number of parts; after all parts are written, a commit record with the same id goes to every involved log. The transaction holds `txGate` for reading and its shard locks until all commit records are written. Recovery keeps a batch if any log has its commit record and rolls it back otherwise.

## Reads

Sealed data files are memory-mapped read-only. The mapping is created on load, on rotation and after a merge, and removed under the exclusive `filesMu` before a file is closed. The active file cannot be mapped on Windows, because a read-only mapping cannot be larger than the file, so the group-commit leader keeps a copy of it in memory: every write goes to the file and to a byte slice, and readers load the slice atomically. The CRC of a record is checked on every read. If mapping fails, reads fall back to `ReadAt`.

## Expiry

The time source is `Options.Now` (`time.Now` by default); tests use the manual clock from `internal/clock`. A transaction reads the time once, so every command in an EXEC sees the same "now".

Keys expire lazily: an expired key reads as missing. Every 100 ms an active cycle walks the shards in a circle, skipping shards without TTL keys. A cycle stops after 16 shards with TTL keys if no more than 25% of the checked keys had expired, or after 1 ms. A full pass takes at most 64 cycles, about 6.4 s. Active expiry removes keys from the index without writing tombstones; on load, an expired record acts as a tombstone.

## Merge

Merge runs per log:

1. Under `txGate` and the log lock, take the boundary B = id of the active file and N = number of files up to B. The new active file gets id B+N+1; ids B+1…B+N are reserved for the result.
2. Without locks, read files up to B. A record is live if the key index still points to it and it has not expired. Live records are copied to `merge/` with hint files; transaction headers, commit records and index marks are dropped.
3. Fsync the new files and write `merge/MERGED` with B.
4. Move the files into the log directory, repoint unchanged index entries, delete files up to B, delete `merge/`.

Step 4 is idempotent and is completed on the next start. `merge/` without `MERGED` is deleted. Before merge or FLUSHDB drops files, the current Raft index mark is carried into the new active file.

## Replication

`internal/replica` has two files. `replica.go` wraps a `node.Node` from github.com/acneism/raft and gives the server `Update`, `Flush` and `Status`. It watches leadership events: on every event it drops proposed values, and it accepts writes only after the `Ready` event of its own term. `fsm.go` implements the library's `StateMachine`:

- `Apply` merges consecutive operation entries into one `db.Apply` transaction, in log order; a FLUSHDB entry or a users entry breaks the batch. An entry that writes a typed value has its own kind, which carries the type of each operation; a node too old to know it stops instead of storing the value as a string. A users entry replaces `SYSTEM` through `db.SetSystem`. Then it calls `db.MarkApplied` with the last index.
- `DurableIndex` returns `db.DurableIndex()`.
- `Snapshot` calls `db.LinkFiles`, which syncs, then hard-links every data and hint file under `txGate`, and writes the file list with lengths to `casketdb-snapshot`.
- `Restore` copies the listed prefixes into a temporary database, opens it with the normal loader, flushes the main database, copies the keys in batches of 1024 and syncs with the snapshot index marked.

`ReadBarrier` implements consistent reads: with `-raft-reads linearizable` or `lease`, the server calls it before every read transaction. It asks the library for a read index (`ReadIndex`), waits until the node has applied that index (`WaitApplied`) and returns; the read then runs against the local engine. The library resolves a read index before the entries up to it are applied, so the wait is required.

The engine side of speculative writes is `db.Propose(scope, term, fn, publish)`: the transaction hands its operations to `publish`, which proposes them to Raft, and stages the values as proposed instead of writing them. Proposed values are visible only to writers of the same term. `db.Apply(ops, upTo)` writes replicated operations and clears proposed marks up to `upTo`; `db.DropProposed` clears all of them.

### Index marks

`db.MarkApplied(i)` only stores `i`. When the engine syncs (`Sync`, the background sync once per second or, with `-appendfsync no`, every 30 seconds, `Close`) or, with `-appendfsync no`, flushes its queues once per second, it first appends a mark record with the latest applied index to every log. A mark M in a log means every record of that log for entries ≤ M comes before it. Each log remembers which marks are covered by its last fsync; `DurableIndex` is the minimum over logs, so the Raft log is never compacted past data that a power loss could take. On load, a log's mark is the largest mark that survived, and the active file is fsynced before it counts.

## On-disk format

A record in a `.data` file is a 21-byte header, then the key and the value. Integers are little-endian. CRC32-Castagnoli covers everything after the CRC field.

| Field | Bytes | Meaning |
| --- | --- | --- |
| crc | 4 | CRC32-C of flags…value |
| flags | 1 | 1 tombstone, 2 batch continues, 4 FLUSH (old format only), 8 cross-log part header, 16 cross-log commit, 32 Raft index mark, 64 typed value, 128 collection member |
| expireAt | 8 | Unix time in ms, 0 = no expiry |
| keyLen | 4 | Key length |
| valueLen | 4 | Value length |
| key, value | keyLen + valueLen | Data |

A batch in one log is a run of records where every record except the last has the "batch continues" flag. An index mark is a record with flag 32, an empty key and the 8-byte index as its value. A record with flag 64 holds a value that is not a string: the first byte of the value is its type, numbered as in Redis (1 list, 2 set, 3 sorted set, 4 hash, 6 stream), and the rest is the encoded collection. A string has neither the flag nor the byte. A hash is its fields in the order they were added, each as the field's length (uvarint), the field, the value's length (uvarint) and the value.

A large collection keeps each member in a record of its own, with flag 128. Its key is the collection key's length (uvarint), the collection key, an 8-byte generation and the member; its value is the member's value. The type byte of the collection key's own record then has its high bit set, and that record's value starts with the same generation. Members whose generation differs from their key's current record are left over from an earlier collection under the key: the loader skips them and merge drops them. See [ADR 11](adr/0011-collection-members.md). See [ADR 10](adr/0010-value-types.md). Bit 0x40 of the type byte marks a collection whose members are also kept in order of their values, in a skiplist in memory; the loader reads their values to build it. See [ADR 12](adr/0012-ordered-members.md).

A record in a `.hint` file is crc (4) + expireAt (8) + offset (8) + keyLen (4) + valueLen (4) + key; the high bit of keyLen marks a collection member. Hint files let the loader build the key index without reading values; only merge writes them.

`META` is a text file: the line `casketdb-meta 2` and the line `logs N`. Version 2 allows typed records. A directory with `casketdb-meta 1`, or `bitkv-meta 1` from BitKV, opens as is and its `META` is rewritten as version 2, so older versions refuse it from then on. A directory without `META` but with `.data` files in its root is a legacy single-log database and opens as is.

`SYSTEM`, when present, is JSON written atomically with an fsync: `{"users": {"<name>": ["on", "#<sha256>", "~<pattern>", "+@all", …]}}`, one list of `ACL SETUSER` rules per user. Snapshots include it, and FLUSHDB leaves it alone.

## Testing

- Unit tests for every package, with the manual clock instead of wall time.
- A model test: 20,000 random operations with merges and reopenings compared against a map.
- Crash tests: torn tails, cut batches, interrupted merges and flushes, and disk images of a database after `kill -9`, power loss and loss of one log's unsynced tail.
- Fuzz tests of the decoders: the RESP reader, the scanner of data files, Raft entries, the file list of a snapshot, and ACL rules stored in `SYSTEM` and loaded back.
- Cluster tests with 3 nodes in one process over TCP: concurrent writes, failover under load on a hot key, log compaction, tail replay after restart, catch-up by snapshot and interrupted restores, consistent reads, leadership transfer, membership changes and mutual TLS.
- Fault-injection tests in `cmd/casketdb` that run real server processes under `kill -9`, network partitions, leadership transfers and membership changes, and check the RESP client history for linearizability with Porcupine. See [CONTRIBUTING](../CONTRIBUTING.md#fault-injection-tests).

Run `go vet ./...` and `go test ./...` on Linux and Windows; run `go test -race ./...` on Linux. See [CONTRIBUTING](../CONTRIBUTING.md).
