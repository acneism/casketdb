# Limitations and roadmap

CasketDB is early software. This page lists what it does not do yet, and how each limitation could be lifted. Nothing here is scheduled; it is a map, not a promise.

## Out of scope for v1

These are deliberate choices, not missing features:

- sharding data across nodes, as Redis Cluster does;
- Lua scripting;
- databases other than `db 0`.

## Known limitations

| Limitation | Possible fix |
| --- | --- |
| All keys must fit in RAM: about 80–100 bytes plus the key length per key | Inherent to Bitcask; a disk-based index would be a different engine |
| HSCAN, SSCAN and ZSCAN return a whole collection in one reply; HRANDFIELD, SPOP, SRANDMEMBER and ZRANDMEMBER read every member | A cursor and random access over the member table of a large collection |
| The start of a database reads every member of a large sorted set, one read per member | Keep the score of such members in hint files |
| A stream entry costs about as much memory as a key | Pack entries into records of up to 100, as Redis packs them into nodes |
| The fields of a large hash and the members of a large set, like keys, must fit in RAM | Inherent to Bitcask, see the first row |
| `CONFIG SET appendfsync` and `proto-max-bulk-len` change one node and last until restart | Store them with the users, as Raft entries |
| A cross-log transaction costs two write rounds plus about 60 bytes of header and commit per log | Hash tags like `{user}:…` in Redis Cluster, so that related keys land in one log |
| With `everysec` or `no`, a power loss can break the atomicity of a cross-log transaction | Fsync the parts before the commit records |
| The number of logs is fixed when a database is created | Offline redistribution of keys to a new number of logs |
| KEYS, SCAN and DBSIZE outside MULTI are not a point-in-time snapshot | MVCC versions, or KEYS under an all-shard lock |
| Merge checks every record's liveness with a separate shard lock | Batches grouped by shard |
| WATCH fires spuriously after a merge and misses a key created and deleted in between | Per-shard version counters |
| FLUSHDB, CONFIG, INFO, SELECT and similar commands are rejected inside MULTI | Run them after the transaction commits |
| Expiry depends on the system clock | Detect a backward clock jump at start |
| No online backup | A backup command built on the hard-link snapshots |
| Snapshots hold hard links, so disk space of files deleted by merge is freed only when the snapshot is dropped | Keep one snapshot, or align merges with snapshots |
| Clients find the leader themselves, from `INFO replication` or a `READONLY` reply | Proxy writes to the leader, or reply with its address |
| Each node expires keys by its own clock | NTP; if needed, expiry as Raft entries from the leader |
| An existing single-node database cannot join a cluster | Import through a snapshot when the cluster starts |
| A learner added after the leader took its latest snapshot cannot catch up from a snapshot until the Raft log is compacted past that snapshot, up to about 65,536 writes later: it rejects the snapshot, whose membership does not include it, and the leader sends the same one again. `RAFT PROMOTE` times out meanwhile | A fix in the Raft library, github.com/acneism/raft v0.3.1: take a new snapshot for such a learner |
| Restoring a snapshot on a follower goes through a temporary database: about twice the key index in memory and a full rewrite of the data | Swap data files and rebuild the index in place |
| No production track record. The fault-injection tests cover crashes, partitions, leadership transfers and membership changes, but not clock skew or disk faults | Real deployments; clock and disk faults in the fault-injection tests |
