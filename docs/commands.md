# Commands and differences from Redis

CasketDB implements the commands of Redis 7 listed below over RESP2 and RESP3. Semantics, replies and error texts follow Redis. An unknown command returns `-ERR unknown command`, a wrong argument count returns `-ERR wrong number of arguments`, and the connection stays open in both cases.

## Supported commands

| Group | Commands | Notes |
| --- | --- | --- |
| Strings | GET, SET, SETNX, SETEX, PSETEX, GETSET, GETEX, GETDEL, MGET, MSET, MSETNX, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, LCS | SET accepts EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET; GETEX accepts EX, PX, EXAT, PXAT, PERSIST; LCS accepts LEN, IDX, MINMATCHLEN, WITHMATCHLEN. MSET, MSETNX and INCR* are atomic |
| Hashes | HSET, HMSET, HSETNX, HGET, HMGET, HDEL, HLEN, HEXISTS, HSTRLEN, HGETALL, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSCAN, HRANDFIELD | See [hashes](#hashes) |
| Sets | SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SINTERSTORE, SINTERCARD, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SSCAN | See [sets](#sets) |
| Lists | LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LMPOP, LLEN, LINDEX, LRANGE, LSET, LREM, LTRIM, LINSERT, LPOS, LMOVE, RPOPLPUSH, BLPOP, BRPOP, BLMPOP, BLMOVE, BRPOPLPUSH | See [lists](#lists) and [blocking commands](#blocking-commands) |
| Sorted sets | ZADD, ZINCRBY, ZREM, ZSCORE, ZMSCORE, ZCARD, ZCOUNT, ZLEXCOUNT, ZRANK, ZREVRANK, ZRANGE, ZRANGESTORE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZPOPMIN, ZPOPMAX, ZMPOP, ZRANDMEMBER, ZSCAN, ZUNION, ZINTER, ZDIFF, ZINTERCARD, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE, BZPOPMIN, BZPOPMAX, BZMPOP | See [sorted sets](#sorted-sets) and [blocking commands](#blocking-commands) |
| Bitmaps | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO | Bitmaps are strings. BITCOUNT and BITPOS accept BYTE and BIT ranges; BITOP supports AND, OR, XOR and NOT; BITFIELD supports GET, SET, INCRBY and OVERFLOW WRAP, SAT or FAIL. A bit offset is below 2³² |
| HyperLogLog | PFADD, PFCOUNT, PFMERGE | See [HyperLogLog](#hyperloglog) |
| Geo | GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE, GEORADIUS, GEORADIUS_RO, GEORADIUSBYMEMBER, GEORADIUSBYMEMBER_RO | See [geo](#geo) |
| Streams | XADD, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM, XREAD, XSETID, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO | See [streams](#streams) |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PUBLISH, SPUBLISH, PUBSUB | See [pub/sub](#pubsub) |
| Keys | DEL, UNLINK, EXISTS, TYPE, RENAME, RENAMENX, COPY, OBJECT, KEYS, SCAN, DBSIZE | Glob patterns `*`, `?`, `[a-z]`, `[^x]`, `\`. SCAN accepts MATCH, COUNT, TYPE. COPY accepts REPLACE and `DB 0`. OBJECT supports ENCODING only, see [data types](#data-types) |
| Expiry | EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST | NX, XX, GT, LT. A time in the past deletes the key. TTL returns −2 for a missing key and −1 for a key without expiry |
| Transactions | MULTI, EXEC, DISCARD, WATCH, UNWATCH | See [transactions](#transactions) |
| Connection | PING, ECHO, QUIT, AUTH, SELECT, HELLO, CLIENT | CLIENT supports ID, GETNAME, SETNAME, SETINFO, TRACKING, TRACKINGINFO, GETREDIR and CACHING |
| Access control | ACL SETUSER, ACL GETUSER, ACL DELUSER, ACL LIST, ACL USERS, ACL WHOAMI, ACL CAT, ACL LOG | See [access control](#access-control) |
| Server | INFO, FLUSHDB, FLUSHALL, SAVE, BGREWRITEAOF, COMMAND, CONFIG | See [server commands](#server-commands) |
| Cluster | RAFT MEMBERS, RAFT ADDLEARNER, RAFT PROMOTE, RAFT REMOVE, RAFT TRANSFER | CasketDB's own commands, see [cluster administration](#cluster-administration) |

## Differences from Redis

### Data types

Strings, including the bitmap and HyperLogLog commands, hashes, sets, lists, sorted sets, including the geo commands, and streams, plus pub/sub. Lua and Functions are not implemented.

- `OBJECT ENCODING` tells how CasketDB stores a value. For a string it answers `int`, `embstr` or `raw` from the value, as Redis does after SET; Redis also keeps `raw` after APPEND, SETRANGE, SETBIT, BITOP or BITFIELD, and answers `embstr` for the result of INCRBYFLOAT. For a collection it follows the thresholds below, which differ from Redis for hashes and lists.
- RENAME, RENAMENX and COPY of a collection stored one element per record rewrite every element, so they take time in proportion to its size, where Redis moves or duplicates the value in memory.

### Hashes

A hash has the two encodings of Redis, but changes encoding at 128 fields, where Redis does at 512 (`hash-max-listpack-entries`), because a change of a hash stored as one value rewrites all of it:

- Up to 128 fields, none of them and none of their values longer than 64 bytes, the hash is one value, its fields in the order they were added. `OBJECT ENCODING` answers `listpack`. A change rewrites the whole hash.
- Beyond that, each field is a record of its own, and a change writes only the fields it touches plus a small record with the field count. `OBJECT ENCODING` answers `hashtable`. Like Redis, a hash never goes back to `listpack`, even when it shrinks.

Differences from Redis:

- HSCAN returns every matching field in one reply with cursor `0`, for both encodings, and accepts `NOVALUES`. COUNT is checked but does not split the reply.
- HRANDFIELD with a count accepts at most 16,777,216 fields either way; Redis has no such limit. See [random members](#random-members) for how it picks.
- Field expiry (HEXPIRE and the other commands of Redis 7.4) is not supported.

### Sets

A set uses the same two encodings as a hash, with the same thresholds: up to 128 members of up to 64 bytes it is one value (`listpack`), beyond that each member is a record of its own (`hashtable`). There is no `intset` encoding for small sets of integers. SSCAN returns every matching member in one reply with cursor `0`. SRANDMEMBER with a count accepts at most 16,777,216 members either way. See [random members](#random-members) for how SPOP and SRANDMEMBER pick.

### Lists

Up to 128 elements of up to 64 bytes a list is one value (`listpack`); beyond that each element is a record of its own, numbered by its position (`quicklist`). Pushing, popping, LINDEX, LSET and LRANGE then touch only the elements they name. LINSERT, LREM, and LTRIM that keeps the smaller part of a list, rewrite the elements that stay. A list never goes back to `listpack`. Redis 7.2 changes the encoding of a list by its size in bytes (`list-max-listpack-size`, 8 KB) rather than by the number of elements, and goes back to `listpack` when a list shrinks, so `OBJECT ENCODING` of a list can differ.

### Sorted sets

Up to 128 members of up to 64 bytes a sorted set is one value (`listpack`), sorted when a command needs the order. Beyond that each member is a record of its own, and the members are also kept in score order in a skiplist in memory (`skiplist`), so ranks, counts and ranges take O(log n) plus the members returned. A sorted set never goes back to `listpack`. See [ADR 12](adr/0012-ordered-members.md).

Differences from Redis:

- Scores are 64-bit floats, as in Redis, and replies give the shortest decimal form that reads back as the same number, as Redis 7.2 and later do: `0.1`, `1e-05`, `1e+20`. A score of `-0` is stored as `0`.
- ZSCAN returns every matching member in one reply with cursor `0`. ZRANDMEMBER with a count accepts at most 16,777,216 members either way. See [random members](#random-members) for how it picks.

ZUNION, ZINTER, ZDIFF, ZINTERCARD and their STORE forms accept sets as inputs, with a score of 1 for each member, as Redis does.

### Random members

SPOP, SRANDMEMBER, HRANDFIELD and ZRANDMEMBER pick from a large collection (`hashtable` or `skiplist`) the way Redis does. They take a random place in the member table, look at the next 16 members, and pick one of those. A pick costs the same at any size, and members come out close to equally often, though not exactly. A count of half the collection or more reads every member and shuffles them. A negative count picks each member independently. A small collection is always read whole.

### HyperLogLog

A HyperLogLog is a string in the format of Redis: the `HYLL` header, the sparse encoding while it fits in 3,000 bytes and every register is at most 32, then the dense encoding of 12,304 bytes. The hash, the register updates and the estimator are ported from Redis 7.2, and a value copied with GET and SET between CasketDB and Redis keeps working.

Differences from Redis:

- PFCOUNT does not store the cardinality it computes in the value, so it stays a read command and also runs on followers. A value written by PFADD or PFMERGE always has an invalid cached cardinality, and PFCOUNT computes it each time.
- `hll-sparse-max-bytes` is fixed at 3,000, the default of Redis. PFDEBUG and PFSELFTEST are missing.

### Geo

A geo index is a sorted set whose scores are 52-bit geohashes, as in Redis: `TYPE` answers `zset`, and the sorted set commands work on it. The geohash, the areas a search scans, and the replies follow Redis 7.2: coordinates with up to 17 decimals, distances with 4, and a search with COUNT but without ASC, DESC or ANY sorts by distance.

Distances use the sine, cosine and arcsine of Go, which can differ from those of the C library Redis uses in the last bit of a result. The 4 decimals of a reply do not show it, but STOREDIST stores the whole number, and its last digits can differ from Redis.

### Streams

Each entry of a stream is a record of its own, and the IDs of the entries are kept in order in a skiplist in memory, without their fields, so XRANGE, XREVRANGE and XREAD find their start in O(log n) and read only the entries they return. An empty stream stays a key, as in Redis. `OBJECT ENCODING` answers `stream`.

Differences from Redis:

- An entry costs memory like a member of a large collection, about the size of a key; Redis packs entries into nodes of up to 100 and costs a few bytes per entry.
- `~` with MAXLEN or MINID trims exactly, within LIMIT (10,000 by default), where Redis trims whole nodes and may keep more entries.
- XINFO STREAM reports `radix-tree-keys` and `radix-tree-nodes` as 0: there is no radix tree.

A consumer group lives in the record of its stream: its last delivered ID, its read counter and its counts. Each consumer, each pending entry, and each pending entry again under its consumer, is a record of its own next to the entries, so XACK, XCLAIM and a delivery write only the entries they touch, and XPENDING and XAUTOCLAIM find a range of pending entries in O(log n). Deleting the stream key drops its groups with it.

XREAD with BLOCK waits like the [blocking commands](#blocking-commands). It is a read, so in a cluster it also waits on followers, and it wakes when the entry it waits for is applied there. XREADGROUP with BLOCK changes the group, so it waits on the leader only.

### Pub/Sub

Over RESP2, a client that subscribes to a channel, a pattern or a shard channel may only send SUBSCRIBE, UNSUBSCRIBE and their P and S forms, PING, which answers `["pong", message]`, and QUIT, as in Redis; over RESP3 it may send any command and receives messages as pushes. Messages arrive as `message`, `pmessage` and `smessage`; a client subscribed to a channel and to a pattern that matches it receives both, and PUBLISH counts both. Shard channels are a namespace of their own: SPUBLISH reaches only SSUBSCRIBE.

Differences from Redis:

- Each subscriber has its own output queue, and a subscriber that falls more than 32 MB behind is disconnected, like the hard `client-output-buffer-limit pubsub` of Redis; there is no soft limit.
- PUBLISH and SPUBLISH are not allowed inside MULTI.
- In a cluster, PUBLISH and SPUBLISH run on the leader and reach subscribers on every node through the Raft log; their reply counts the subscribers of the leader. Followers answer `READONLY`. See [replication](replication.md#what-is-replicated).
- ACL channel rules (`&channel`) are not supported, so any user allowed to run the commands may use any channel. Keyspace notifications are not supported.

A subscribed client is not closed by `-timeout`.

### Blocking commands

BLPOP, BRPOP, BLMPOP, BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX and BZMPOP wait for an element when every key they name is empty, as in Redis. The timeout is in seconds and may have a fraction; `0` waits forever. Clients waiting on the same key are served in the order they started to wait, and a client that waits on several keys takes from the first of them, in the order it named them, that has an element. Inside MULTI these commands do not wait: with nothing to pop they reply as if the timeout had passed. A waiting client is not closed by `-timeout`, and `INFO` counts waiting clients in `blocked_clients`.

A client that disconnects while it waits stops waiting at once, so it cannot take an element no one will read. CasketDB notices the disconnect by watching the connection; if the client sends more than 16 KB of further commands while it waits, CasketDB stops watching, and that client keeps its place until its timeout or until it is served.

In a cluster only the leader serves these commands; followers answer `READONLY`. When the node a client waits on stops being the leader, the client gets `-UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)`, as Redis replies when a primary becomes a replica, and should retry on the new leader.

### Numbers and string sizes

INCR, DECR, INCRBY and DECRBY accept a strict 64-bit integer: no leading `+`, no leading zeros. Overflow returns an error, as in Redis.

INCRBYFLOAT computes with 64-bit floating point and stores the shortest decimal form of the result. Redis computes with `long double`, so the last digits of a result can differ: `0.1` plus `0.2` is `0.30000000000000004` here and `0.3` in Redis.

APPEND, SETRANGE and LCS limit a string, or the memory LCS needs, to 512 MB, the default `proto-max-bulk-len` of Redis, whatever `-proto-max-bulk-len` is set to.

### SCAN

The cursor is the number of a keydir shard (0–1023), not a position in a hash table. A full iteration visits every shard once, so it returns every key that existed for the whole scan, like Redis. COUNT (10 by default) is how many keys to examine; a call always finishes the shard it started, so a reply may hold more keys than COUNT.

Outside MULTI, KEYS, SCAN and DBSIZE lock one shard at a time. The result is not a point-in-time snapshot: keys written during the call may or may not appear.

### Transactions

- EXEC runs the queued commands in one transaction and writes them as one atomic batch.
- A queued command with an unknown name or a wrong argument count aborts EXEC with `EXECABORT`, as in Redis.
- WATCH compares the position of a key's last write, and for a missing key a counter of the keys created and deleted in its shard, one of 1,024. Expiry of a watched key counts as a change, and so does a key created and deleted again between WATCH and EXEC.
- WATCH can fire spuriously after a background merge moves a watched key, and, while a watched key is missing, when another key of its shard is created or deleted.
- Connection and server commands (SELECT, INFO, CONFIG, FLUSHDB and others) are rejected inside MULTI with `ERR Command not allowed inside a transaction`.

### Connection

- `HELLO 3` switches a connection to RESP3 and `HELLO 2` back; both accept `AUTH` and `SETNAME`, and other versions return `-NOPROTO`. Over RESP3, replies take the types Redis gives them: maps for HGETALL, CONFIG GET, XREAD, XREADGROUP, XINFO, ACL GETUSER and `LCS … IDX`, sets for SMEMBERS, SINTER, SUNION, SDIFF and `SPOP … count`, doubles for scores and coordinates, pairs for WITHSCORES and WITHVALUES, a verbatim string for INFO, `_` for null, and pushes for pub/sub messages, where a subscribed client may also run any other command. See [client-side caching](#client-side-caching).
- `AUTH <password>` signs in as `default`, `AUTH <user> <password>` as any user. Until a client authenticates, every command except AUTH, HELLO and QUIT returns `NOAUTH`. When `default` has no password, a new connection is signed in as `default` right away; in [protected mode](configuration.md#protected-mode), on by default, a client from another host gets `DENIED` instead and is disconnected.
- After 10 failed `AUTH` attempts from one address within a second, `AUTH` and `HELLO … AUTH` from that address answer `ERR too many failed AUTH attempts` until the second is over, even with the right password. Redis has no such limit; see [client limits](configuration.md#client-limits).
- Only database 0. `SELECT 0` succeeds; any other index returns an error.

### Client-side caching

`CLIENT TRACKING ON` makes the server remember the keys a client reads and send it an invalidation when one of them changes, then forget the key until the client reads it again, as in Redis. Over RESP3 the invalidation is a push, `["invalidate", [key]]`; over RESP2 it goes with `REDIRECT` to a client subscribed to `__redis__:invalidate`. `BCAST` with `PREFIX` invalidates every key that starts with a prefix, `OPTIN` and `OPTOUT` work with `CLIENT CACHING`, FLUSHDB and FLUSHALL send `["invalidate", null]`, and in a cluster a node sends invalidations as it applies writes, so a client may read and track on a follower.

Differences from Redis:

- `NOLOOP` is accepted but not honored: a client also gets invalidations for keys it changed itself.
- The server remembers at most 1,000,000 keys, the default `tracking-table-max-keys` of Redis, and the limit cannot be changed; reading a new key beyond that invalidates one of the others.
- A write that lands while a client reads a key may send the client an invalidation for the value it has just read. The server remembers the key before the read and sends the invalidation after the reply, so the client drops a fresh value rather than keeping a stale one.

### Access control

Users work as in Redis 6 and later. `default` always exists; `CONFIG SET requirepass` sets its password, and so does `-requirepass` until the first change to users is stored. Passwords are stored as SHA-256 hashes.

Users are kept in the file `SYSTEM` in the data directory and survive restarts; once it exists, `-requirepass` is ignored at start, with a warning. In a cluster a change to users is a Raft entry: run it on the leader, a follower answers `READONLY`, and every node applies it and includes it in snapshots. FLUSHDB does not touch users.

`ACL SETUSER` understands `on`, `off`, `>password`, `<password`, `#hash`, `!hash`, `nopass`, `resetpass`, `~pattern`, `allkeys`, `resetkeys`, `+command`, `-command`, `+@category`, `-@category`, `allcommands`, `nocommands` and `reset`. A new user starts `off`, without passwords, keys or commands. The categories are `keyspace`, `read`, `write`, `string`, `bitmap`, `hash`, `set`, `list`, `sortedset`, `blocking`, `hyperloglog`, `geo`, `stream`, `pubsub`, `fast`, `slow`, `admin`, `dangerous`, `connection` and `transaction`, assigned as in Redis; `RAFT` is `@admin` and `@dangerous`. A denied command answers `NOPERM`, and inside MULTI it aborts EXEC. `ACL LOG [count|RESET]` lists the latest denials of commands, keys and logins on this node, newest first, up to 128; a repeat within a minute adds to the count of its entry.

Differences from Redis:

- Read-only and write-only key patterns (`%R~`, `%W~`), channels (`&`), selectors and rules for single subcommands (`+config|get`) are not supported.
- User names and key patterns must be valid UTF-8, because `SYSTEM` is JSON; Redis takes any bytes in a key pattern.
- `ACL SAVE`, `ACL LOAD`, `ACL GENPASS` and `ACL DRYRUN` are missing.
- `ACL WHOAMI` and `ACL CAT` are open to every authenticated user; the other ACL subcommands need the `acl` command.
- Disabling a user with `off` stops new logins; open connections keep working. Deleting a user closes its connections at their next command.

### Server commands

- `SAVE` forces an fsync of all logs. There is no RDB file.
- `BGREWRITEAOF` starts a merge (compaction) of all logs.
- `CONFIG GET` answers `appendonly`, `appendfsync`, `save`, `databases`, `proto-max-bulk-len` and `requirepass`.
- `CONFIG SET` changes `requirepass`, `appendfsync` and `proto-max-bulk-len`, several at once and all or none. `requirepass` is the password of the `default` user: it is stored and, in a cluster, replicated like other changes to users. `appendfsync` and `proto-max-bulk-len` change only the node it runs on, as in Redis, until restart; there is no config file for `CONFIG REWRITE` to write, so keep them in [flags or `CASKETDB_` variables](configuration.md). A new password does not log out open connections, and a new `proto-max-bulk-len` applies to new connections.
- `COMMAND` returns an empty list and `COMMAND COUNT` the number of commands. Both exist so that `redis-cli` and `redis-benchmark` start.
- `FLUSHDB` and `FLUSHALL` do the same thing.

### Expiry

Expiry is exact to the millisecond. Keys expire lazily on access and actively in the background: every 100 ms a cursor walks the shards, and a full pass takes at most about 6.4 s. In a cluster each node expires keys by its own clock, see [replication](replication.md#expiry).

### Cluster mode

In a cluster only the leader accepts writes. Followers answer `-READONLY You can't write against a read only replica.`, as a Redis replica does. There is no Redis Cluster protocol (`CLUSTER`, `MOVED`, `ASK`): every node holds all keys.

With consistent reads turned on, a read that no leader could confirm returns `-TRYAGAIN No leader confirmed the read, retry.` The read had no effect, so retrying is safe. See [consistent reads](replication.md#consistent-reads).

### Cluster administration

`RAFT` groups the commands that manage a CasketDB cluster. Redis has no equivalent: it is not the Redis Cluster protocol. The commands need a cluster node (`-raft-id`) and are rejected inside MULTI. All but `RAFT MEMBERS` must run on the leader.

| Command | Reply | Effect |
| --- | --- | --- |
| `RAFT MEMBERS` | Array of `[id, raft address, voter\|learner]` | The membership as this node knows it |
| `RAFT ADDLEARNER id addr` | The `-raft-peers` value for the new node | Adds a node as a learner; start it with `-raft-join` and that value. See [adding a node](replication.md#adding-a-node) |
| `RAFT PROMOTE id` | `OK` | Waits until the learner has caught up, then makes it a voter |
| `RAFT REMOVE id` | `OK` | Removes a voter or a learner; stop the removed node afterwards |
| `RAFT TRANSFER [id]` | `OK` | Hands leadership to the node `id`, or to any other voter. See [leadership transfer](replication.md#leadership-transfer) |

See [changing membership](replication.md#changing-membership) for the procedure.

On a node that is not the leader, the commands answer `ERR this node is not the leader, run it on <id> (raft address <addr>)`.

## Limits

| Limit | Value |
| --- | --- |
| Bulk string (key or value) | `-proto-max-bulk-len`, 512 MB by default |
| Arguments per command | 1,048,576 |
| Inline command line | 64 KB |
| Command from a client that has not authenticated | 10 arguments of up to 16 KB, as in Redis; more closes the connection with `ERR Protocol error: unauthenticated …` |
