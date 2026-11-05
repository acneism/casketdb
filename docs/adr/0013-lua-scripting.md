# 13. Lua scripting through github.com/acneism/lua

Status: accepted, 2026-11-05. Adds a dependency to [ADR 7](0007-standard-library-only.md) and stores function libraries in the file of [ADR 9](0009-users-in-system-file.md).

## Context

Job queues, locks and rate limiters for Redis (BullMQ, Sidekiq, redlock and many others) send their logic as Lua with EVAL, EVALSHA or FCALL. The interpreter was written as a separate library, github.com/acneism/lua: Lua 5.1 as patched in Redis, the Redis sandbox and libraries, replies and error texts of Redis 7.2.16, the standard library only. CasketDB has to run its scripts atomically, with many cores, and replicate them through Raft.

## Decision

- The server holds one `redis.Engine`. EVAL, EVALSHA and FCALL are data commands, and their `_RO` forms are read commands. A script runs inside one engine transaction, as EXEC does. `redis.call` looks the command up in the command table and runs it on that transaction; its reply passes to Lua through RESP.
- The transaction locks the shards of the declared keys. A command on another shard returns `ErrNotLocked`. The host turns that into an abort, which unwinds the script with nothing written, and the server runs the transaction again with every shard locked. The same retry applies to any transaction.
- The transaction's writes replicate as one Raft entry, like any other command. Followers never run scripts, so TIME and random numbers in a script cannot make nodes diverge.
- PUBLISH from a script is sent after the commit. While a script holds shard locks it must not wait for Raft: applying an earlier entry may need those locks.
- INFO, FLUSHDB and FLUSHALL take shard locks or the flush lock themselves, so a script may not call them.
- FUNCTION LOAD, DELETE, FLUSH and RESTORE store the output of FUNCTION DUMP in `SYSTEM` next to the users. Every node, the leader included, replaces its libraries from that payload with FUNCTION RESTORE FLUSH, which swaps them at once. A change that cannot be stored, as on a follower, puts the libraries back from `SYSTEM`.
- The script cache of EVAL and SCRIPT LOAD stays on each node and is lost at restart, as in Redis.

## Consequences

- A script that declares its keys runs in parallel with other commands. One that touches other keys runs again under a lock of every shard, and the whole server waits for it.
- Clients get no `BUSY` while a long script runs: they wait for the locks it holds. SCRIPT KILL and FUNCTION KILL need no lock and stop a script before its first write.
- The server depends on a second library of the same author. Like the Raft library, it uses only the standard library.
- The bit-exact trigonometry of lua-libm is LGPL and is not linked, so seven `math` functions can differ from Redis in the last bit.
- `SYSTEM` can now exist without users. `-requirepass` is ignored only once users are stored in it.
