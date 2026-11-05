# Architecture decision records

Each record explains one decision that shapes CasketDB: the context, what was decided and what follows from it, including the costs. How the code works is in [architecture](../architecture.md); these records say why.

| # | Decision | Date |
| --- | --- | --- |
| [1](0001-bitcask.md) | Bitcask as the storage engine | 2026-09-25 |
| [2](0002-redis-protocol.md) | The Redis protocol and Redis semantics | 2026-09-25 |
| [3](0003-sharded-keydir.md) | A sharded key index and transactions that declare their keys | 2026-09-26 |
| [4](0004-parallel-logs.md) | Parallel logs with two-phase commit across them | 2026-09-26 |
| [5](0005-raft-effect-replication.md) | Raft, replicating the effects of commands | 2026-09-26 |
| [6](0006-durable-index-marks.md) | Compacting the Raft log behind the engine's durable index | 2026-09-27 |
| [7](0007-standard-library-only.md) | The standard library only, outside the Raft adapter | 2026-09-25 |
| [8](0008-configuration.md) | Flags and environment variables, node-local CONFIG SET | 2026-10-01 |
| [9](0009-users-in-system-file.md) | Users in a replicated SYSTEM file | 2026-10-01 |
| [10](0010-value-types.md) | Value types in records, collections as one value first | 2026-10-02 |
| [11](0011-collection-members.md) | Members of large collections as records, tied by a generation | 2026-10-02 |
| [12](0012-ordered-members.md) | An ordered index of members in the engine | 2026-10-03 |
| [13](0013-lua-scripting.md) | Lua scripting through github.com/acneism/lua | 2026-11-05 |

A new record gets the next number and the sections Context, Decision and Consequences. A record is not edited when the decision changes: a new record replaces it, and the old one says so on its status line.
