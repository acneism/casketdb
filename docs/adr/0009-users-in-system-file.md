# 9. Users in a replicated SYSTEM file

Status: accepted, 2026-10-01. Moves `requirepass` out of [ADR 8](0008-configuration.md). [ADR 13](0013-lua-scripting.md) stores function libraries in the same file.

## Context

ACL users must survive restarts and be the same on every node of a cluster. Settings shared by the whole cluster need the same treatment. Records in the data logs would have to survive merge and FLUSHDB, which drop old files.

## Decision

Users live in one JSON file, `SYSTEM`, in the data directory, written atomically with an fsync. It lists every user with the rules `ACL SETUSER` understands. A change is made on a copy of the users and written as a whole: directly on a single node, as a new kind of Raft entry in a cluster, applied by every node in log order and carried in snapshots. The server watches the file and updates the permissions of connected users in place. `CONFIG SET requirepass` is a change to the `default` user.

## Consequences

- Users survive restarts and FLUSHDB, and every node of a cluster has the same ones; only the leader accepts changes.
- Each change rewrites the whole file and, in a cluster, sends it as one entry. That is fine for hundreds of users, not for millions.
- Once the file exists, `-requirepass` is ignored at start.
- A node older than this entry kind stops when it meets one, so a cluster must be upgraded before users change.
- The same file and entry can carry other cluster-wide settings later.
