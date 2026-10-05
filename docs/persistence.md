# Persistence and recovery

The log is the database: there is no separate snapshot file to save. A write that CasketDB acknowledged survives a process crash (`kill -9`) in every mode, and survives a power loss according to the fsync policy.

## Fsync policies

The policies match `appendfsync` in Redis.

| `-appendfsync` | Behavior | Lost on power loss |
| --- | --- | --- |
| `always` | Reply after fsync. Group commit: one fsync covers every writer waiting at that moment | Nothing acknowledged |
| `everysec` (default) | Background fsync once per second | Up to about 1 s of writes |
| `no` | Fsync every 30 seconds, on rotation, `SAVE` and shutdown | Up to about 30 s of writes |

`SAVE` forces an fsync of every log in any mode.

In a cluster, durability of acknowledged writes comes from the Raft log, which is fsynced by default; the policy above governs only the local data files. See [replication](replication.md).

## How a write reaches disk

1. A transaction locks the shards of its keys, reserves space in the log, updates the in-memory index and queues the batch.
2. The group-commit leader of each log writes the whole queue with one write call. Until then, readers see the new values from memory.
3. The client gets its reply after its batch is written, so it survives `kill -9`. With `always`, the reply also waits for the fsync.

A batch in one log is atomic: a batch cut off by a crash is dropped on the next start. A batch that touches keys in several logs uses two-phase commit: its parts are written to every log first, then a commit record to each. Its locks are held until all commit records are written, so nobody reads a half-committed batch.

A write or fsync error is sticky. The database switches to a failed state and rejects all writes; it does not retry the fsync, because after a failed fsync the page cache can no longer be trusted. Restart the server after fixing the disk.

## Crash recovery

On start, CasketDB:

1. Takes the directory lock. A second process on the same directory fails with an error.
2. Reads `META`, finishes or rolls back an interrupted merge in each log, and finishes an interrupted `FLUSHDB`.
3. Loads the logs in parallel. Each file is read from its hint file when there is one, otherwise by scanning the data file.
4. Cuts a torn tail of the last file in a log back to the last complete batch. A damaged record anywhere else is not repaired: the server refuses to start with a corruption error.
5. Keeps a cross-log batch if its commit record reached at least one log, and writes the missing commit records. Otherwise it rolls the batch back, only for keys nobody overwrote afterwards.

An expired record acts as a deletion during load, so an old value of a key never comes back after its newer version expired.

With `everysec` or `no`, a power loss that destroys the unsynced tail of one log but not of another can break the atomicity of a cross-log batch. After `kill -9`, and with `always`, cross-log batches stay atomic.

## Merge

Deleted and overwritten values stay in the log until a merge rewrites it. Each log merges on its own:

- automatically, when dead bytes reach `-merge-ratio` of the log and the database is at least `-merge-min-bytes` (checked every `-merge-interval`);
- on `BGREWRITEAOF`, for all logs.

A merge copies live records of the sealed files into new files with hint files, then swaps them in. Writes continue during a merge. A crash at any point is safe: the next start either completes the swap or discards the unfinished result.

## FLUSHDB

`FLUSHDB` rotates every log, writes one atomic marker with the boundary of each log, and deletes the files before it. A crash in the middle is completed on the next start, so a flushed database never comes back.

## Data directory

```
data/
  META          format version and number of logs
  LOCK          directory lock
  FLUSH         present only while FLUSHDB is in progress
  log-000/      one directory per log
    000000001.data
    000000002.data
    000000002.hint
  log-001/
  ...
  raft/         Raft state, only in a cluster
```

Directories are created with mode `0700` and files with `0600`, readable only by the user that runs CasketDB. See [SECURITY.md](../SECURITY.md#security-model).

The record format is described in [architecture](architecture.md#on-disk-format).

### Changing the number of logs

The number of logs is fixed when a database is created. To change it, stop the node and copy the database into an empty directory with the new number:

```bash
casketdb -dir data -logs 8 -relog-to data8
```

The copy keeps every key, its TTL and the users, and in a cluster the durable Raft index, so the node replays only the entries after it. Start the node with `-dir data8`; in a cluster, move `data/raft` into `data8` first or pass `-raft-dir data/raft`. The original directory is left as it was, so remove it once the node runs from the copy.

## Memory

Memory holds the key index — about 80–100 bytes plus the key length per key — and a copy of each log's active data file, up to `-max-file-size` (64 MB by default) per log. Values are read from memory-mapped sealed files and the page cache.

## Backups

There is no online backup command yet. For a consistent copy, stop the server and copy the data directory. In a cluster, a node that falls behind catches up from the leader automatically.
