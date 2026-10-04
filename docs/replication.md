# Replication

A CasketDB cluster is 3 or 5 nodes that hold the same data and agree on every write through Raft, using the library [github.com/acneism/raft](https://github.com/acneism/raft). Each node stores a full copy; there is no sharding between nodes.

## Guarantees

- **A write is acknowledged only after a majority of nodes has it in the Raft log** and the leader has applied it. If the leader fails, the new leader has every acknowledged write. Redis replication, by contrast, is asynchronous and can lose acknowledged writes on failover.
- **Writes go to the leader only.** Followers answer `-READONLY You can't write against a read only replica.`
- **Every node serves reads.** By default a read may be stale: a follower, or a leader that has just lost its leadership, can return data that is behind the latest write. With `-raft-reads linearizable` or `lease`, a read reflects every write acknowledged before it started; see [consistent reads](#consistent-reads). Reads never see uncommitted data.
- **A cluster of 3 survives one failed node, a cluster of 5 survives two.**

## Starting a cluster

Start every node with the same `-raft-peers` and empty directories. The initial configuration comes from `-raft-peers`, so no bootstrap step is needed.

```bash
CASKETDB_REQUIREPASS=secret casketdb -addr 10.0.0.1:6379 -dir data -raft-id n1 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
```

The address in `-raft-peers` is how the other nodes reach that node's Raft transport; the node listens on it too, unless `-raft-listen` gives another address, for example behind NAT or in a container. `-addr` is where clients connect. Clients come from other hosts, so give every node the same password: without one, [protected mode](configuration.md#protected-mode) refuses them. The `redis-cli` examples below read it from `REDISCLI_AUTH`.

A failed leader is replaced after an election timeout, 1 s by default. `-raft-election-timeout` changes it; heartbeats go ten times as often. A lower value speeds up failover on a fast network, a higher one avoids needless elections across slow links.

A node whose data directory holds keys but has no Raft state refuses to start: joining it would make the nodes diverge. To turn an existing single-node database into a cluster, start the cluster empty and load the data through a client.

## Finding the leader

`INFO replication` on any node shows `role`, `raft_state`, `raft_term`, `raft_leader_id`, `raft_leader_addr` and the node's own place in the cluster: `raft_membership` (`voter`, `learner` or `none`), `raft_voters` and `raft_learners`. Clients find the leader themselves: from `INFO replication`, or by trying another node after a `READONLY` reply.

## What is replicated

The leader replicates the effects of a command, not the command. It runs the command in an ordinary transaction and sends the resulting operations — key, value, absolute expiry time, deletion — to Raft. As a result:

- INCR, APPEND and `SET … EX` give the same result on every node, because the time and the read happen once, on the leader;
- applying an entry twice is harmless, since the operations are absolute;
- WATCH and EXEC are checked on the leader against its local versions.

PUBLISH and SPUBLISH run on the leader and travel through the Raft log as entries of their own; every node hands the message to its own subscribers when it applies the entry, so a client subscribed on any node receives it. See [pub/sub](commands.md#pubsub).

## Writes on the leader

The transaction proposes its operations to Raft while it still holds its key locks, so the order of entries in the log matches the order of dependent writes. It then marks the new values as proposed, releases the locks and waits for the commit without them. The next write to the same key builds on the proposed value and does not wait for the previous Raft round, so a hot key is not limited to one write per round trip.

A write command that ends up changing nothing — DEL or GETDEL of a missing key, SETNX of an existing key, EXPIRE of a missing key — still waits until the proposed values it read are committed before it answers. Its answer never rests on a write that could still be lost.

Reads see only committed values.

## Leader changes

- A new leader accepts writes only after it has applied an entry of its own term, which guarantees that every entry from earlier terms is applied.
- A proposal carries the leader's term; the library rejects it if the term has changed. Only operations computed by the current leader reach the log.
- On a leadership change, proposed values are dropped. Writes that were waiting get `ERR replica: write interrupted by a leadership change, it may or may not be applied`: the write may or may not have been committed, as in any consensus system. Retry it if it is idempotent, or read the key to check.

`FLUSHDB` also goes through Raft. It waits for all started writes to finish first.

## Changing membership

The set of voters and learners, with their Raft addresses, is stored in the Raft log. `-raft-peers` matters only when a cluster is created and when a node joins; after that each node takes the membership from its log. `RAFT MEMBERS` on any node shows it.

Changes go one at a time and run on the leader. A second change while one is in progress returns `ERR replica: another membership change is in progress, retry`.

### Adding a node

1. Start the new node with an empty data directory, `-raft-join`, and `-raft-peers` listing itself and **every current member**, as `RAFT MEMBERS` shows them. A node that does not list the current leader rejects its messages and never catches up.

   ```bash
   CASKETDB_REQUIREPASS=secret casketdb -addr 10.0.0.4:6379 -dir data -raft-id n4 -raft-join \
     -raft-peers n4=10.0.0.4:7000,n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
   ```

2. On the leader, add it as a learner. A learner receives the log, or a snapshot if the log was compacted, but does not vote and does not count towards a majority, so a slow new node cannot stall the cluster.

   ```bash
   redis-cli -h 10.0.0.1 RAFT ADDLEARNER n4 10.0.0.4:7000
   ```

3. Promote it to a voter. `RAFT PROMOTE` waits until the learner has caught up with the leader, up to 10 minutes, then makes it a voter.

   ```bash
   redis-cli -h 10.0.0.1 RAFT PROMOTE n4
   ```

`INFO replication` on the new node shows `raft_membership:learner`, then `voter`. Keep an odd number of voters: 4 voters survive one failure, like 3, but need one more node for a majority.

Once the Raft log has been compacted, a new learner catches up from a snapshot. The leader takes one when a follower first needs it, and sends that same snapshot to later learners too. A learner added after the snapshot was taken is not in its membership and rejects it, and with the current Raft library the leader takes a new snapshot only after the log is compacted past the old one, up to about 65,536 writes later. So the second and later nodes added in that window stay at `raft_applied_index:0` and `RAFT PROMOTE` times out. Such a node catches up by itself once the log is compacted past that snapshot; run `RAFT PROMOTE` again then. See [limitations](limitations.md).

### Removing a node

```bash
redis-cli -h 10.0.0.1 RAFT REMOVE n2
```

Then **stop the removed node** and delete its directories. A removed node may never learn that it was removed. Running on with its old membership, it can disturb the cluster, and clients that reach it can get stale data. To remove the leader itself, run `RAFT REMOVE` with its own id: it steps down once the change commits, and the others elect a new leader. Or transfer the leadership first.

To replace a failed node, remove it and add a new one with a new id and an empty directory.

### If a change is interrupted

If the leader changes or loses its majority while a change is in progress, the command returns `ERR replica: the membership change was interrupted and may or may not be applied, check RAFT MEMBERS`. Check `RAFT MEMBERS` on the new leader and repeat the command if needed. Repeating a change that was applied returns an error such as `n4 is already a member`.

## Leadership transfer

Before you stop or restart the leader for maintenance, hand its leadership over, so the cluster does not wait an election timeout for a new one:

```bash
redis-cli -h 10.0.0.1 RAFT TRANSFER n2
redis-cli -h 10.0.0.1 RAFT TRANSFER
```

Without an id, the leader tries the other voters one by one. The leader first brings the target up to date, then tells it to start an election at once. The command returns `OK` when the target leads.

While the transfer runs, usually for well under a second, the leader rejects writes with `READONLY`; clients retry on the new leader. If the target does not take over within an election timeout, for example because it is down, the command returns `ERR replica: leadership transfer failed` and the old leader continues. Run the command on the leader; other nodes answer with the leader's id and Raft address.

## Consistent reads

`-raft-reads` sets how a node answers read commands — GET, MGET, EXISTS, TTL, KEYS, SCAN and the others, and EXEC of a transaction without writes. Writes always go through Raft and are not affected.

| Mode | How a read works | Guarantee |
| --- | --- | --- |
| `local` (default) | The node reads its own data | May be stale |
| `linearizable` | The node asks the leader for its commit index. The leader confirms that it is still the leader with one heartbeat round to a majority, concurrent reads sharing the round. The node waits until it has applied that index, then reads its own data | Linearizable: the read sees every write acknowledged before it started |
| `lease` | Like `linearizable`, but the leader answers from its lease without a heartbeat round. Followers still ask the leader | Linearizable as long as node clocks run at rates that differ by at most `-raft-max-clock-drift` |

A consistent read works on any node: a follower forwards the question to the leader and then reads its own data, so followers take read load off the leader. It never sees the proposed values of writes still in flight, only the state up to the confirmed commit index.

If no leader confirms the read within two election timeouts (2 s by default), the node answers `-TRYAGAIN No leader confirmed the read, retry.` Retrying is always safe. A node cut off from the majority, including a leader that has been deposed and does not know it yet, refuses consistent reads instead of answering with stale data.

### Lease reads

After a leader hears from a majority, no other node can win an election for at least an election timeout. The leader uses a slightly shorter period as a lease: (election timeout − 2 ticks) / (1 + drift), about 0.89 s by default. While the lease holds, it answers reads without a network round.

The lease measures time with each node's own clock. It stays correct as long as clock rates differ by at most `-raft-max-clock-drift` (0.1, that is 10%, by default); the absolute time and NTP steps do not matter. A leader process that was paused, by a long garbage-collection pause or a VM migration, counts the time it missed before answering. Use `linearizable` when you cannot bound clock rate differences, for example on overcommitted virtual machines.

## Durable index and log compaction

Every node applies committed entries to its data files without waiting for an fsync. To know how much of the Raft log it can drop, CasketDB tracks the durable index: the highest Raft index whose effects, and the effects of all earlier entries, are on disk in every log.

- After each batch of entries, the node records the index it has applied. On every fsync of the data files (once per second with `everysec`, on `SAVE` and on shutdown), it appends an index mark to each log. No fsync is added to the write path.
- The durable index is the minimum, over all logs, of the last mark covered by an fsync. After a crash it is the minimum of the last surviving mark in each log, so losing the unsynced tail of one log never overstates it.
- The Raft log is compacted without snapshots, up to 65,536 entries behind the durable index, once at least 65,536 entries can be dropped.
- On restart, a node replays only the entries after its durable index. Replay is idempotent.

With `-appendfsync no`, the durable index follows writes to the files instead of fsyncs. That survives `kill -9`. After a power loss the Raft log may already be compacted past the data that survived; if no local snapshot covers the gap, the node refuses to start rather than diverge from the cluster.

## Snapshots

A snapshot is taken only when a follower falls behind the start of the leader's log. It hard-links the data and hint files and adds a list of their lengths, so it costs time proportional to the number of files, not the data size. The leader keeps the two latest snapshots in `<raft-dir>/snap/`. While a snapshot is kept, files that a merge deleted still take disk space.

A follower restores a snapshot through a temporary database in `<raft-dir>/restore`, then clears its own data and copies the keys in batches. Peak memory is about twice the key index. An interrupted restore is marked in the Raft log and repeated on the next start.

## Raft directory

```
raft/
  wal/        log segments, term and vote (hardstate), compaction and snapshot bounds (meta)
  snap/       snapshots
  restore/    temporary database while a snapshot is being restored
```

A Raft directory written by hashicorp/raft (CasketDB v0.9 and older, then named BitKV) is rejected at start. See the [changelog](../CHANGELOG.md#upgrading-from-v09).

## Mutual TLS between nodes

Without TLS, Raft traffic between nodes travels in plain text and a node accepts any peer that connects to its Raft port; the server logs a warning when its Raft address is not a loopback address. With mutual TLS, every connection is encrypted with TLS 1.3 and both sides present a certificate signed by the cluster's CA. The id a node claims must match its certificate, so a node cannot pose as another member.

```bash
casketdb -dir data -raft-id n1 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000 \
  -raft-tls-cert n1.crt -raft-tls-key n1.key -raft-tls-ca ca.crt
```

A node certificate must:

- carry the node id as its first DNS name (subject alternative name), for example `DNS:n1`;
- allow both server and client authentication, since each node both accepts and opens connections;
- be signed by a CA in the `-raft-tls-ca` file.

CasketDB checks the certificate at start and refuses to run with one that does not fit, for example a certificate issued for another node. A CA and a node certificate can be made with OpenSSL:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 \
  -subj "/CN=casketdb-ca" -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=n1" -keyout n1.key -out n1.csr
printf "subjectAltName=DNS:n1\nextendedKeyUsage=serverAuth,clientAuth\n" > n1.ext
openssl x509 -req -in n1.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 -extfile n1.ext -out n1.crt
```

All nodes of a cluster use TLS or none does: a node without TLS, or with a certificate from another CA, cannot talk to the others. To turn TLS on in a running cluster, restart all nodes with the flags. A certificate is read at start, so renewing it means restarting the node. To move to a new CA, first put both CAs into every node's `-raft-tls-ca` file and restart the nodes one by one, then switch the node certificates.

These flags cover only traffic between nodes. Serve clients over TLS with `-tls-addr`; see [TLS for clients](configuration.md#tls-for-clients).

## Upgrades

Nodes negotiate the version of the protocol they speak to each other, so a release that keeps the protocol compatible can be rolled through the cluster one node at a time. The [changelog](../CHANGELOG.md) says when a release breaks compatibility and all nodes have to be upgraded together, as the move from v0.10 does.

## Running without fsync

`-raft-unsafe-no-fsync` skips the fsync of Raft log segments, like `--unsafe-no-fsync` in etcd. Term, vote, log bounds and snapshots are still fsynced. A process crash stays safe, because the data is in the OS cache. A power loss is not: the node forgets acknowledged entries, and if the leader then fails, a new leader can be elected without them. The server logs a warning when the flag is set.

## Expiry

Each node expires keys by its own clock. On a follower, a key can stay visible for as long as the clocks differ. Keep node clocks in sync with NTP.

## Limitations

What clusters cannot do yet, such as sharding across nodes, is listed in [limitations](limitations.md).
