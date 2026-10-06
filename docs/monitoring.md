# Monitoring

## Prometheus metrics

Start the server with `-metrics-addr` to serve metrics in the Prometheus text format at `http://<addr>/metrics`:

```bash
casketdb -dir data -metrics-addr 127.0.0.1:9121
```

```yaml
scrape_configs:
  - job_name: casketdb
    static_configs:
      - targets: ["10.0.0.1:9121", "10.0.0.2:9121", "10.0.0.3:9121"]
```

The server times commands only while `-metrics-addr` is set. Timing costs about 100 ns per command run on its own, 10–15% of a pipelined GET served from memory and a negligible part of a request that crosses the network. A pipelined batch of writes is timed once.

The endpoint has no authentication. Bind it to a loopback or private address, or put it behind a firewall. It reveals counts and sizes, not keys or values.

| Metric | Type | Meaning |
| --- | --- | --- |
| `casketdb_build_info{version}` | gauge | Always 1; the label carries the CasketDB version |
| `casketdb_connected_clients` | gauge | Open client connections |
| `casketdb_connections_received_total` | counter | Client connections accepted since start |
| `casketdb_rejected_connections_total` | counter | Client connections refused by `-maxclients` or protected mode |
| `casketdb_commands_processed_total` | counter | Commands run since start |
| `casketdb_command_duration_seconds` | histogram | Time to run a command inside the server, from parsing to the reply, without the network. A pipelined batch of writes counts each of its commands with the batch's time. EXEC counts once |
| `casketdb_keys` | gauge | Keys in the database |
| `casketdb_keys_with_ttl` | gauge | Keys with an expiry |
| `casketdb_expired_keys_total` | counter | Keys removed by expiry |
| `casketdb_bitcask_logs` | gauge | Parallel logs |
| `casketdb_bitcask_data_files` | gauge | Data files in all logs |
| `casketdb_bitcask_total_bytes` | gauge | Size of the data files |
| `casketdb_bitcask_live_bytes` | gauge | Bytes of the records that are still current; the rest is reclaimed by merge |
| `casketdb_bitcask_merges_total` | counter | Merges since start |
| `casketdb_bitcask_writes_total` | counter | Writes to data files; one write carries a group of transactions |
| `casketdb_bitcask_fsyncs_total` | counter | Fsyncs of data files |
| `casketdb_bitcask_fsync_seconds_total` | counter | Time spent in those fsyncs |

A cluster node also exports:

| Metric | Type | Meaning |
| --- | --- | --- |
| `casketdb_raft_leader` | gauge | 1 on the leader, 0 elsewhere |
| `casketdb_raft_term` | gauge | Current term |
| `casketdb_raft_commit_index` | gauge | Highest log index known to be committed |
| `casketdb_raft_applied_index` | gauge | Highest log index applied to the database |
| `casketdb_raft_first_index` | gauge | First index still in this node's log, after compaction |
| `casketdb_raft_last_index` | gauge | Last index in this node's log |
| `casketdb_raft_snapshot_index` | gauge | Index of this node's latest snapshot, 0 if it has none |
| `casketdb_raft_voters` | gauge | Voters in the cluster |
| `casketdb_raft_learners` | gauge | Learners in the cluster |

## Useful queries

```promql
histogram_quantile(0.99, rate(casketdb_command_duration_seconds_bucket[5m]))
rate(casketdb_bitcask_fsync_seconds_total[5m]) / rate(casketdb_bitcask_fsyncs_total[5m])
max(casketdb_raft_commit_index) - casketdb_raft_applied_index
sum(casketdb_raft_leader) != 1
changes(casketdb_raft_term[10m]) > 3
```

The first is the 99th percentile command time, the second the average fsync time. The third is how far each node lags behind the cluster in log entries: a node that keeps growing this number cannot keep up. The last two catch a cluster without exactly one leader and frequent elections.

## Logs

The server logs to standard error, one record per line: key=value text by default, JSON with `-log-format json`. `-log-level` picks the lowest level written: `debug`, `info` (default), `warn` or `error`. Messages from the Raft library carry `component=raft`.

### Audit records

Logins and administrative actions are logged with `component=audit`, the user who acted and the client's address:

```text
time=2026-10-01T15:04:05.000+00:00 level=INFO msg="ACL user changed" component=audit user=default client=10.0.0.7:52144 target=app rules="[on >*** ~app:* +@read]"
```

| Message | Level | Logged when | Other fields |
| --- | --- | --- | --- |
| `AUTH succeeded` | info | `AUTH` or `HELLO … AUTH` signs a client in | `user` is the user signed in |
| `AUTH failed` | warn | The user is unknown or disabled, or the password is wrong | `user` is the name tried, cut to 64 bytes |
| `ACL user changed` | info | `ACL SETUSER` | `target`; `rules`, with passwords and hashes shown as `>***`, `<***`, `#***`, `!***` |
| `ACL users deleted` | info | `ACL DELUSER` | `targets`, `deleted` |
| `config changed` | info | `CONFIG SET` | `params`, the names without values |
| `database flushed` | info | `FLUSHDB`, `FLUSHALL` | |
| `RAFT command` | info | `RAFT TRANSFER`, `ADDLEARNER`, `PROMOTE` or `REMOVE` succeeds | `command` |

A node logs what ran on it: in a cluster, a change to users or a FLUSHDB appears only in the log of the leader that ran it. Attempts refused by the [AUTH limit](configuration.md#client-limits) and commands denied by ACL are not logged; `ACL LOG` lists the denials. Every successful AUTH writes a line, so clients that open a connection per request fill the log quickly; `-log-level warn` keeps the failed logins but drops the info records too.

`INFO` answers most of the counters and gauges in the Redis format, but not the latency histogram, the fsync time or the Raft commit and last index.
