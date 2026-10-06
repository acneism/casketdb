<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/logo-dark.svg">
    <img alt="CasketDB" src="docs/images/logo.svg" width="420">
  </picture>
</h1>

<p align="center">
  <a href="https://github.com/acneism/casketdb/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/acneism/casketdb/actions/workflows/ci.yml/badge.svg"></a>
</p>

CasketDB is a Redis-compatible key-value store written in Go. It keeps values on disk in a Bitcask log, runs commands on all cores, and can replicate through Raft so that a failed node never takes acknowledged writes with it.

Any Redis client — `redis-cli`, `redis-benchmark`, go-redis, redis-py — works with CasketDB unchanged, within the [supported commands](docs/commands.md).

> **Status: early.** CasketDB is covered by unit, model and fuzz tests and by fault-injection tests that check linearizability under `kill -9`, network partitions and membership changes, but it has not run in production yet. It supports string commands only. Read [limitations](docs/limitations.md) before relying on it.
>
> The project was called BitKV until 2026-09-28.

## Contents

- [Why CasketDB](#why-casketdb)
- [Quick start](#quick-start)
- [Running a cluster](#running-a-cluster)
- [When to use it](#when-to-use-it)
- [Documentation](#documentation)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## Why CasketDB

- **Data larger than RAM.** Values live on disk; memory holds only the key index (about 80–100 bytes plus the key length per key). Restart reads keys from hint files, not the values.
- **All cores.** 1024 lock shards instead of a single command thread, and four parallel logs with group commit.
- **No lost acknowledged writes in a cluster.** With 3 or 5 nodes, a write is acknowledged only after a majority has it in the Raft log. Redis replication is asynchronous; CasketDB's is not.
- **Familiar durability.** `appendfsync always | everysec | no` with Redis semantics. An acknowledged write survives `kill -9` in every mode and a power loss according to the fsync policy.
- **Small and dependency-light.** One binary. The only external dependency of the server is the Raft library [github.com/acneism/raft](https://github.com/acneism/raft); the tests also use [Porcupine](https://github.com/anishathalye/porcupine).

## Quick start

Build from source (Go 1.26.1 or newer):

```bash
git clone https://github.com/acneism/casketdb
cd casketdb
go build -o casketdb ./cmd/casketdb
./casketdb -dir data
```

Connect with any Redis client:

```bash
redis-cli SET greeting hello
redis-cli GET greeting
```

The server listens on `127.0.0.1:6379` by default; change it with `-addr`. On a non-loopback address, set a password with `-requirepass` or the `CASKETDB_REQUIREPASS` environment variable; without one, [protected mode](docs/configuration.md#protected-mode) refuses clients from other hosts. All flags are listed in [configuration](docs/configuration.md).

## Running a cluster

Start three nodes with the same `-raft-peers`, the same password and empty directories. The cluster forms by itself:

```bash
CASKETDB_REQUIREPASS=secret casketdb -addr 10.0.0.1:6379 -dir data -raft-id n1 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
CASKETDB_REQUIREPASS=secret casketdb -addr 10.0.0.2:6379 -dir data -raft-id n2 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
CASKETDB_REQUIREPASS=secret casketdb -addr 10.0.0.3:6379 -dir data -raft-id n3 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
```

Only the leader accepts writes; followers answer `-READONLY`. `INFO replication` shows the leader's id and address. Every node serves reads: by default they may lag behind the leader, and with `-raft-reads linearizable` they reflect every acknowledged write. Nodes can be added and removed at runtime with `RAFT` commands, and talk to each other over mutual TLS with `-raft-tls-*`. Details are in [replication](docs/replication.md).

## When to use it

CasketDB fits when:

- the data is larger than RAM, but the keys fit;
- losing an acknowledged write after a failover is unacceptable — idempotency keys, balances, counters, sessions, rate limits;
- you want one multi-core node without setting up Redis Cluster.

## Documentation

User guides:

- [Commands and differences from Redis](docs/commands.md)
- [Configuration](docs/configuration.md)
- [Persistence and recovery](docs/persistence.md)
- [Replication](docs/replication.md)
- [Limitations and roadmap](docs/limitations.md)

Design and development:

- [Architecture and on-disk format](docs/architecture.md)
- [Benchmarks](docs/benchmarks.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## Performance

On a 4-core laptop (AMD Ryzen 5 3500U, WSL2), measured with `redis-benchmark` against Redis 7.2 with AOF `everysec`:

- Pipelined SET and GET from one client run at 1.2 and 1.1 times Redis's rate. From four clients they run at 1.3 and 1.5 times, because CasketDB uses every core.
- Commands on one hot list, set, hash or sorted set run at 0.25–0.5 of Redis.
- A three-node cluster writes 1.6–3.1 times as fast as etcd 3.7 on the same machine and reads 3.7–5.6 times as fast.

See [benchmarks](docs/benchmarks.md) for the method and all numbers.

## Contributing

Bug reports and pull requests are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues privately as described in [SECURITY.md](SECURITY.md).

## License

Apache License 2.0, see [LICENSE](LICENSE). Parts follow algorithms of Redis under its BSD license; see [NOTICE](NOTICE).
