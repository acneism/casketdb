# Contributing to CasketDB

Thanks for helping. This page explains how to report problems, build and test CasketDB, and what a change needs before it is merged. Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).

## Reporting bugs

Open a GitHub issue with:

- the version (`casketdb_version` in `INFO`) and the commit, if you built from source;
- the OS and the flags you started the server with;
- the smallest sequence of commands that shows the problem, what you expected and what happened;
- the server log around the failure.

Report security problems privately, as described in [SECURITY.md](SECURITY.md), not in an issue.

## Development setup

You need Go 1.26.1 or newer. Nothing else: no cgo and no code generation.

```bash
git clone https://github.com/acneism/casketdb
cd casketdb
go build ./...
go build -o casketdb ./cmd/casketdb
```

The Raft library is a regular module dependency, [github.com/acneism/raft](https://github.com/acneism/raft). To change both at once, use a `go.work` file or a local `replace` directive, and do not commit it.

## Running tests

```bash
go vet ./...
go test ./...
```

Run both on Linux and on Windows when your change touches files, fsync, memory mapping or locking: these paths differ between the two systems.

The replica tests run three-node clusters with a 500 ms election timeout. On a machine whose fsync stalls for longer, a leader can lose its followers mid-test and a write fails with a leadership change; set `CASKETDB_TEST_ELECTION_TICKS` (in milliseconds) to raise it. The Windows CI runners use 2000.

The race detector needs cgo, so run it on Linux (on Windows, use WSL):

```bash
go test -race ./...
```

To test a Linux build from Windows without installing Go in WSL, cross-compile the test binary and run it there:

```bash
GOOS=linux go test -c -o replica.test ./internal/replica
wsl ./replica.test
```

### Differential tests

`TestDifferential` in `internal/server` sends the same random commands, over all data types, to an in-process CasketDB and to a real Redis 7.2, and compares the replies. Replies whose order Redis leaves open, such as SMEMBERS and HGETALL, are sorted first, and the differences that [commands](docs/commands.md#differences-from-redis) documents are tolerated. A difference prints the command, both replies and the earlier commands on the same keys. The test is skipped unless `CASKETDB_REDIS_ADDR` names a Redis server, which it flushes:

```bash
CASKETDB_REDIS_ADDR=127.0.0.1:6379 go test -run TestDifferential -v ./internal/server
```

`CASKETDB_DIFF_SEED` picks another sequence of commands and `CASKETDB_DIFF_STEPS` sets its length, 20,000 by default.

### Fault-injection tests

`cmd/casketdb` holds end-to-end tests that run real `casketdb` processes: three nodes, eight RESP clients and a nemesis. The clients work on strings (SET, GET, INCR, DEL), fields of hashes, members of sorted sets and sets, lists used as queues (RPUSH, LPOP, LINDEX, LLEN) and streams (XADD, XLEN, XREVRANGE), each also inside MULTI/EXEC. Hashes, sorted sets, sets and lists start each epoch with 200 elements, so every element is a record of its own. They check the recorded history for linearizability with [Porcupine](https://github.com/anishathalye/porcupine). They are skipped unless a duration is given:

```bash
go test ./cmd/casketdb -run TestFaults -timeout 30m -args -fault.duration=5m
go test ./cmd/casketdb -run TestMembershipChanges -timeout 30m -args -member.duration=5m
```

`TestFaults` kills nodes with `kill -9`, cuts network links between nodes in one or both directions through a proxy, and transfers leadership. `TestMembershipChanges` adds nodes with `-raft-join` and removes voters while the load runs. A run of four minutes or longer is split into epochs of at least two minutes. Each epoch uses its own keys, ends with the faults healed and every key read back, and is checked on its own. An operation with an unknown outcome stays open until the end of its history, and the memory the check needs grows fast with the number of open operations on a key: a single ten-minute history took more than 5 GB. Other flags: `-fault.reads=lease` runs the nodes with lease reads, `-fault.nosync` with `-raft-unsafe-no-fsync`, `-fault.bin` uses a prebuilt binary, `-fault.out` sets where the Porcupine visualization of a failure goes. Node logs stay in the test's temporary directory, printed on failure.

On Linux without a Go toolchain, cross-compile both the server and the test:

```bash
GOOS=linux go build -o casketdb-linux ./cmd/casketdb
GOOS=linux go test -c -o fault.test ./cmd/casketdb
wsl ./fault.test -test.run TestFaults -test.timeout 30m -fault.duration=5m -fault.bin=./casketdb-linux
```

### Continuous integration

[GitHub Actions](.github/workflows/ci.yml) runs on every push to `main` and every pull request:

- `go mod tidy -diff`, [golangci-lint](#linters) and govulncheck;
- `go vet` and `go test` on Linux, Windows and macOS; on Linux with coverage, listed in the run's summary. Coverage of `internal/bitcask`, `internal/replica` and `internal/server` below 80% fails the run;
- `go test -race` on Linux;
- the [differential test](#differential-tests) against a `redis:7.2` service container;
- the fuzz tests: one minute of `FuzzReadCommand` (the RESP reader) and 30 seconds each of `FuzzACLRules` (ACL rules stored in `SYSTEM` and loaded back), `FuzzScanner` (records of a data file), `FuzzEntry` (Raft entries) and `FuzzSnapshotInfo` (the file list of a snapshot).

The [nightly workflow](.github/workflows/nightly.yml), which can also be started by hand, runs govulncheck, so a new advisory shows up without a push, `TestFaults` with linearizable reads, `TestFaults` with lease reads and `TestMembershipChanges`, ten minutes each, and every fuzz test for ten minutes. A failed run keeps the node logs, the Porcupine visualization or the failing fuzz input as build artifacts. To fuzz locally, run for example `go test -run '^$' -fuzz '^FuzzEntry$' -fuzztime 5m ./internal/replica`; a failing input lands in the package's `testdata/fuzz` directory and becomes a regression test once committed.

`main` is protected: a commit lands there only after CI has passed on it, on a branch that is up to date with `main`. Push a branch, open a pull request, wait for a green run, then merge.

### Benchmarks

Benchmarks live next to the code, for example:

```bash
go test -run '^$' -bench 'BenchmarkReplicatedSet' ./internal/replica
```

Fsync time varies a lot between runs. When you compare two builds, alternate their runs instead of running one after the other; see [benchmarks](docs/benchmarks.md#methodology-notes).

## Code style

- Code is formatted with `gofmt` and passes `go vet` and the [linters](#linters).
- **No comments in code, tests included.** Names and structure carry the intent. Build constraints such as `//go:build` are not comments and stay.
- Match the surrounding code: naming, error handling, test helpers.
- Code outside `internal/replica` uses only the standard library; the fault-injection tests also use Porcupine. Discuss a new dependency in an issue first.
- Everything that knows about Raft stays in `internal/replica`.
- A command is a handler plus one entry in the table at the top of its file: `keys.go`, `strings.go`, `multi.go`, `raft.go`, or `commands.go` for connection and server commands.

### Linters

[.golangci.yml](.golangci.yml) configures [golangci-lint](https://golangci-lint.run/) v2: errcheck, govet, ineffassign, staticcheck and unused, plus gocritic, revive without its rules about comments, and gofmt. CI runs v2.14; install it as the golangci-lint documentation describes, then:

```bash
golangci-lint run
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```

Handle every error. When ignoring one is right, say so with `_ =`. The linter does not flag unchecked `Close`, `Flush` and `os.Remove`; check them anyway wherever a failure can lose data, as when closing a file that was written. Fix a finding rather than silencing it; a new exclusion in the config needs a reason in the pull request.

## Tests for a change

- Every behavior change comes with a test that fails without it. `go test -cover ./...` shows coverage; a package's own tests count, not tests of the packages above it.
- Use the manual clock from `internal/clock` instead of sleeping on wall time.
- A change to the write path, recovery or merge needs a crash test: a torn tail, a cut batch or a disk image taken at the point of failure. `internal/bitcask/multilog_test.go` has helpers for this.
- A change to the on-disk format must keep opening existing data directories, or bump the format in `META` and migrate.

## Commits and pull requests

- One logical change per commit.
- Subject in the imperative mood, about 72 characters at most, no trailing period: `Track the durable Raft index in Bitcask records`. Explain the why in the body when it is not obvious.
- Open a pull request from a branch. Say what changed and how you tested it: Windows, Linux, `-race`.
- Update the documentation in [docs/](docs/README.md) and the [changelog](CHANGELOG.md) together with the code.

## Where to start reading

[Architecture](docs/architecture.md) describes the packages, the write and read paths, and the on-disk format. The [architecture decisions](docs/adr/README.md) explain why; a change that reverses one, or makes a new choice of the same weight, adds a record there.
