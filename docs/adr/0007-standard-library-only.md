# 7. The standard library only, outside the Raft adapter

Status: accepted, 2026-09-25. [ADR 13](0013-lua-scripting.md) adds github.com/acneism/lua to the server.

## Context

Every dependency brings updates, advisories and code nobody here has read. A database lives for years and holds other people's data.

## Decision

Code outside `internal/replica` uses only the Go standard library. `internal/replica` depends on the Raft library; the fault-injection tests also use Porcupine. A new dependency is discussed in an issue first. When the standard library makes a feature cheap, it is written here: the RESP codec, the Prometheus text format of `/metrics`, configuration through flags and environment variables instead of a configuration file format.

## Consequences

- govulncheck and dependency updates rarely concern anything but Go itself.
- Some code exists that a library would provide, and it needs its own tests.
- Features that would need a large library, such as a YAML configuration file, are left out or done another way.
