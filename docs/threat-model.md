# Threat model

This page lists what CasketDB protects, from whom, and what it leaves to the deployment. [SECURITY.md](../SECURITY.md) describes the settings; this page explains which attacker each of them stops.

## Assets

- **Data:** keys and values in memory, in the data files, in the Raft log and in snapshots.
- **Credentials:** user passwords, stored in `SYSTEM` as SHA-256 hashes, and the TLS private keys.
- **The replicated log:** every node of a cluster applies the same entries; whoever can append to it changes the data on every node.
- **Availability** of the client and Raft ports.

## Trust boundaries

| Boundary | Channel | Protection |
| --- | --- | --- |
| Client and server | `-addr`, plain text | Password or user, protected mode, AUTH limit; keep it on loopback |
| Client and server | `-tls-addr` | TLS 1.2 or 1.3, client certificates with `-tls-ca` |
| Node and node | Raft port | Mutual TLS with `-raft-tls-*`: a node proves its id with a certificate from the cluster CA |
| Server and host | Data and Raft directories, flags, environment, logs, `/metrics` | File modes `0700` and `0600`; the operating system |

## Who is trusted

- **Root and the account CasketDB runs as, on every node.** They can read memory and files, change flags and replace the binary.
- **Administrators:** users allowed to run `acl`, `config`, `raft`, `flushdb` or `+@all`. Audit records name them, but nothing limits them.
- **Nodes holding a certificate from the cluster CA.** They are trusted to follow the Raft protocol.
- **The Go standard library, github.com/acneism/raft, github.com/acneism/lua and github.com/acneism/lua-libm,** the only code the server runs besides its own.

## Threats

| Attacker | Threat | Defense | What remains |
| --- | --- | --- | --- |
| Anyone who reaches the client port | Uses the server without a password | Loopback by default; [protected mode](configuration.md#protected-mode) refuses other hosts while `default` has no password | `-protected-mode=false` or a weak password opens it again |
| | Reads the traffic or steals the password | TLS on `-tls-addr`; a warning when `-addr` takes clients in plain text on a non-loopback address | `-addr` stays plain text: keep it on loopback or turn it off |
| | Guesses a password | 10 failed AUTH attempts per second per address, an IPv6 /64 counting as one; `AUTH failed` audit records | Many addresses together guess faster: use long random passwords |
| | Exhausts memory or connections before authenticating | 10 arguments of up to 16 KB per command until AUTH, `-maxclients`, `-timeout` | `-timeout` is off by default; there is no limit per address |
| A client with a limited user | Runs commands or touches keys beyond its rights | [ACL](commands.md#access-control) by command, category and key pattern; `NOPERM`; `ACL LOG` | No read-only or write-only key patterns, no rules for single subcommands |
| | Overloads the server | `-proto-max-bulk-len`; KEYS and SCAN lock one shard at a time; SCRIPT KILL and FUNCTION KILL | No limits per user; `KEYS *` on a large database builds a large reply, so keep `@dangerous` away from applications. A script can loop or allocate memory without limit, as in Redis, and one that touches undeclared keys holds every shard while it runs; SCRIPT KILL works only before its first write. Keep `@scripting` away from users that should not run code |
| Anyone with an administrator's password | Reads or deletes all data, changes users and the cluster | Audit records with the user and the client's address | Everything an administrator may do. Paths, addresses and TLS files cannot change at runtime, Lua scripts run in the Redis sandbox without `io`, `os`, `require`, `dofile` or `loadfile`, and there is no `MODULE` or `DEBUG`, so the password does not lead to writing files or running code on the host |
| Anyone who reaches a Raft port | Poses as a member: reads replicated data, disturbs elections, appends entries | [Mutual TLS](replication.md#mutual-tls-between-nodes): a node must present a certificate for its own id; a warning when Raft runs without TLS on a non-loopback address | Without mutual TLS, all of this is possible. Firewall Raft ports from clients either way |
| A local user of a node | Reads the data files | Directories `0700`, files `0600`, a warning when `-dir` or the Raft directory is open to others | On Windows, the ACLs inherited from the parent directory decide |
| | Reads the password from the process list | `CASKETDB_REQUIREPASS` instead of `-requirepass` | |
| Anyone who reaches `/metrics` | Learns about the database | Counts and sizes only, no keys or values | No authentication: bind it to loopback or a private address |
| Anyone who gets the disks, backups or copies of the files | Reads keys, values and password hashes | None inside CasketDB; see [encryption at rest](../SECURITY.md#encryption-at-rest) | Hashes are unsalted SHA-256: a short password falls to offline guessing |
| Malformed input from the network | Crashes the server or blows up its memory | The RESP parser, the decoders of data files, Raft entries and snapshots, and the ACL rules are fuzzed in CI and nightly; a command that panics closes only its own connection | |
| A vulnerable dependency | Exploits a bug outside CasketDB's code | The standard library only, except the Raft library, the Lua interpreter and its trigonometry, which themselves use only the standard library; `govulncheck` in CI and nightly | |

## Out of scope

- Root or the CasketDB account on any node, the hypervisor and cloud administrators.
- A node that holds a valid certificate and breaks the Raft protocol. Raft does not tolerate such nodes; a certificate cannot be revoked alone, so remove the node with `RAFT REMOVE` and move the cluster to a new CA.
- Side channels between processes on the same host.
- What a user does with the permissions it was given, beyond the audit record.
- Floods below the protocol, such as SYN floods: leave them to the firewall and the operating system.

## Checklist

1. Keep `-addr` on loopback, or turn it off with `-addr ""`, and serve other hosts on `-tls-addr`.
2. Give every user a long random password, and applications users limited to their commands and keys.
3. Turn on mutual TLS between nodes and firewall the Raft and metrics ports.
4. Put the data and Raft directories on an encrypted volume and encrypt backups.
5. Ship the server log off the host, so that audit records survive a compromised node.
6. Set `-timeout` if clients may leave connections idle.
