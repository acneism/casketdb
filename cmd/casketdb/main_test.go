package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/testcert"
)

func TestListen(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	for name, cfg := range map[string]config{
		"no address":                        {},
		"TLS address without a key":         {tlsAddr: "127.0.0.1:0", tlsCert: "server.crt"},
		"certificate without a TLS address": {addr: "127.0.0.1:0", tlsCert: "server.crt", tlsKey: "server.key"},
	} {
		if lns, err := listen(cfg, logger, true); err == nil {
			for _, ln := range lns {
				ln.Close()
			}
			t.Fatalf("%s: listen accepted the flags", name)
		}
	}
	ca := testcert.New(t, "casketdb test ca")
	cert, key := ca.Issue(t, "server", []string{"localhost"}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	lns, err := listen(config{tlsAddr: "127.0.0.1:0", tlsCert: cert, tlsKey: key}, logger, true)
	if err != nil || len(lns) != 1 {
		t.Fatalf("TLS only: %d listeners, %v", len(lns), err)
	}
	defer lns[0].Close()
	go func() {
		if c, err := lns[0].Accept(); err == nil {
			_ = c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	pem, err := os.ReadFile(ca.File)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	conn, err := tls.Dial("tcp", lns[0].Addr().String(), &tls.Config{RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatalf("TLS handshake with the listener: %v", err)
	}
	conn.Close()
}

func TestApplyEnv(t *testing.T) {
	fs := flag.NewFlagSet("casketdb", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:6379", "")
	peers := fs.String("raft-peers", "", "")
	join := fs.Bool("raft-join", false, "")
	timeout := fs.Duration("raft-election-timeout", time.Second, "")
	if err := fs.Parse([]string{"-addr", "0.0.0.0:7000"}); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"CASKETDB_ADDR":                  "10.0.0.1:6379",
		"CASKETDB_RAFT_PEERS":            "n1=a:1,n2=b:2",
		"CASKETDB_RAFT_JOIN":             "true",
		"CASKETDB_RAFT_ELECTION_TIMEOUT": "2s",
	}
	lookup := func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
	shadowed, err := applyEnv(fs, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if *addr != "0.0.0.0:7000" || *peers != "n1=a:1,n2=b:2" || !*join || *timeout != 2*time.Second {
		t.Fatalf("addr %q, peers %q, join %v, timeout %v", *addr, *peers, *join, *timeout)
	}
	if len(shadowed) != 1 || shadowed[0] != "CASKETDB_ADDR" {
		t.Fatalf("shadowed = %v, want [CASKETDB_ADDR]", shadowed)
	}
	fresh := flag.NewFlagSet("casketdb", flag.ContinueOnError)
	fresh.Duration("raft-election-timeout", time.Second, "")
	env["CASKETDB_RAFT_ELECTION_TIMEOUT"] = "soon"
	if _, err := applyEnv(fresh, lookup); err == nil || !strings.Contains(err.Error(), "CASKETDB_RAFT_ELECTION_TIMEOUT") {
		t.Fatalf("bad duration: %v", err)
	}
}

func TestRelog(t *testing.T) {
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "relogged")
	opts := bitcask.DefaultOptions()
	opts.Logs = 2
	db, err := bitcask.Open(src, opts)
	if err != nil {
		t.Fatal(err)
	}
	ops := []bitcask.Op{{Key: "t", Value: []byte("gen00000"), Kind: 0x84}}
	expireAt := time.Now().Add(time.Hour).UnixMilli()
	for i := range 300 {
		ops = append(ops, bitcask.Op{Key: fmt.Sprintf("k%d", i), Value: []byte("v"), ExpireAt: expireAt},
			bitcask.Op{Key: "t", Member: fmt.Sprintf("f%d", i), IsMember: true, Value: []byte("m")})
	}
	if err := db.Apply(ops, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.SetSystem([]byte("users")); err != nil {
		t.Fatal(err)
	}
	db.MarkApplied(42)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config{dir: src, relogTo: dst, opts: bitcask.DefaultOptions()}
	cfg.opts.Logs = 8
	logger := slog.New(slog.DiscardHandler)
	if err := relog(logger, cfg); err != nil {
		t.Fatal(err)
	}
	if err := relog(logger, cfg); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("second copy into the same directory: %v", err)
	}
	dump := func(dir string) (map[string]string, *bitcask.DB) {
		db, err := bitcask.Open(dir, bitcask.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		m := map[string]string{}
		if err := db.Dump(func(op bitcask.Op) error {
			m[op.Key+"/"+op.Member] = fmt.Sprintf("%s %d %d", op.Value, op.ExpireAt, op.Kind)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return m, db
	}
	before, _ := dump(src)
	after, copied := dump(dst)
	if len(before) != 601 || !maps.Equal(before, after) {
		t.Fatalf("copied %d records, the source has %d", len(after), len(before))
	}
	if st := copied.Stats(); st.Logs != 8 || string(copied.System()) != "users" || copied.DurableIndex() != 42 {
		t.Fatalf("copy has %d logs, system %q, durable index %d", st.Logs, copied.System(), copied.DurableIndex())
	}
}
