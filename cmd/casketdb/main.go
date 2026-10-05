package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/server"
)

type config struct {
	addr        string
	dir         string
	fsync       string
	maxBulk     int
	requirePass string
	raftID      string
	raftPeers   string
	raftDir     string
	raftNoFsync bool
	raftReads   string
	raftDrift   float64
	raftJoin    bool
	raftCert    string
	raftKey     string
	raftCA      string
	raftListen  string
	raftTimeout time.Duration
	metricsAddr string
	tlsAddr     string
	tlsCert     string
	tlsKey      string
	tlsCA       string
	protected   bool
	maxClients  int
	timeout     time.Duration
	logLevel    string
	logFormat   string
	opts        bitcask.Options
}

func main() {
	cfg := config{opts: bitcask.DefaultOptions()}
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:6379", "TCP listen address for clients without TLS; empty turns it off")
	flag.StringVar(&cfg.tlsAddr, "tls-addr", "", "TCP listen address for TLS clients; empty turns TLS off")
	flag.StringVar(&cfg.tlsCert, "tls-cert", "", "PEM certificate for -tls-addr, read again whenever the file changes")
	flag.StringVar(&cfg.tlsKey, "tls-key", "", "PEM private key of -tls-cert")
	flag.StringVar(&cfg.tlsCA, "tls-ca", "", "PEM certificates of the CA that signs client certificates; when set, every TLS client must present one")
	flag.StringVar(&cfg.dir, "dir", "data", "data directory")
	flag.StringVar(&cfg.fsync, "appendfsync", "everysec", "fsync policy: always, everysec or no")
	flag.Int64Var(&cfg.opts.MaxFileSize, "max-file-size", cfg.opts.MaxFileSize, "data file rotation threshold in bytes")
	flag.Float64Var(&cfg.opts.MergeRatio, "merge-ratio", cfg.opts.MergeRatio, "dead bytes ratio that triggers automatic merge")
	flag.Int64Var(&cfg.opts.MergeMinBytes, "merge-min-bytes", cfg.opts.MergeMinBytes, "minimum total size for automatic merge")
	flag.DurationVar(&cfg.opts.MergeInterval, "merge-interval", cfg.opts.MergeInterval, "automatic merge check interval, 0 disables it")
	flag.IntVar(&cfg.opts.Logs, "logs", 0, "number of parallel data logs for a new database (0 means 4; an existing database keeps its own)")
	flag.IntVar(&cfg.maxBulk, "proto-max-bulk-len", 512<<20, "maximum bulk string length in bytes")
	flag.StringVar(&cfg.requirePass, "requirepass", "", "password clients must AUTH with")
	flag.BoolVar(&cfg.protected, "protected-mode", true, "while the default user has no password, refuse clients that connect from other hosts")
	flag.IntVar(&cfg.maxClients, "maxclients", 10000, "most clients connected at once; 0 means no limit")
	flag.DurationVar(&cfg.timeout, "timeout", 0, "close a client that sends nothing or reads no reply for this long; 0 turns it off")
	flag.StringVar(&cfg.raftID, "raft-id", "", "raft node id; enables replication")
	flag.StringVar(&cfg.raftPeers, "raft-peers", "", "all raft nodes including this one: id=host:port,id=host:port")
	flag.StringVar(&cfg.raftDir, "raft-dir", "", "raft log and snapshot directory (default <dir>/raft)")
	flag.StringVar(&cfg.raftListen, "raft-listen", "", "address the raft transport listens on when it differs from this node's address in -raft-peers, for example behind NAT")
	flag.DurationVar(&cfg.raftTimeout, "raft-election-timeout", time.Second, "time without a leader before a node starts an election; heartbeats go 10 times as often")
	flag.BoolVar(&cfg.raftNoFsync, "raft-unsafe-no-fsync", false, "skip fsync of the raft log: faster, but a power loss on one node followed by a leader failure can lose acknowledged writes")
	flag.StringVar(&cfg.raftReads, "raft-reads", "local", "read consistency in a cluster: local (may be stale), linearizable (confirmed by the leader) or lease (the leader answers from its lease)")
	flag.Float64Var(&cfg.raftDrift, "raft-max-clock-drift", 0.1, "largest relative difference between node clock rates that -raft-reads lease tolerates")
	flag.BoolVar(&cfg.raftJoin, "raft-join", false, "join a running cluster: add the node on the leader with RAFT ADDLEARNER and pass its reply as -raft-peers")
	flag.StringVar(&cfg.raftCert, "raft-tls-cert", "", "PEM certificate of this node for mutual TLS between nodes; its DNS name must be the node id")
	flag.StringVar(&cfg.raftKey, "raft-tls-key", "", "PEM private key for -raft-tls-cert")
	flag.StringVar(&cfg.raftCA, "raft-tls-ca", "", "PEM certificates of the CA that signs node certificates")
	flag.StringVar(&cfg.metricsAddr, "metrics-addr", "", "address of the Prometheus /metrics endpoint, for example 127.0.0.1:9121; empty turns it off")
	flag.StringVar(&cfg.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	flag.StringVar(&cfg.logFormat, "log-format", "text", "log format: text or json")
	flag.Parse()
	shadowed, err := applyEnv(flag.CommandLine, os.LookupEnv)
	logger, lerr := newLogger(cfg.logLevel, cfg.logFormat)
	if err == nil {
		err = lerr
	}
	for _, name := range shadowed {
		logger.Warn("environment variable ignored, the command-line flag wins", "variable", name)
	}
	if cfg.raftDir == "" {
		cfg.raftDir = filepath.Join(cfg.dir, "raft")
	}
	if err == nil {
		err = run(logger, cfg)
	}
	if err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config) error {
	policy, err := bitcask.ParseSyncPolicy(cfg.fsync)
	if err != nil {
		return err
	}
	cfg.opts.Sync = policy

	started := time.Now()
	db, err := bitcask.Open(cfg.dir, cfg.opts)
	if err != nil {
		return err
	}
	st := db.Stats()
	logger.Info("database loaded", "dir", cfg.dir, "keys", st.Keys, "logs", st.Logs, "files", st.DataFiles, "took", time.Since(started).Round(time.Millisecond))
	warnIfShared(logger, cfg.dir)

	var rep *replica.Node
	if cfg.raftID != "" {
		if rep, err = openReplica(cfg, db, logger); err != nil {
			return errors.Join(err, db.Close())
		}
		logger.Info("raft started", "id", cfg.raftID, "peers", cfg.raftPeers)
		warnIfShared(logger, cfg.raftDir)
		if cfg.raftNoFsync {
			logger.Warn("raft log fsync is disabled (-raft-unsafe-no-fsync): a power loss can lose acknowledged writes")
		}
		if cfg.raftCert == "" && !loopbackPeer(cfg.raftPeers, cfg.raftID) {
			logger.Warn("raft traffic between nodes is not encrypted or authenticated; set -raft-tls-cert, -raft-tls-key and -raft-tls-ca")
		}
	}

	srv := server.New(db, server.Config{MaxBulkLen: cfg.maxBulk, RequirePass: cfg.requirePass, Logger: logger, Replica: rep, TrackLatency: cfg.metricsAddr != "", ProtectedMode: cfg.protected, MaxClients: cfg.maxClients, Timeout: cfg.timeout})
	lns, err := listen(cfg, logger, srv.AuthRequired())
	if err != nil {
		return errors.Join(err, closeStore(rep, db))
	}
	var metrics *http.Server
	if cfg.metricsAddr != "" {
		mln, err := net.Listen("tcp", cfg.metricsAddr)
		if err != nil {
			for _, ln := range lns {
				ln.Close()
			}
			return errors.Join(err, closeStore(rep, db))
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			srv.WriteMetrics(w)
		})
		metrics = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = metrics.Serve(mln) }()
		logger.Info("metrics endpoint ready", "url", "http://"+mln.Addr().String()+"/metrics")
	}
	serveErr := make(chan error, len(lns))
	attrs := []any{"appendfsync", policy.String(), "auth", srv.AuthRequired(), "version", server.Version}
	for i, ln := range lns {
		key := "addr"
		if cfg.tlsAddr != "" && i == len(lns)-1 {
			key = "tls_addr"
		}
		attrs = append(attrs, key, ln.Addr().String())
		go func() { serveErr <- srv.Serve(ln) }()
	}
	logger.Info("ready to accept connections", attrs...)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-signals:
		logger.Info("shutting down", "signal", sig.String())
	case err = <-serveErr:
		if errors.Is(err, server.ErrServerClosed) {
			err = nil
		}
	}
	if metrics != nil {
		metrics.Close()
	}
	srv.Close()
	if cerr := closeStore(rep, db); cerr != nil && err == nil {
		err = cerr
	}
	if err == nil {
		logger.Info("bye")
	}
	return err
}

func listen(cfg config, logger *slog.Logger, auth bool) ([]net.Listener, error) {
	switch {
	case cfg.addr == "" && cfg.tlsAddr == "":
		return nil, errors.New("set -addr, -tls-addr or both")
	case cfg.tlsAddr != "" && (cfg.tlsCert == "" || cfg.tlsKey == ""):
		return nil, errors.New("-tls-addr needs -tls-cert and -tls-key")
	case cfg.tlsAddr == "" && (cfg.tlsCert != "" || cfg.tlsKey != "" || cfg.tlsCA != ""):
		return nil, errors.New("-tls-cert, -tls-key and -tls-ca need -tls-addr")
	}
	var lns []net.Listener
	fail := func(err error) ([]net.Listener, error) {
		for _, ln := range lns {
			ln.Close()
		}
		return nil, err
	}
	if cfg.addr != "" {
		ln, err := net.Listen("tcp", cfg.addr)
		if err != nil {
			return fail(err)
		}
		lns = append(lns, ln)
		switch {
		case server.IsLoopback(ln.Addr()):
		case auth:
			logger.Warn("clients on a non-loopback address send the password in plain text; serve them on -tls-addr", "addr", ln.Addr().String())
		default:
			warnNoPassword(logger, cfg, ln.Addr())
		}
	}
	if cfg.tlsAddr != "" {
		tc, err := server.TLSConfig(cfg.tlsCert, cfg.tlsKey, cfg.tlsCA, logger)
		if err != nil {
			return fail(err)
		}
		ln, err := tls.Listen("tcp", cfg.tlsAddr, tc)
		if err != nil {
			return fail(err)
		}
		lns = append(lns, ln)
		if !auth && !server.IsLoopback(ln.Addr()) && (cfg.protected || cfg.tlsCA == "") {
			warnNoPassword(logger, cfg, ln.Addr())
		}
	}
	return lns, nil
}

func warnNoPassword(logger *slog.Logger, cfg config, addr net.Addr) {
	if cfg.protected {
		logger.Warn("protected mode: the default user has no password, so clients from other hosts are refused; set a password or -protected-mode=false", "addr", addr.String())
	} else {
		logger.Warn("clients from other hosts need no password; set -requirepass", "addr", addr.String())
	}
}

func applyEnv(fs *flag.FlagSet, lookup func(string) (string, bool)) (shadowed []string, err error) {
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	fs.VisitAll(func(f *flag.Flag) {
		name := "CASKETDB_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		v, ok := lookup(name)
		switch {
		case !ok || err != nil:
		case explicit[f.Name]:
			shadowed = append(shadowed, name)
		default:
			if serr := fs.Set(f.Name, v); serr != nil {
				err = fmt.Errorf("%s: %w", name, serr)
			}
		}
	})
	return shadowed, err
}

func loopbackPeer(peers, id string) bool {
	parsed, err := replica.ParsePeers(peers)
	if err != nil {
		return false
	}
	host, _, err := net.SplitHostPort(parsed[id])
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return host == "localhost" || ip != nil && ip.IsLoopback()
}

func warnIfShared(logger *slog.Logger, dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	if fi, err := os.Stat(dir); err == nil && fi.Mode().Perm()&0o077 != 0 {
		logger.Warn("directory is accessible to other users; restrict it with chmod 700", "dir", dir, "mode", fi.Mode().Perm().String())
	}
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lv slog.Level
	err := lv.UnmarshalText([]byte(level))
	if err != nil {
		err = fmt.Errorf("-log-level: %w", err)
	}
	opts := &slog.HandlerOptions{Level: lv}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), err
	}
	if format != "text" && err == nil {
		err = fmt.Errorf("-log-format %q: want text or json", format)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts)), err
}

func openReplica(cfg config, db *bitcask.DB, logger *slog.Logger) (*replica.Node, error) {
	peers, err := replica.ParsePeers(cfg.raftPeers)
	if err != nil {
		return nil, err
	}
	reads, err := replica.ParseReadMode(cfg.raftReads)
	if err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	switch {
	case cfg.raftCert != "" && cfg.raftKey != "" && cfg.raftCA != "":
		if tlsConfig, err = replica.TLSConfig(cfg.raftID, cfg.raftCert, cfg.raftKey, cfg.raftCA); err != nil {
			return nil, err
		}
	case cfg.raftCert != "" || cfg.raftKey != "" || cfg.raftCA != "":
		return nil, errors.New("-raft-tls-cert, -raft-tls-key and -raft-tls-ca go together")
	}
	return replica.Open(db, replica.Config{
		ID:              cfg.raftID,
		Peers:           peers,
		Listen:          cfg.raftListen,
		Dir:             cfg.raftDir,
		Logger:          logger,
		UnsafeNoFsync:   cfg.raftNoFsync,
		TLS:             tlsConfig,
		Reads:           reads,
		MaxClockDrift:   cfg.raftDrift,
		Join:            cfg.raftJoin,
		ElectionTimeout: cfg.raftTimeout,
	})
}

func closeStore(rep *replica.Node, db *bitcask.DB) error {
	var err error
	if rep != nil {
		err = rep.Close()
	}
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	return err
}
