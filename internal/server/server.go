package server

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/resp"
)

const (
	Version      = "0.15.0"
	redisVersion = "7.2.0"
)

var ErrServerClosed = errors.New("server: closed")

type Config struct {
	MaxBulkLen    int
	RequirePass   string
	Logger        *slog.Logger
	Replica       *replica.Node
	TrackLatency  bool
	ProtectedMode bool
	MaxClients    int
	Timeout       time.Duration
}

const errMaxClients = "ERR max number of clients reached"

const denied = "DENIED CasketDB is running in protected mode because the default user has no password, so it accepts clients on the loopback interface only. " +
	"Connect from the loopback interface and set a password with CONFIG SET requirepass, or restart the server with -protected-mode=false if every client that can reach it is trusted."

type Server struct {
	db      *bitcask.DB
	cfg     Config
	log     *slog.Logger
	started time.Time

	mu        sync.Mutex
	listeners map[net.Listener]struct{}
	clients   map[*client]struct{}
	closed    bool
	done      chan struct{}
	wg        sync.WaitGroup
	blocked   blockedClients
	subs      subscriptions
	tracking  trackingTable
	byID      map[int64]*client

	nextID      atomic.Int64
	connections atomic.Int64
	rejected    atomic.Int64
	processed   atomic.Int64

	pass     atomic.Pointer[string]
	users    *users
	aclLog   aclLog
	aclMu    sync.Mutex
	throttle authThrottle
	maxBulk  atomic.Int64
	latency  histogram
}

type client struct {
	id           int64
	conn         net.Conn
	r            *resp.Reader
	w            *resp.Writer
	name         string
	user         *user
	refused      string
	quit         bool
	multi        bool
	dirty        bool
	queue        []queued
	watched      map[string]bitcask.Version
	subs         [3]map[string]bool
	out          atomic.Pointer[outQueue]
	proto        atomic.Int32
	caching      int
	multiCaching int
}

func New(db *bitcask.DB, cfg Config) *Server {
	if cfg.MaxBulkLen <= 0 {
		cfg.MaxBulkLen = 512 << 20
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{
		db:        db,
		cfg:       cfg,
		log:       logger,
		started:   time.Now(),
		listeners: make(map[net.Listener]struct{}),
		clients:   make(map[*client]struct{}),
		byID:      make(map[int64]*client),
		done:      make(chan struct{}),
		throttle:  authThrottle{hosts: make(map[string]failures)},
	}
	s.pass.Store(&cfg.RequirePass)
	s.users = newUsers(cfg.RequirePass)
	db.WatchSystem(s.loadSystem)
	db.WatchWrites(func(key string) {
		s.blocked.signal(key)
		s.invalidate(key)
	})
	db.WatchFlush(s.invalidateAll)
	if cfg.Replica != nil {
		cfg.Replica.WatchLeadership(s.blocked.wakeAll)
		cfg.Replica.WatchPublish(s.subs.publish)
	}
	if db.System() != nil && cfg.RequirePass != "" {
		logger.Warn("-requirepass is ignored: users are stored in the database; change the password with CONFIG SET requirepass or ACL SETUSER default")
	}
	s.maxBulk.Store(int64(cfg.MaxBulkLen))
	return s
}

func (s *Server) password() string {
	return *s.pass.Load()
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()
	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrServerClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.log.Warn("accept failed", "err", err, "retry_in", backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		if s.cfg.Timeout > 0 {
			conn = &deadlineConn{Conn: conn, timeout: s.cfg.Timeout}
		}
		c := &client{
			id:   s.nextID.Add(1),
			conn: conn,
			r:    resp.NewReader(conn, int(s.maxBulk.Load())),
			w:    resp.NewWriter(conn),
			user: s.autoUser(),
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return ErrServerClosed
		}
		switch {
		case s.cfg.MaxClients > 0 && len(s.clients) >= s.cfg.MaxClients:
			c.refused = errMaxClients
		case s.cfg.ProtectedMode && c.user != nil && !IsLoopback(conn.RemoteAddr()):
			c.refused = denied
		}
		s.clients[c] = struct{}{}
		s.byID[c.id] = c
		s.wg.Add(1)
		s.mu.Unlock()
		s.connections.Add(1)
		go s.serveClient(c)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	if !s.closed {
		close(s.done)
	}
	s.closed = true
	for ln := range s.listeners {
		ln.Close()
	}
	for c := range s.clients {
		c.conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

func (s *Server) autoUser() *user {
	u := s.users.get("default")
	if p := u.perms.Load(); p.enabled && p.nopass {
		return u
	}
	return nil
}

func (s *Server) AuthRequired() bool {
	return s.autoUser() == nil
}

func IsLoopback(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}

type deadlineConn struct {
	net.Conn
	timeout time.Duration
	paused  atomic.Bool
}

func (c *deadlineConn) Read(b []byte) (int, error) {
	if !c.paused.Load() {
		_ = c.SetReadDeadline(time.Now().Add(c.timeout))
	}
	return c.Conn.Read(b)
}

func (c *deadlineConn) Write(b []byte) (int, error) {
	_ = c.SetWriteDeadline(time.Now().Add(c.timeout))
	return c.Conn.Write(b)
}

func (s *Server) serveClient(c *client) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.clients, c)
		delete(s.byID, c.id)
		s.mu.Unlock()
		s.setTracker(c, nil)
		s.unsubscribeAll(c)
		c.conn.Close()
	}()
	if c.refused != "" {
		s.rejected.Add(1)
		c.w.Error(c.refused)
		c.w.Flush()
		return
	}
	var batch []queued
	for !c.quit {
		c.r.Unauthenticated = c.user == nil
		args, err := c.r.ReadCommand()
		c.hold()
		if err != nil {
			s.runBatch(c, batch)
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				c.w.Error("ERR " + pe.Error())
			}
			c.w.Flush()
			return
		}
		cmd, ok := s.batchable(c, args)
		if ok {
			batch = append(batch, queued{cmd: cmd, args: args})
			if c.r.Ready() {
				continue
			}
		}
		s.runBatch(c, batch)
		batch = batch[:0]
		if !ok && len(args) > 0 {
			s.execute(c, args)
		}
		if c.quit || c.r.Buffered() == 0 {
			if err := c.w.Flush(); err != nil {
				return
			}
			c.release()
		}
	}
}

func (s *Server) batchable(c *client, args [][]byte) (command, bool) {
	if len(args) == 0 || c.multi || c.user == nil || !c.user.perms.Load().all {
		return command{}, false
	}
	cmd, ok := lookup(args[0])
	if !ok || cmd.kind != kindWrite || cmd.global || cmd.blocks || !cmd.validArity(len(args)) {
		return command{}, false
	}
	return cmd, true
}

func (s *Server) runBatch(c *client, batch []queued) {
	switch len(batch) {
	case 0:
		return
	case 1:
		s.execute(c, batch[0].args)
		return
	}
	defer s.recoverCommand(c, batch[0].args[0])
	c.caching = 0
	var keys []string
	single := true
	for _, q := range batch {
		n := len(keys)
		keys = q.cmd.keys.extract(q.args, keys)
		single = single && len(keys)-n <= 1
	}
	scope := bitcask.Keys(keys...)
	if single {
		scope = scope.Independent()
	}
	var replies arrayReply
	start := s.clock()
	err := s.update(scope, func(tx *bitcask.Tx) (err error) {
		replies, err = runQueue(tx, batch)
		return err
	})
	s.observe(start, len(batch))
	s.processed.Add(int64(len(batch)))
	for i := range batch {
		if err != nil {
			storageError(err).writeTo(c.w)
		} else {
			replies[i].writeTo(c.w)
		}
	}
}

func (s *Server) recoverCommand(c *client, name []byte) {
	if r := recover(); r != nil {
		s.log.Error("command panicked", "cmd", truncate(name, 64), "panic", r)
		c.w.Error("ERR internal error")
		c.quit = true
	}
}

func (s *Server) execute(c *client, args [][]byte) {
	defer s.recoverCommand(c, args[0])
	cmd, ok := lookup(args[0])
	switch {
	case !ok:
		c.reject(errorReply(unknownCommand(args)))
		return
	case !cmd.validArity(len(args)):
		c.reject(errorReply("ERR wrong number of arguments for '" + strings.ToLower(string(args[0])) + "' command"))
		return
	case c.user == nil && !cmd.noAuth:
		c.reject(errorReply("NOAUTH Authentication required."))
		return
	case c.user != nil && c.user.perms.Load().deleted:
		c.quit = true
		return
	case c.user != nil && !cmd.noAuth:
		if msg := s.permission(c, cmd, args); msg != "" {
			c.reject(errorReply(msg))
			return
		}
	}
	if c.subscribed() && c.w.Proto == 2 {
		switch {
		case !subscribedCommands[cmd.name]:
			c.reject(errorReply("ERR Can't execute '" + cmd.name + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"))
			return
		case cmd.name == "ping" && len(args) <= 2:
			msg := []byte{}
			if len(args) == 2 {
				msg = args[1]
			}
			arrayReply{bulkReply("pong"), bulkReply(msg)}.writeTo(c.w)
			return
		}
	}
	caching := c.caching
	if cmd.name != "client" {
		c.caching = 0
		if cmd.name == "multi" {
			c.multiCaching = caching
		}
	}
	if c.multi && !cmd.inMulti {
		if cmd.kind == kindConn {
			c.reject(errorReply("ERR Command not allowed inside a transaction"))
			return
		}
		c.queue = append(c.queue, queued{cmd: cmd, args: args})
		c.w.Simple("QUEUED")
		return
	}
	s.trackRead(c, cmd, args, caching)
	s.processed.Add(1)
	start := s.clock()
	r := s.run(c, cmd, args)
	s.observe(start, 1)
	if b, ok := r.(blockReply); ok {
		r = s.block(c, cmd, args, b)
	}
	r.writeTo(c.w)
}

func (c *client) reject(r errorReply) {
	if c.multi {
		c.dirty = true
	}
	r.writeTo(c.w)
}

func (s *Server) run(c *client, cmd command, args [][]byte) reply {
	if cmd.kind == kindConn {
		return cmd.conn(s, c, args)
	}
	if cmd.kind == kindPure {
		r, err := cmd.tx(nil, args)
		if err != nil {
			return storageError(err)
		}
		return r
	}
	r, err := s.call(cmd, args)
	if err != nil {
		return storageError(err)
	}
	return r
}

func (s *Server) call(cmd command, args [][]byte) (reply, error) {
	var r reply
	fn := func(tx *bitcask.Tx) error {
		var err error
		r, err = cmd.tx(tx, args)
		return err
	}
	var kb [8]string
	scope := bitcask.Keys(cmd.keys.extract(args, kb[:0])...)
	if cmd.global {
		scope = bitcask.Shardwise()
	}
	var err error
	if cmd.kind == kindRead {
		err = s.view(scope, fn)
	} else {
		err = s.update(scope, fn)
	}
	return r, err
}

func (s *Server) view(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	if s.cfg.Replica != nil {
		if err := s.cfg.Replica.ReadBarrier(); err != nil {
			return err
		}
	}
	return s.db.View(scope, fn)
}

func (s *Server) update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	if s.cfg.Replica != nil {
		return s.cfg.Replica.Update(scope, fn)
	}
	return s.db.Update(scope, fn)
}

func (s *Server) flush() error {
	if s.cfg.Replica != nil {
		return s.cfg.Replica.Flush()
	}
	return s.db.Flush()
}
