package server

import (
	"maps"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/acneism/casketdb/internal/resp"
)

const (
	pubsubLimit  = 32 << 20
	replyBacklog = 1 << 20
)

const (
	subChannel = iota
	subPattern
	subShard
)

var subVerbs = [3][2]string{{"subscribe", "unsubscribe"}, {"psubscribe", "punsubscribe"}, {"ssubscribe", "sunsubscribe"}}

var pubsubCommands = map[string]command{
	"subscribe":    {arity: -2, kind: kindConn, acl: catPubSub, conn: subscribe(subChannel)},
	"unsubscribe":  {arity: -1, kind: kindConn, acl: catPubSub, conn: unsubscribe(subChannel)},
	"psubscribe":   {arity: -2, kind: kindConn, acl: catPubSub, conn: subscribe(subPattern)},
	"punsubscribe": {arity: -1, kind: kindConn, acl: catPubSub, conn: unsubscribe(subPattern)},
	"ssubscribe":   {arity: -2, kind: kindConn, acl: catPubSub, conn: subscribe(subShard)},
	"sunsubscribe": {arity: -1, kind: kindConn, acl: catPubSub, conn: unsubscribe(subShard)},
	"publish":      {arity: 3, kind: kindConn, acl: catPubSub | catFast, conn: publish(false)},
	"spublish":     {arity: 3, kind: kindConn, acl: catPubSub | catFast, conn: publish(true)},
	"pubsub":       {arity: -2, kind: kindConn, acl: catPubSub, conn: cmdPubSub},
}

var subscribedCommands = map[string]bool{
	"subscribe": true, "unsubscribe": true, "psubscribe": true, "punsubscribe": true,
	"ssubscribe": true, "sunsubscribe": true, "ping": true, "quit": true,
}

type multiReply []reply

func (r multiReply) writeTo(w *resp.Writer) {
	for _, x := range r {
		x.writeTo(w)
	}
}

type outQueue struct {
	conn    net.Conn
	mu      sync.Mutex
	drained *sync.Cond
	buf     []byte
	held    []byte
	holding bool
	dead    bool
	ready   chan struct{}
	done    chan struct{}
	exited  chan struct{}
}

func newOutQueue(conn net.Conn) *outQueue {
	q := &outQueue{conn: conn, ready: make(chan struct{}, 1), done: make(chan struct{}), exited: make(chan struct{})}
	q.drained = sync.NewCond(&q.mu)
	go q.run()
	return q
}

func (q *outQueue) Write(p []byte) (int, error) {
	q.mu.Lock()
	for len(q.buf) >= replyBacklog && !q.dead {
		q.drained.Wait()
	}
	if q.dead {
		q.mu.Unlock()
		return 0, net.ErrClosed
	}
	q.buf = append(q.buf, p...)
	q.mu.Unlock()
	q.wake()
	return len(p), nil
}

func (q *outQueue) push(p []byte) {
	q.mu.Lock()
	switch {
	case q.dead:
		q.mu.Unlock()
	case len(q.buf)+len(q.held)+len(p) > pubsubLimit:
		q.dead = true
		q.drained.Broadcast()
		q.mu.Unlock()
		q.conn.Close()
	case q.holding:
		q.held = append(q.held, p...)
		q.mu.Unlock()
	default:
		q.buf = append(q.buf, p...)
		q.mu.Unlock()
		q.wake()
	}
}

func (q *outQueue) hold() {
	q.mu.Lock()
	q.holding = true
	q.mu.Unlock()
}

func (q *outQueue) release() {
	q.mu.Lock()
	q.holding = false
	q.buf = append(q.buf, q.held...)
	q.held = nil
	q.mu.Unlock()
	q.wake()
}

func (q *outQueue) wake() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *outQueue) run() {
	defer close(q.exited)
	for stop := false; !stop; {
		select {
		case <-q.ready:
		case <-q.done:
			stop = true
		}
		q.mu.Lock()
		b := q.buf
		q.buf = nil
		q.drained.Broadcast()
		q.mu.Unlock()
		if len(b) == 0 {
			continue
		}
		if _, err := q.conn.Write(b); err != nil {
			q.mu.Lock()
			q.dead = true
			q.drained.Broadcast()
			q.mu.Unlock()
			return
		}
	}
}

func (q *outQueue) close() {
	close(q.done)
	select {
	case <-q.exited:
	case <-time.After(time.Second):
	}
}

func encodePush(parts ...[]byte) []byte {
	b := strconv.AppendInt([]byte{'*'}, int64(len(parts)), 10)
	b = append(b, "\r\n"...)
	for _, p := range parts {
		b = strconv.AppendInt(append(b, '$'), int64(len(p)), 10)
		b = append(append(append(b, "\r\n"...), p...), "\r\n"...)
	}
	return b
}

type subscriptions struct {
	mu   sync.RWMutex
	sets [3]map[string]map[*client]struct{}
}

func (ps *subscriptions) add(kind int, name string, c *client) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.sets[kind] == nil {
		ps.sets[kind] = make(map[string]map[*client]struct{})
	}
	if ps.sets[kind][name] == nil {
		ps.sets[kind][name] = make(map[*client]struct{})
	}
	ps.sets[kind][name][c] = struct{}{}
}

func (ps *subscriptions) remove(kind int, name string, c *client) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.sets[kind][name], c)
	if len(ps.sets[kind][name]) == 0 {
		delete(ps.sets[kind], name)
	}
}

func (ps *subscriptions) publish(channel, message []byte, shard bool) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	kind, verb := subChannel, "message"
	if shard {
		kind, verb = subShard, "smessage"
	}
	n := 0
	if clients := ps.sets[kind][string(channel)]; len(clients) > 0 {
		n += pushTo(clients, encodePush([]byte(verb), channel, message))
	}
	for pattern, clients := range ps.sets[subPattern] {
		if !shard && matchGlob(pattern, string(channel)) {
			n += pushTo(clients, encodePush([]byte("pmessage"), []byte(pattern), channel, message))
		}
	}
	return n
}

func pushTo(clients map[*client]struct{}, msg []byte) int {
	var resp3 []byte
	for c := range clients {
		if c.proto.Load() != 3 {
			c.out.Load().push(msg)
			continue
		}
		if resp3 == nil {
			resp3 = append([]byte{'>'}, msg[1:]...)
		}
		c.out.Load().push(resp3)
	}
	return len(clients)
}

func (ps *subscriptions) names(kind int, pattern []byte) stringsReply {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	out := stringsReply{}
	for _, name := range slices.Sorted(maps.Keys(ps.sets[kind])) {
		if pattern == nil || matchGlob(string(pattern), name) {
			out = append(out, name)
		}
	}
	return out
}

func (ps *subscriptions) counts(kind int, names [][]byte) arrayReply {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	out := arrayReply{}
	for _, name := range names {
		out = append(out, bulkReply(name), intReply(len(ps.sets[kind][string(name)])))
	}
	return out
}

func (c *client) subscriptions(kind int) int {
	if kind == subShard {
		return len(c.subs[subShard])
	}
	return len(c.subs[subChannel]) + len(c.subs[subPattern])
}

func (c *client) subscribed() bool {
	return len(c.subs[subChannel])+len(c.subs[subPattern])+len(c.subs[subShard]) > 0
}

func (c *client) useQueue() bool {
	if c.out.Load() != nil {
		return true
	}
	if err := c.w.Flush(); err != nil {
		c.quit = true
		return false
	}
	q := newOutQueue(c.conn)
	q.hold()
	w := resp.NewWriter(q)
	w.Proto, c.w = c.w.Proto, w
	c.out.Store(q)
	return true
}

func (c *client) hold() {
	if q := c.out.Load(); q != nil {
		q.hold()
	}
}

func (c *client) release() {
	if q := c.out.Load(); q != nil {
		q.release()
	}
}

func (c *client) pauseTimeout(paused bool) {
	if dc, ok := c.conn.(*deadlineConn); ok {
		dc.paused.Store(paused)
		if paused {
			_ = c.conn.SetReadDeadline(time.Time{})
		}
	}
}

func subscribe(kind int) connFunc {
	return func(s *Server, c *client, args [][]byte) reply {
		if !c.useQueue() {
			return multiReply{}
		}
		c.pauseTimeout(true)
		if c.subs[kind] == nil {
			c.subs[kind] = make(map[string]bool)
		}
		out := multiReply{}
		for _, arg := range args[1:] {
			name := string(arg)
			if !c.subs[kind][name] {
				c.subs[kind][name] = true
				s.subs.add(kind, name, c)
			}
			out = append(out, pushReply{bulkReply(subVerbs[kind][0]), bulkReply(arg), intReply(c.subscriptions(kind))})
		}
		return out
	}
}

func unsubscribe(kind int) connFunc {
	return func(s *Server, c *client, args [][]byte) reply {
		names := args[1:]
		if len(names) == 0 {
			for _, name := range slices.Sorted(maps.Keys(c.subs[kind])) {
				names = append(names, []byte(name))
			}
		}
		if len(names) == 0 {
			return pushReply{bulkReply(subVerbs[kind][1]), nilReply, intReply(c.subscriptions(kind))}
		}
		out := multiReply{}
		for _, arg := range names {
			if c.subs[kind][string(arg)] {
				delete(c.subs[kind], string(arg))
				s.subs.remove(kind, string(arg), c)
			}
			out = append(out, pushReply{bulkReply(subVerbs[kind][1]), bulkReply(arg), intReply(c.subscriptions(kind))})
		}
		if !c.subscribed() {
			c.pauseTimeout(false)
		}
		return out
	}
}

func (s *Server) unsubscribeAll(c *client) {
	for kind, names := range c.subs {
		for name := range names {
			s.subs.remove(kind, name, c)
		}
	}
	if q := c.out.Load(); q != nil {
		q.close()
	}
}

func publish(shard bool) connFunc {
	return func(s *Server, c *client, args [][]byte) reply {
		if s.cfg.Replica == nil {
			return intReply(s.subs.publish(args[1], args[2], shard))
		}
		n, err := s.cfg.Replica.Publish(args[1], args[2], shard)
		if err != nil {
			return storageError(err)
		}
		return intReply(n)
	}
}

func cmdPubSub(s *Server, c *client, args [][]byte) reply {
	var pattern []byte
	if len(args) == 3 {
		pattern = args[2]
	}
	switch sub := upper(args[1]); {
	case sub == "CHANNELS" && len(args) <= 3:
		return s.subs.names(subChannel, pattern)
	case sub == "SHARDCHANNELS" && len(args) <= 3:
		return s.subs.names(subShard, pattern)
	case sub == "NUMSUB":
		return s.subs.counts(subChannel, args[2:])
	case sub == "SHARDNUMSUB":
		return s.subs.counts(subShard, args[2:])
	case sub == "NUMPAT" && len(args) == 2:
		return intReply(len(s.subs.names(subPattern, nil)))
	case sub == "HELP" && len(args) == 2:
		return stringsReply{
			"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
			"CHANNELS [<pattern>]",
			"    Return the currently active channels matching a <pattern> (default: '*').",
			"NUMPAT",
			"    Return number of subscriptions to patterns.",
			"NUMSUB [<channel> ...]",
			"    Return the number of subscribers for the specified channels, excluding",
			"    pattern subscriptions(default: no channels).",
			"SHARDCHANNELS [<pattern>]",
			"    Return the currently active shard level channels matching a <pattern> (default: '*').",
			"SHARDNUMSUB [<shardchannel> ...]",
			"    Return the number of subscribers for the specified shard level channel(s)",
			"HELP",
			"    Print this help.",
		}
	}
	return unknownSubcommand(args)
}
