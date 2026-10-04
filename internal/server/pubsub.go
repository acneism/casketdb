package server

import (
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/acneism/casketdb/internal/resp"
)

const pubsubLimit = 32 << 20

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
	conn   net.Conn
	mu     sync.Mutex
	buf    []byte
	dead   bool
	ready  chan struct{}
	done   chan struct{}
	exited chan struct{}
}

func newOutQueue(conn net.Conn) *outQueue {
	q := &outQueue{conn: conn, ready: make(chan struct{}, 1), done: make(chan struct{}), exited: make(chan struct{})}
	go q.run()
	return q
}

func (q *outQueue) Write(p []byte) (int, error) {
	q.mu.Lock()
	switch {
	case q.dead:
		q.mu.Unlock()
		return 0, net.ErrClosed
	case len(q.buf)+len(p) > pubsubLimit:
		q.dead = true
		q.mu.Unlock()
		q.conn.Close()
		return 0, errors.New("output buffer limit reached")
	}
	q.buf = append(q.buf, p...)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (q *outQueue) push(p []byte) {
	_, _ = q.Write(p)
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
		q.mu.Unlock()
		if len(b) == 0 {
			continue
		}
		if _, err := q.conn.Write(b); err != nil {
			q.mu.Lock()
			q.dead = true
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
		msg := encodePush([]byte(verb), channel, message)
		for c := range clients {
			c.out.push(msg)
			n++
		}
	}
	for pattern, clients := range ps.sets[subPattern] {
		if shard || !matchGlob(pattern, string(channel)) {
			continue
		}
		msg := encodePush([]byte("pmessage"), []byte(pattern), channel, message)
		for c := range clients {
			c.out.push(msg)
			n++
		}
	}
	return n
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
		if c.out == nil {
			if err := c.w.Flush(); err != nil {
				c.quit = true
				return multiReply{}
			}
			c.out = newOutQueue(c.conn)
			c.w = resp.NewWriter(c.out)
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
			out = append(out, arrayReply{bulkReply(subVerbs[kind][0]), bulkReply(arg), intReply(c.subscriptions(kind))})
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
			return arrayReply{bulkReply(subVerbs[kind][1]), nilReply, intReply(c.subscriptions(kind))}
		}
		out := multiReply{}
		for _, arg := range names {
			if c.subs[kind][string(arg)] {
				delete(c.subs[kind], string(arg))
				s.subs.remove(kind, string(arg), c)
			}
			out = append(out, arrayReply{bulkReply(subVerbs[kind][1]), bulkReply(arg), intReply(c.subscriptions(kind))})
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
	if c.out != nil {
		c.out.close()
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
