package server

import (
	"bufio"
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/resp"
)

const errUnblocked = "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)"

var lastKeys = keySpec{first: 1, last: -2, step: 1}

var blockingCommands = map[string]command{
	"blpop":      {arity: -3, kind: kindWrite, keys: lastKeys, acl: catList | catBlocking, tx: blockingPop(true), blocks: true},
	"brpop":      {arity: -3, kind: kindWrite, keys: lastKeys, acl: catList | catBlocking, tx: blockingPop(false), blocks: true},
	"blmove":     {arity: 6, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catList | catBlocking, tx: cmdBLMove, blocks: true},
	"brpoplpush": {arity: 4, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catList | catBlocking, tx: cmdBRPopLPush, blocks: true},
	"blmpop":     {arity: -5, kind: kindWrite, keys: keySpec{numkeys: 2}, acl: catList | catBlocking, tx: cmdBLMPop, blocks: true},
	"bzpopmin":   {arity: -3, kind: kindWrite, keys: lastKeys, acl: catSortedSet | catFast | catBlocking, tx: blockingZPop(false), blocks: true},
	"bzpopmax":   {arity: -3, kind: kindWrite, keys: lastKeys, acl: catSortedSet | catFast | catBlocking, tx: blockingZPop(true), blocks: true},
	"bzmpop":     {arity: -5, kind: kindWrite, keys: keySpec{numkeys: 2}, acl: catSortedSet | catBlocking, tx: cmdBZMPop, blocks: true},
}

type blockReply struct {
	timeout time.Duration
	keys    [][]byte
	empty   reply
	retry   [][]byte
}

func (r blockReply) writeTo(w *resp.Writer) { r.empty.writeTo(w) }

func parseTimeout(arg []byte) (time.Duration, reply) {
	f, err := strconv.ParseFloat(string(arg), 64)
	switch {
	case err != nil || math.IsNaN(f):
		return 0, errorReply("ERR timeout is not a float or out of range")
	case f < 0:
		return 0, errorReply("ERR timeout is negative")
	case f*1000 > math.MaxInt64:
		return 0, errorReply("ERR timeout is out of range")
	}
	ms := min(math.Ceil(f*1000), float64(math.MaxInt64/int64(time.Millisecond)))
	return time.Duration(ms) * time.Millisecond, nil
}

func blockingPop(left bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		timeout, bad := parseTimeout(args[len(args)-1])
		if bad != nil {
			return bad, nil
		}
		keys := args[1 : len(args)-1]
		for _, key := range keys {
			l, bad, err := openList(tx, key)
			if bad != nil || err != nil {
				return bad, err
			}
			if l.len() == 0 {
				continue
			}
			v, err := l.pop(left)
			if err != nil {
				return nil, err
			}
			l.store()
			return arrayReply{bulkReply(key), bulkReply(v)}, nil
		}
		return blockReply{timeout: timeout, keys: keys, empty: nullArrayReply{}}, nil
	}
}

func cmdBLMove(tx *bitcask.Tx, args [][]byte) (reply, error) {
	from, ok1 := side(args[3])
	to, ok2 := side(args[4])
	if !ok1 || !ok2 {
		return errorReply(errSyntax), nil
	}
	return blockingMove(tx, args, from, to, args[5])
}

func cmdBRPopLPush(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return blockingMove(tx, args, false, true, args[3])
}

func blockingMove(tx *bitcask.Tx, args [][]byte, from, to bool, timeoutArg []byte) (reply, error) {
	timeout, bad := parseTimeout(timeoutArg)
	if bad != nil {
		return bad, nil
	}
	r, err := lmove(tx, args, from, to)
	if _, empty := r.(nullReply); !empty || err != nil {
		return r, err
	}
	return blockReply{timeout: timeout, keys: args[1:2], empty: nilReply}, nil
}

func cmdBLMPop(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return blockingMPop(tx, args, "LEFT", "RIGHT", lmpop)
}

func cmdBZMPop(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return blockingMPop(tx, args, "MIN", "MAX", zmpop)
}

func blockingMPop(tx *bitcask.Tx, args [][]byte, first, last string, pop func(*bitcask.Tx, mpop) (reply, error)) (reply, error) {
	p, bad := parseMPop(args, 2, first, last)
	if bad != nil {
		return bad, nil
	}
	timeout, bad := parseTimeout(args[1])
	if bad != nil {
		return bad, nil
	}
	r, err := pop(tx, p)
	if _, empty := r.(nullArrayReply); !empty || err != nil {
		return r, err
	}
	return blockReply{timeout: timeout, keys: p.keys, empty: nullArrayReply{}}, nil
}

func blockingZPop(highest bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		timeout, bad := parseTimeout(args[len(args)-1])
		if bad != nil {
			return bad, nil
		}
		keys := args[1 : len(args)-1]
		for _, key := range keys {
			z, bad, err := openZSet(tx, key)
			if bad != nil || err != nil {
				return bad, err
			}
			if z.len() == 0 {
				continue
			}
			items, err := z.pop(1, highest)
			if err != nil {
				return nil, err
			}
			return arrayReply{bulkReply(key), bulkReply(items[0].member), scoreReply(items[0].score)}, nil
		}
		return blockReply{timeout: timeout, keys: keys, empty: nullArrayReply{}}, nil
	}
}

type waiter struct {
	keys  []string
	ready chan struct{}
}

type blockedClients struct {
	mu      sync.Mutex
	count   atomic.Int64
	waiters map[string][]*waiter
}

func (b *blockedClients) add(keys [][]byte) *waiter {
	w := &waiter{ready: make(chan struct{}, 1)}
	b.mu.Lock()
	if b.waiters == nil {
		b.waiters = make(map[string][]*waiter)
	}
	for _, key := range keys {
		w.keys = append(w.keys, string(key))
		b.waiters[string(key)] = append(b.waiters[string(key)], w)
	}
	b.mu.Unlock()
	b.count.Add(1)
	return w
}

func (b *blockedClients) remove(w *waiter) {
	b.mu.Lock()
	for _, key := range w.keys {
		if q := slices.DeleteFunc(b.waiters[key], func(x *waiter) bool { return x == w }); len(q) > 0 {
			b.waiters[key] = q
		} else {
			delete(b.waiters, key)
		}
	}
	b.mu.Unlock()
	b.count.Add(-1)
	for _, key := range w.keys {
		b.signal(key)
	}
}

func (b *blockedClients) signal(key string) {
	if b.count.Load() == 0 {
		return
	}
	b.mu.Lock()
	if q := b.waiters[key]; len(q) > 0 {
		q[0].wake()
	}
	b.mu.Unlock()
}

func (b *blockedClients) wakeAll() {
	b.mu.Lock()
	for _, q := range b.waiters {
		for _, w := range q {
			w.wake()
		}
	}
	b.mu.Unlock()
}

func (w *waiter) wake() {
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

func (s *Server) block(c *client, cmd command, args [][]byte, b blockReply) reply {
	w := s.blocked.add(b.keys)
	defer s.blocked.remove(w)
	if err := c.w.Flush(); err != nil {
		c.quit = true
		return b.empty
	}
	gone, stop := c.watch()
	defer stop()
	var expired <-chan time.Time
	if b.timeout > 0 {
		t := time.NewTimer(b.timeout)
		defer t.Stop()
		expired = t.C
	}
	for {
		if b.retry != nil {
			args = b.retry
		}
		r, err := s.call(cmd, args)
		switch {
		case errors.Is(err, replica.ErrNotLeader), errors.Is(err, replica.ErrLeadershipLost):
			return errorReply(errUnblocked)
		case err != nil:
			return storageError(err)
		}
		if _, still := r.(blockReply); !still {
			return r
		}
		c.release()
		select {
		case <-w.ready:
			c.hold()
		case <-expired:
			return nullArrayReply{}
		case <-gone:
			c.quit = true
			return nullArrayReply{}
		case <-s.done:
			c.quit = true
			return nullArrayReply{}
		}
	}
}

func (c *client) watch() (<-chan struct{}, func()) {
	gone, exited := make(chan struct{}), make(chan struct{})
	var stopping atomic.Bool
	dc, _ := c.conn.(*deadlineConn)
	if dc != nil {
		dc.paused.Store(true)
	}
	_ = c.conn.SetReadDeadline(time.Time{})
	go func() {
		defer close(exited)
		if err := c.r.Watch(); !stopping.Load() && !errors.Is(err, bufio.ErrBufferFull) {
			close(gone)
		}
	}()
	return gone, func() {
		stopping.Store(true)
		_ = c.conn.SetReadDeadline(time.Now())
		<-exited
		_ = c.conn.SetReadDeadline(time.Time{})
		if dc != nil {
			dc.paused.Store(false)
		}
	}
}
