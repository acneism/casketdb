package server

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	trackingMaxKeys     = 1000000
	invalidationChannel = "__redis__:invalidate"
)

type tracker struct {
	redirect int64
	bcast    bool
	prefixes []string
	optin    bool
	optout   bool
	noloop   bool
}

type trackingTable struct {
	mu       sync.Mutex
	trackers map[int64]trackTarget
	bcast    map[int64]trackTarget
	keys     map[string]map[int64]struct{}
	active   atomic.Int64
}

type trackTarget struct {
	c *client
	t *tracker
}

func (s *Server) tracker(c *client) *tracker {
	s.tracking.mu.Lock()
	defer s.tracking.mu.Unlock()
	return s.tracking.trackers[c.id].t
}

func (s *Server) setTracker(c *client, t *tracker) {
	s.tracking.mu.Lock()
	defer s.tracking.mu.Unlock()
	if s.tracking.trackers == nil {
		s.tracking.trackers = make(map[int64]trackTarget)
		s.tracking.bcast = make(map[int64]trackTarget)
	}
	delete(s.tracking.trackers, c.id)
	delete(s.tracking.bcast, c.id)
	if t != nil {
		s.tracking.trackers[c.id] = trackTarget{c, t}
		if t.bcast {
			s.tracking.bcast[c.id] = trackTarget{c, t}
		}
	}
	s.tracking.active.Store(int64(len(s.tracking.trackers)))
}

func (s *Server) takeTargets(key string) []trackTarget {
	var out []trackTarget
	for id := range s.tracking.keys[key] {
		if tg, ok := s.tracking.trackers[id]; ok && !tg.t.bcast {
			out = append(out, tg)
		}
	}
	delete(s.tracking.keys, key)
	return out
}

func (s *Server) trackRead(c *client, cmd command, args [][]byte, caching int) {
	if cmd.kind != kindRead || s.tracking.active.Load() == 0 {
		return
	}
	var kb [8]string
	keys := cmd.keys.extract(args, kb[:0])
	var evicted map[string][]trackTarget
	s.tracking.mu.Lock()
	t := s.tracking.trackers[c.id].t
	if t == nil || t.bcast || t.optin && caching != 1 || t.optout && caching == -1 {
		s.tracking.mu.Unlock()
		return
	}
	if s.tracking.keys == nil {
		s.tracking.keys = make(map[string]map[int64]struct{})
	}
	for _, key := range keys {
		if s.tracking.keys[key] == nil {
			for old := range s.tracking.keys {
				if len(s.tracking.keys) < trackingMaxKeys {
					break
				}
				if evicted == nil {
					evicted = make(map[string][]trackTarget)
				}
				evicted[old] = s.takeTargets(old)
			}
			s.tracking.keys[key] = make(map[int64]struct{})
		}
		s.tracking.keys[key][c.id] = struct{}{}
	}
	s.tracking.mu.Unlock()
	for key, targets := range evicted {
		for _, tg := range targets {
			s.sendInvalidation(tg, []byte(key), false)
		}
	}
}

func (s *Server) invalidate(key string) {
	if s.tracking.active.Load() == 0 {
		return
	}
	s.tracking.mu.Lock()
	targets := s.takeTargets(key)
	for _, tg := range s.tracking.bcast {
		if len(tg.t.prefixes) == 0 || slices.ContainsFunc(tg.t.prefixes, func(p string) bool { return strings.HasPrefix(key, p) }) {
			targets = append(targets, tg)
		}
	}
	s.tracking.mu.Unlock()
	for _, tg := range targets {
		s.sendInvalidation(tg, []byte(key), false)
	}
}

func (s *Server) invalidateAll() {
	if s.tracking.active.Load() == 0 {
		return
	}
	s.tracking.mu.Lock()
	s.tracking.keys = nil
	targets := slices.Collect(maps.Values(s.tracking.trackers))
	s.tracking.mu.Unlock()
	for _, tg := range targets {
		s.sendInvalidation(tg, nil, true)
	}
}

func (s *Server) sendInvalidation(tg trackTarget, key []byte, all bool) {
	to := tg.c
	if tg.t.redirect != 0 {
		s.mu.Lock()
		to = s.byID[tg.t.redirect]
		s.mu.Unlock()
		if to == nil {
			if q := tg.c.out.Load(); q != nil && tg.c.proto.Load() == 3 {
				q.push([]byte(">2\r\n$21\r\ntracking-redir-broken\r\n:" + strconv.FormatInt(tg.t.redirect, 10) + "\r\n"))
			}
			return
		}
	}
	q := to.out.Load()
	if q == nil {
		return
	}
	resp3 := to.proto.Load() == 3
	value := "*1\r\n$" + strconv.Itoa(len(key)) + "\r\n" + string(key) + "\r\n"
	switch {
	case all && resp3:
		value = "_\r\n"
	case all:
		value = "$-1\r\n"
	}
	switch {
	case resp3:
		q.push([]byte(">2\r\n$10\r\ninvalidate\r\n" + value))
	case tg.t.redirect != 0 && s.subs.has(subChannel, invalidationChannel, to):
		q.push([]byte("*3\r\n$7\r\nmessage\r\n$20\r\n" + invalidationChannel + "\r\n" + value))
	}
}

func (ps *subscriptions) has(kind int, name string, c *client) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	_, ok := ps.sets[kind][name][c]
	return ok
}

func clientTracking(s *Server, c *client, args [][]byte) reply {
	t := &tracker{}
	for j := 3; j < len(args); j++ {
		more := len(args) - 1 - j
		switch opt := upper(args[j]); {
		case opt == "REDIRECT" && more > 0:
			j++
			if t.redirect != 0 {
				return errorReply("ERR A client can only redirect to a single other client")
			}
			id, ok := parseInt(args[j])
			if !ok {
				return errorReply(errNotInteger)
			}
			s.mu.Lock()
			_, exists := s.byID[id]
			s.mu.Unlock()
			if !exists {
				return errorReply("ERR The client ID you want redirect to does not exist")
			}
			t.redirect = id
		case opt == "BCAST":
			t.bcast = true
		case opt == "OPTIN":
			t.optin = true
		case opt == "OPTOUT":
			t.optout = true
		case opt == "NOLOOP":
			t.noloop = true
		case opt == "PREFIX" && more > 0:
			j++
			t.prefixes = append(t.prefixes, string(args[j]))
		default:
			return errorReply(errSyntax)
		}
	}
	switch upper(args[2]) {
	case "OFF":
		s.setTracker(c, nil)
		return okReply
	case "ON":
	default:
		return errorReply(errSyntax)
	}
	old := s.tracker(c)
	switch {
	case !t.bcast && len(t.prefixes) > 0:
		return errorReply("ERR PREFIX option requires BCAST mode to be enabled")
	case old != nil && old.bcast != t.bcast:
		return errorReply("ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.")
	case t.bcast && (t.optin || t.optout):
		return errorReply("ERR OPTIN and OPTOUT are not compatible with BCAST")
	case t.optin && t.optout:
		return errorReply("ERR You can't use both OPTIN and OPTOUT")
	case old != nil && (t.optin && old.optout || t.optout && old.optin):
		return errorReply("ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.")
	}
	for i, p := range t.prefixes {
		for _, q := range t.prefixes[i+1:] {
			if strings.HasPrefix(p, q) || strings.HasPrefix(q, p) {
				return errorReply("ERR Prefix '" + q + "' overlaps with another provided prefix '" + p + "'. Prefixes for a single client must not overlap.")
			}
		}
	}
	if c.w.Proto == 3 && !c.useQueue() {
		return okReply
	}
	s.setTracker(c, t)
	return okReply
}

func clientTrackingInfo(s *Server, c *client) reply {
	t := s.tracker(c)
	if t == nil {
		return mapReply{bulkReply("flags"), stringsReply{"off"}, bulkReply("redirect"), intReply(-1), bulkReply("prefixes"), stringsReply{}}
	}
	flags := stringsReply{"on"}
	for _, f := range []struct {
		set  bool
		name string
	}{{t.bcast, "bcast"}, {t.optin, "optin"}, {t.optout, "optout"}, {c.caching == 1, "caching-yes"}, {c.caching == -1, "caching-no"}, {t.noloop, "noloop"}} {
		if f.set {
			flags = append(flags, f.name)
		}
	}
	return mapReply{bulkReply("flags"), flags, bulkReply("redirect"), intReply(t.redirect), bulkReply("prefixes"), stringsReply(t.prefixes)}
}

func clientGetRedir(s *Server, c *client) reply {
	t := s.tracker(c)
	if t == nil {
		return intReply(-1)
	}
	return intReply(t.redirect)
}

func clientCaching(s *Server, c *client, arg []byte) reply {
	t := s.tracker(c)
	if t == nil || !t.optin && !t.optout {
		return errorReply("ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled")
	}
	switch upper(arg) {
	case "YES":
		if !t.optin {
			return errorReply("ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.")
		}
		c.caching = 1
	case "NO":
		if !t.optout {
			return errorReply("ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.")
		}
		c.caching = -1
	default:
		return errorReply(errSyntax)
	}
	return okReply
}
