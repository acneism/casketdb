package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type category uint32

const (
	catKeyspace category = 1 << iota
	catRead
	catWrite
	catString
	catFast
	catSlow
	catAdmin
	catDangerous
	catConnection
	catTransaction
	catBitmap
	catHash
	catSet
	catList
	catSortedSet
	catBlocking
	catHyperLogLog
	catGeo
	catStream
	catPubSub
)

var categoryNames = []string{"keyspace", "read", "write", "string", "fast", "slow", "admin", "dangerous", "connection", "transaction", "bitmap", "hash", "set", "list", "sortedset", "blocking", "hyperloglog", "geo", "stream", "pubsub"}

func (cmd command) categories() category {
	c := cmd.acl
	switch cmd.kind {
	case kindRead:
		c |= catRead
	case kindWrite:
		c |= catWrite
	}
	if c&catFast == 0 {
		c |= catSlow
	}
	return c
}

func categoryByName(name string) (category, bool) {
	i := slices.Index(categoryNames, strings.ToLower(name))
	if i < 0 {
		return 0, false
	}
	return category(1) << i, true
}

var aclCommands = map[string]command{
	"acl": {arity: -2, kind: kindConn, acl: catAdmin | catDangerous, conn: cmdACL},
}

type user struct {
	name  string
	perms atomic.Pointer[perms]
}

type perms struct {
	enabled   bool
	nopass    bool
	deleted   bool
	passwords [][32]byte
	commands  []bool
	allKeys   bool
	patterns  []string
	all       bool
}

func newPerms() *perms {
	return &perms{commands: make([]bool, len(commands))}
}

func (p *perms) clone() *perms {
	c := *p
	c.passwords = slices.Clone(p.passwords)
	c.commands = slices.Clone(p.commands)
	c.patterns = slices.Clone(p.patterns)
	return &c
}

func (p *perms) setCommands(allowed bool, match func(command) bool) {
	for _, cmd := range commands {
		if match(cmd) {
			p.commands[cmd.id] = allowed
		}
	}
}

func (p *perms) apply(rule string) bool {
	lower := strings.ToLower(rule)
	switch {
	case lower == "on":
		p.enabled = true
	case lower == "off":
		p.enabled = false
	case lower == "nopass":
		p.nopass, p.passwords = true, nil
	case lower == "resetpass":
		p.nopass, p.passwords = false, nil
	case lower == "allkeys" || rule == "~*":
		p.allKeys, p.patterns = true, nil
	case lower == "resetkeys":
		p.allKeys, p.patterns = false, nil
	case lower == "allcommands" || lower == "+@all":
		p.setCommands(true, func(command) bool { return true })
	case lower == "nocommands" || lower == "-@all":
		p.setCommands(false, func(command) bool { return true })
	case lower == "reset":
		*p = *newPerms()
	case strings.HasPrefix(rule, ">"):
		p.addPassword(sha256.Sum256([]byte(rule[1:])))
	case strings.HasPrefix(rule, "<"):
		p.removePassword(sha256.Sum256([]byte(rule[1:])))
	case strings.HasPrefix(rule, "#") || strings.HasPrefix(rule, "!"):
		h, err := hex.DecodeString(rule[1:])
		if err != nil || len(h) != sha256.Size {
			return false
		}
		if rule[0] == '#' {
			p.addPassword([32]byte(h))
		} else {
			p.removePassword([32]byte(h))
		}
	case strings.HasPrefix(rule, "~"):
		if !utf8.ValidString(rule) {
			return false
		}
		if !p.allKeys {
			p.patterns = append(p.patterns, rule[1:])
		}
	case strings.HasPrefix(lower, "+@") || strings.HasPrefix(lower, "-@"):
		cat, ok := categoryByName(lower[2:])
		if !ok {
			return false
		}
		p.setCommands(lower[0] == '+', func(cmd command) bool { return cmd.categories()&cat != 0 })
	case strings.HasPrefix(lower, "+") || strings.HasPrefix(lower, "-"):
		target, ok := commands[lower[1:]]
		if !ok {
			return false
		}
		p.setCommands(lower[0] == '+', func(cmd command) bool { return cmd.id == target.id })
	default:
		return false
	}
	p.all = p.allKeys && !slices.Contains(p.commands, false)
	return true
}

func (p *perms) addPassword(h [32]byte) {
	if !slices.Contains(p.passwords, h) {
		p.passwords = append(p.passwords, h)
	}
	p.nopass = false
}

func (p *perms) removePassword(h [32]byte) {
	p.passwords = slices.DeleteFunc(p.passwords, func(x [32]byte) bool { return x == h })
}

func (p *perms) accepts(pass []byte) bool {
	if p.nopass {
		return true
	}
	h := sha256.Sum256(pass)
	ok := false
	for _, want := range p.passwords {
		ok = subtle.ConstantTimeCompare(h[:], want[:]) == 1 || ok
	}
	return ok
}

func (p *perms) keyAllowed(key string) bool {
	if p.allKeys {
		return true
	}
	for _, pattern := range p.patterns {
		if matchGlob(pattern, key) {
			return true
		}
	}
	return false
}

func (p *perms) rules() []string {
	out := []string{"off"}
	if p.enabled {
		out[0] = "on"
	}
	if p.nopass {
		out = append(out, "nopass")
	}
	for _, h := range p.passwords {
		out = append(out, "#"+hex.EncodeToString(h[:]))
	}
	return append(append(out, p.keyRules()...), p.commandRules()...)
}

func (p *perms) keyRules() []string {
	if p.allKeys {
		return []string{"~*"}
	}
	keys := make([]string, len(p.patterns))
	for i, pattern := range p.patterns {
		keys[i] = "~" + pattern
	}
	return keys
}

func (p *perms) commandRules() []string {
	if !slices.Contains(p.commands, false) {
		return []string{"+@all"}
	}
	rules := []string{"-@all"}
	for _, name := range slices.Sorted(maps.Keys(commands)) {
		if p.commands[commands[name].id] {
			rules = append(rules, "+"+name)
		}
	}
	return rules
}

type users struct {
	mu     sync.RWMutex
	byName map[string]*user
}

type systemState struct {
	Users map[string][]string `json:"users"`
}

func (us *users) clone() *users {
	us.mu.RLock()
	defer us.mu.RUnlock()
	c := &users{byName: make(map[string]*user, len(us.byName))}
	for name, u := range us.byName {
		nu := &user{name: name}
		nu.perms.Store(u.perms.Load().clone())
		c.byName[name] = nu
	}
	return c
}

func (us *users) encode() ([]byte, error) {
	st := systemState{Users: map[string][]string{}}
	us.mu.RLock()
	for name, u := range us.byName {
		st.Users[name] = u.perms.Load().rules()
	}
	us.mu.RUnlock()
	return json.Marshal(st)
}

func (us *users) load(b []byte) error {
	var st systemState
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	parsed := make(map[string]*perms, len(st.Users))
	for name, rules := range st.Users {
		p := newPerms()
		for _, rule := range rules {
			if !p.apply(rule) {
				return fmt.Errorf("server: stored rule %q of user %s", rule, name)
			}
		}
		parsed[name] = p
	}
	if parsed["default"] == nil {
		return errors.New("server: the stored users lack default")
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	for name, p := range parsed {
		u := us.byName[name]
		if u == nil {
			u = &user{name: name}
			us.byName[name] = u
		}
		u.perms.Store(p)
	}
	for name, u := range us.byName {
		if parsed[name] == nil {
			delete(us.byName, name)
			u.perms.Store(&perms{deleted: true, commands: make([]bool, len(commands))})
		}
	}
	return nil
}

func newUsers(password string) *users {
	p := newPerms()
	for _, rule := range []string{"on", "allkeys", "allcommands", "nopass"} {
		p.apply(rule)
	}
	if password != "" {
		p.apply(">" + password)
	}
	u := &user{name: "default"}
	u.perms.Store(p)
	return &users{byName: map[string]*user{"default": u}}
}

func (us *users) get(name string) *user {
	us.mu.RLock()
	defer us.mu.RUnlock()
	return us.byName[name]
}

func (us *users) names() []string {
	us.mu.RLock()
	defer us.mu.RUnlock()
	return slices.Sorted(maps.Keys(us.byName))
}

func (us *users) setPassword(password string) {
	u := us.get("default")
	p := u.perms.Load().clone()
	p.apply("resetpass")
	if password == "" {
		p.apply("nopass")
	} else {
		p.apply(">" + password)
	}
	u.perms.Store(p)
}

func (us *users) authenticate(name string, pass []byte) *user {
	u := us.get(name)
	if u == nil {
		return nil
	}
	if p := u.perms.Load(); !p.enabled || !p.accepts(pass) {
		return nil
	}
	return u
}

const (
	authFailures = 10
	authWindow   = time.Second
	errAuthLimit = "ERR too many failed AUTH attempts from this address, retry in a second"
)

type authThrottle struct {
	mu    sync.Mutex
	hosts map[string]failures
	swept time.Time
}

type failures struct {
	n     int
	until time.Time
}

func (s *Server) login(c *client, name string, pass []byte) (*user, reply) {
	u, allowed := s.throttle.attempt(hostOf(c.conn.RemoteAddr()), time.Now(), func() *user { return s.users.authenticate(name, pass) })
	switch {
	case !allowed:
		return nil, errorReply(errAuthLimit)
	case u == nil:
		s.aclLog.add("auth", "AUTH", name, c)
		s.audit(c, slog.LevelWarn, "AUTH failed", name[:min(len(name), 64)])
		return nil, errorReply(errWrongPass)
	}
	s.audit(c, slog.LevelInfo, "AUTH succeeded", u.name)
	return u, nil
}

func (t *authThrottle) attempt(host string, now time.Time, check func() *user) (*user, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.hosts[host]
	if !now.Before(f.until) {
		f = failures{until: now.Add(authWindow)}
	}
	if f.n >= authFailures {
		return nil, false
	}
	u := check()
	if u == nil {
		if now.Sub(t.swept) >= authWindow {
			maps.DeleteFunc(t.hosts, func(_ string, f failures) bool { return !now.Before(f.until) })
			t.swept = now
		}
		f.n++
		t.hosts[host] = f
	}
	return u, true
}

func (s *Server) audit(c *client, level slog.Level, msg, user string, args ...any) {
	s.log.Log(context.Background(), level, msg, append([]any{"component", "audit", "user", user, "client", c.conn.RemoteAddr().String()}, args...)...)
}

func hostOf(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	switch {
	case !ok:
		return addr.String()
	case tcp.IP.To4() == nil:
		return tcp.IP.Mask(net.CIDRMask(64, 128)).String()
	}
	return tcp.IP.String()
}

func (us *users) setUser(name string, rules []string) string {
	if !utf8.ValidString(name) {
		return "ERR Usernames must be valid UTF-8"
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	u := us.byName[name]
	p := newPerms()
	if u != nil {
		p = u.perms.Load().clone()
	}
	for _, rule := range rules {
		if !p.apply(rule) {
			return "ERR Error in ACL SETUSER modifier '" + rule + "': Syntax error"
		}
	}
	if u == nil {
		u = &user{name: name}
		us.byName[name] = u
	}
	u.perms.Store(p)
	return ""
}

func (us *users) delete(names []string) (int, bool) {
	us.mu.Lock()
	defer us.mu.Unlock()
	if slices.Contains(names, "default") {
		return 0, false
	}
	deleted := 0
	for _, name := range names {
		if u, ok := us.byName[name]; ok {
			delete(us.byName, name)
			u.perms.Store(&perms{deleted: true, commands: make([]bool, len(commands))})
			deleted++
		}
	}
	return deleted, true
}

func (s *Server) permission(c *client, cmd command, args [][]byte) string {
	p := c.user.perms.Load()
	switch {
	case p.all:
		return ""
	case !p.commands[cmd.id] && !openToEveryone(cmd, args):
		s.aclLog.add("command", cmd.name, c.user.name, c)
		return "NOPERM User " + c.user.name + " has no permissions to run the '" + cmd.name + "' command"
	}
	var kb [8]string
	for _, key := range cmd.keys.extract(args, kb[:0]) {
		if !p.keyAllowed(key) {
			s.aclLog.add("key", key, c.user.name, c)
			return "NOPERM No permissions to access a key"
		}
	}
	return ""
}

const aclLogMax = 128

type denial struct {
	count                                     int64
	reason, context, object, username, client string
	id                                        int64
	created, updated                          time.Time
}

type aclLog struct {
	mu      sync.Mutex
	entries []*denial
	nextID  int64
}

func (l *aclLog) add(reason, object, username string, c *client) {
	now := time.Now()
	context := "toplevel"
	if c.multi {
		context = "multi"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, d := range l.entries {
		if d.reason == reason && d.context == context && d.object == object && d.username == username && now.Sub(d.updated) < time.Minute {
			d.count++
			d.updated = now
			copy(l.entries[1:i+1], l.entries[:i])
			l.entries[0] = d
			return
		}
	}
	client := fmt.Sprintf("id=%d addr=%s laddr=%s name=%s", c.id, c.conn.RemoteAddr(), c.conn.LocalAddr(), c.name)
	d := &denial{count: 1, reason: reason, context: context, object: object, username: username, client: client, id: l.nextID, created: now, updated: now}
	l.nextID++
	l.entries = append([]*denial{d}, l.entries...)
	if len(l.entries) > aclLogMax {
		l.entries = l.entries[:aclLogMax]
	}
}

func (l *aclLog) reply(n int) arrayReply {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	out := arrayReply{}
	for _, d := range l.entries[:min(n, len(l.entries))] {
		out = append(out, arrayReply{
			bulkReply("count"), intReply(d.count),
			bulkReply("reason"), bulkReply(d.reason),
			bulkReply("context"), bulkReply(d.context),
			bulkReply("object"), bulkReply(d.object),
			bulkReply("username"), bulkReply(d.username),
			bulkReply("age-seconds"), bulkReply(strconv.FormatFloat(now.Sub(d.created).Seconds(), 'f', 3, 64)),
			bulkReply("client-info"), bulkReply(d.client),
			bulkReply("entry-id"), intReply(d.id),
			bulkReply("timestamp-created"), intReply(d.created.UnixMilli()),
			bulkReply("timestamp-last-updated"), intReply(d.updated.UnixMilli()),
		})
	}
	return out
}

func (l *aclLog) reset() {
	l.mu.Lock()
	l.entries = nil
	l.mu.Unlock()
}

func (s *Server) changeUsers(change func(*users) string) reply {
	s.aclMu.Lock()
	defer s.aclMu.Unlock()
	next := s.users.clone()
	if msg := change(next); msg != "" {
		return errorReply(msg)
	}
	b, err := next.encode()
	if err == nil {
		if rep := s.cfg.Replica; rep != nil {
			err = rep.SetSystem(b)
		} else {
			err = s.db.SetSystem(b)
		}
	}
	if err != nil {
		return storageError(err)
	}
	return okReply
}

func (s *Server) loadSystem(b []byte) {
	if len(b) == 0 {
		return
	}
	if err := s.users.load(b); err != nil {
		s.log.Error("stored users not loaded, keeping the current ones", "err", err)
	}
}

func openToEveryone(cmd command, args [][]byte) bool {
	if cmd.name != "acl" {
		return false
	}
	sub := upper(args[1])
	return sub == "WHOAMI" || sub == "CAT"
}

func cmdACL(s *Server, c *client, args [][]byte) reply {
	switch sub := upper(args[1]); {
	case sub == "WHOAMI" && len(args) == 2:
		return bulkReply(c.user.name)
	case sub == "USERS" && len(args) == 2:
		return stringsReply(s.users.names())
	case sub == "LIST" && len(args) == 2:
		var out stringsReply
		for _, name := range s.users.names() {
			if u := s.users.get(name); u != nil {
				out = append(out, strings.Join(append([]string{"user", name}, u.perms.Load().rules()...), " "))
			}
		}
		return out
	case sub == "GETUSER" && len(args) == 3:
		u := s.users.get(string(args[2]))
		if u == nil {
			return nilReply
		}
		p := u.perms.Load()
		flags := stringsReply{"off"}
		if p.enabled {
			flags[0] = "on"
		}
		if p.nopass {
			flags = append(flags, "nopass")
		}
		passwords := stringsReply{}
		for _, h := range p.passwords {
			passwords = append(passwords, hex.EncodeToString(h[:]))
		}
		return mapReply{
			bulkReply("flags"), flags,
			bulkReply("passwords"), passwords,
			bulkReply("commands"), bulkReply(strings.Join(p.commandRules(), " ")),
			bulkReply("keys"), bulkReply(strings.Join(p.keyRules(), " ")),
			bulkReply("channels"), bulkReply(""),
			bulkReply("selectors"), arrayReply{},
		}
	case sub == "SETUSER" && len(args) >= 3:
		rules := make([]string, len(args)-3)
		for i, a := range args[3:] {
			rules[i] = string(a)
		}
		name := string(args[2])
		r := s.changeUsers(func(us *users) string { return us.setUser(name, rules) })
		if r == reply(okReply) {
			shown := slices.Clone(rules)
			for i, rule := range shown {
				if rule != "" && strings.IndexByte("><#!", rule[0]) >= 0 {
					shown[i] = rule[:1] + "***"
				}
			}
			s.audit(c, slog.LevelInfo, "ACL user changed", c.user.name, "target", name, "rules", shown)
		}
		return r
	case sub == "DELUSER" && len(args) >= 3:
		names := make([]string, len(args)-2)
		for i, a := range args[2:] {
			names[i] = string(a)
		}
		n := 0
		r := s.changeUsers(func(us *users) string {
			deleted, ok := us.delete(names)
			if !ok {
				return "ERR The 'default' user cannot be removed"
			}
			n = deleted
			return ""
		})
		if r != reply(okReply) {
			return r
		}
		s.audit(c, slog.LevelInfo, "ACL users deleted", c.user.name, "targets", names, "deleted", n)
		return intReply(n)
	case sub == "LOG" && len(args) == 2:
		return s.aclLog.reply(10)
	case sub == "LOG" && len(args) == 3 && upper(args[2]) == "RESET":
		s.aclLog.reset()
		return okReply
	case sub == "LOG" && len(args) == 3:
		n, ok := parseInt(args[2])
		if !ok || n < 0 {
			return errorReply(errNotInteger)
		}
		return s.aclLog.reply(int(min(n, aclLogMax)))
	case sub == "CAT" && len(args) == 2:
		return stringsReply(categoryNames)
	case sub == "CAT" && len(args) == 3:
		cat, ok := categoryByName(string(args[2]))
		if !ok {
			return errorReply("ERR Unknown category '" + truncate(args[2], 128) + "'")
		}
		var names stringsReply
		for _, name := range slices.Sorted(maps.Keys(commands)) {
			if commands[name].categories()&cat != 0 {
				names = append(names, name)
			}
		}
		return names
	}
	return unknownSubcommand(args)
}
