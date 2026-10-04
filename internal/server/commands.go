package server

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
)

const (
	errSyntax     = "ERR syntax error"
	errNotInteger = "ERR value is not an integer or out of range"
	errOverflow   = "ERR increment or decrement would overflow"
	errWrongPass  = "WRONGPASS invalid username-password pair or user is disabled."
	errBadName    = "ERR Client names cannot contain spaces, newlines or special characters."
	errWrongType  = "WRONGTYPE Operation against a key holding the wrong kind of value"
)

func readError(err error) (reply, error) {
	if errors.Is(err, bitcask.ErrWrongKind) {
		return errorReply(errWrongType), nil
	}
	return nil, err
}

type kind int

const (
	kindConn kind = iota
	kindPure
	kindRead
	kindWrite
)

type txFunc func(tx *bitcask.Tx, args [][]byte) (reply, error)

type connFunc func(s *Server, c *client, args [][]byte) reply

type keySpec struct {
	first, last, step int
	numkeys           int
	storeFrom         int
	streams           bool
}

var (
	oneKey  = keySpec{first: 1, last: 1, step: 1}
	allArgs = keySpec{first: 1, last: -1, step: 1}
	pairs   = keySpec{first: 1, last: -1, step: 2}
)

func (k keySpec) extract(args [][]byte, dst []string) []string {
	if k.step > 0 {
		last := k.last
		if last < 0 {
			last += len(args)
		}
		for i := k.first; i <= last && i < len(args); i += k.step {
			dst = append(dst, string(args[i]))
		}
	}
	if k.numkeys > 0 && k.numkeys < len(args) {
		n, ok := parseInt(args[k.numkeys])
		for i := k.numkeys + 1; ok && i < len(args) && int64(i-k.numkeys) <= n; i++ {
			dst = append(dst, string(args[i]))
		}
	}
	for i := k.storeFrom; k.storeFrom > 0 && i+1 < len(args); i++ {
		if opt := upper(args[i]); opt == "STORE" || opt == "STOREDIST" {
			dst = append(dst, string(args[i+1]))
			i++
		}
	}
	for i := 1; k.streams && i < len(args); i++ {
		if upper(args[i]) == "STREAMS" {
			for _, key := range args[i+1 : i+1+(len(args)-i-1)/2] {
				dst = append(dst, string(key))
			}
			break
		}
	}
	return dst
}

type command struct {
	name    string
	id      int
	arity   int
	kind    kind
	keys    keySpec
	global  bool
	acl     category
	tx      txFunc
	conn    connFunc
	inMulti bool
	noAuth  bool
	blocks  bool
}

func (cmd command) validArity(n int) bool {
	return (cmd.arity <= 0 || n == cmd.arity) && (cmd.arity >= 0 || n >= -cmd.arity)
}

func lookup(name []byte) (command, bool) {
	var buf [24]byte
	if len(name) > len(buf) {
		return command{}, false
	}
	for i, c := range name {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		buf[i] = c
	}
	cmd, ok := commands[string(buf[:len(name)])]
	return cmd, ok
}

var commands map[string]command

func init() {
	commands = map[string]command{}
	for _, table := range []map[string]command{serverCommands, keyCommands, stringCommands, bitCommands, hashCommands, setCommands, listCommands, zsetCommands, blockingCommands, hyperLogLogCommands, geoCommands, streamCommands, streamGroupCommands, transactionCommands, raftCommands, aclCommands} {
		maps.Copy(commands, table)
	}
	for id, name := range slices.Sorted(maps.Keys(commands)) {
		cmd := commands[name]
		cmd.name, cmd.id = name, id
		commands[name] = cmd
	}
}

var serverCommands = map[string]command{
	"ping":         {arity: -1, kind: kindPure, acl: catFast | catConnection, tx: cmdPing},
	"echo":         {arity: 2, kind: kindPure, acl: catFast | catConnection, tx: cmdEcho},
	"quit":         {arity: -1, kind: kindConn, acl: catFast | catConnection, conn: cmdQuit, inMulti: true, noAuth: true},
	"auth":         {arity: -2, kind: kindConn, acl: catFast | catConnection, conn: cmdAuth, noAuth: true},
	"hello":        {arity: -1, kind: kindConn, acl: catFast | catConnection, conn: cmdHello, noAuth: true},
	"select":       {arity: 2, kind: kindConn, acl: catFast | catConnection, conn: cmdSelect},
	"client":       {arity: -2, kind: kindConn, acl: catConnection, conn: cmdClient},
	"command":      {arity: -1, kind: kindConn, acl: catConnection, conn: cmdCommand},
	"config":       {arity: -2, kind: kindConn, acl: catAdmin | catDangerous, conn: cmdConfig},
	"info":         {arity: -1, kind: kindConn, acl: catDangerous, conn: cmdInfo},
	"flushdb":      {arity: -1, kind: kindConn, acl: catKeyspace | catWrite | catDangerous, conn: cmdFlush},
	"flushall":     {arity: -1, kind: kindConn, acl: catKeyspace | catWrite | catDangerous, conn: cmdFlush},
	"save":         {arity: 1, kind: kindConn, acl: catAdmin | catDangerous, conn: cmdSave},
	"bgrewriteaof": {arity: 1, kind: kindConn, acl: catAdmin | catDangerous, conn: cmdBgRewriteAOF},
}

func upper(b []byte) string {
	return strings.ToUpper(string(b))
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		b = b[:n]
	}
	return string(b)
}

func parseInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	digits := b
	if digits[0] == '-' {
		digits = digits[1:]
	}
	if len(digits) == 0 || (digits[0] == '0' && len(b) > 1) {
		return 0, false
	}
	for _, ch := range digits {
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

func unknownCommand(args [][]byte) string {
	var b strings.Builder
	b.WriteString("ERR unknown command '")
	b.WriteString(truncate(args[0], 128))
	b.WriteString("', with args beginning with: ")
	for _, a := range args[1:] {
		if b.Len() > 512 {
			break
		}
		b.WriteString("'")
		b.WriteString(truncate(a, 128))
		b.WriteString("' ")
	}
	return b.String()
}

func unknownSubcommand(args [][]byte) errorReply {
	return errorReply(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.",
		truncate(args[1], 128), upper(args[0])))
}

func validClientName(b []byte) bool {
	for _, ch := range b {
		if ch < '!' || ch > '~' {
			return false
		}
	}
	return true
}

func cmdPing(_ *bitcask.Tx, args [][]byte) (reply, error) {
	switch len(args) {
	case 1:
		return statusReply("PONG"), nil
	case 2:
		return bulkReply(args[1]), nil
	}
	return errorReply("ERR wrong number of arguments for 'ping' command"), nil
}

func cmdEcho(_ *bitcask.Tx, args [][]byte) (reply, error) {
	return bulkReply(args[1]), nil
}

func cmdQuit(s *Server, c *client, args [][]byte) reply {
	c.quit = true
	return okReply
}

func cmdAuth(s *Server, c *client, args [][]byte) reply {
	if len(args) > 3 {
		return errorReply(errSyntax)
	}
	if len(args) == 2 && s.users.get("default").perms.Load().nopass {
		return errorReply("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	user := "default"
	if len(args) == 3 {
		user = string(args[1])
	}
	u, rep := s.login(c, user, args[len(args)-1])
	if u == nil {
		return rep
	}
	c.user = u
	return okReply
}

func cmdHello(s *Server, c *client, args [][]byte) reply {
	if len(args) >= 2 {
		v, ok := parseInt(args[1])
		if !ok {
			return errorReply("ERR Protocol version is not an integer or out of range")
		}
		if v != 2 {
			return errorReply("NOPROTO unsupported protocol version")
		}
	}
	name, u := c.name, c.user
	for i := 2; i < len(args); i++ {
		switch upper(args[i]) {
		case "AUTH":
			if i+2 >= len(args) {
				return errorReply(errSyntax)
			}
			var rep reply
			if u, rep = s.login(c, string(args[i+1]), args[i+2]); u == nil {
				return rep
			}
			i += 2
		case "SETNAME":
			if i+1 >= len(args) {
				return errorReply(errSyntax)
			}
			if !validClientName(args[i+1]) {
				return errorReply(errBadName)
			}
			name = string(args[i+1])
			i++
		default:
			return errorReply(errSyntax)
		}
	}
	if u == nil {
		return errorReply("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
	}
	c.name, c.user = name, u
	return arrayReply{
		bulkReply("server"), bulkReply("redis"),
		bulkReply("version"), bulkReply(redisVersion),
		bulkReply("proto"), intReply(2),
		bulkReply("id"), intReply(c.id),
		bulkReply("mode"), bulkReply("standalone"),
		bulkReply("role"), bulkReply("master"),
		bulkReply("modules"), arrayReply{},
	}
}

func cmdSelect(s *Server, c *client, args [][]byte) reply {
	n, ok := parseInt(args[1])
	if !ok {
		return errorReply(errNotInteger)
	}
	if n != 0 {
		return errorReply("ERR DB index is out of range")
	}
	return okReply
}

func cmdClient(s *Server, c *client, args [][]byte) reply {
	switch sub := upper(args[1]); {
	case sub == "ID" && len(args) == 2:
		return intReply(c.id)
	case sub == "GETNAME" && len(args) == 2:
		if c.name == "" {
			return nilReply
		}
		return bulkReply(c.name)
	case sub == "SETNAME" && len(args) == 3:
		if !validClientName(args[2]) {
			return errorReply(errBadName)
		}
		c.name = string(args[2])
		return okReply
	case sub == "SETINFO" && len(args) == 4:
		return okReply
	}
	return unknownSubcommand(args)
}

func cmdCommand(s *Server, c *client, args [][]byte) reply {
	if len(args) == 1 {
		return arrayReply{}
	}
	switch upper(args[1]) {
	case "COUNT":
		return intReply(len(commands))
	case "DOCS", "INFO", "LIST":
		return arrayReply{}
	}
	return unknownSubcommand(args)
}

func cmdConfig(s *Server, c *client, args [][]byte) reply {
	switch upper(args[1]) {
	case "GET":
		if len(args) < 3 {
			return errorReply("ERR wrong number of arguments for 'config|get' command")
		}
		params := map[string]string{
			"appendonly":         "yes",
			"appendfsync":        s.db.Options().Sync.String(),
			"save":               "",
			"databases":          "1",
			"proto-max-bulk-len": strconv.FormatInt(s.maxBulk.Load(), 10),
			"requirepass":        s.password(),
		}
		var out stringsReply
		for _, name := range slices.Sorted(maps.Keys(params)) {
			for _, p := range args[2:] {
				if matchGlob(strings.ToLower(string(p)), name) {
					out = append(out, name, params[name])
					break
				}
			}
		}
		return out
	case "SET":
		return configSet(s, c, args[2:])
	case "REWRITE":
		return errorReply("ERR CONFIG REWRITE is not supported: CONFIG SET lasts until restart, keep settings in flags or CASKETDB_ variables")
	case "RESETSTAT":
		return okReply
	}
	return unknownSubcommand(args)
}

func configSet(s *Server, c *client, args [][]byte) reply {
	if len(args) == 0 || len(args)%2 != 0 {
		return errorReply("ERR wrong number of arguments for 'config|set' command")
	}
	var names []string
	var apply []func()
	var password *string
	for i := 0; i < len(args); i += 2 {
		name, value := strings.ToLower(string(args[i])), string(args[i+1])
		if slices.Contains(names, name) {
			return errorReply("ERR CONFIG SET failed (possibly related to argument '" + name + "') - duplicate parameter")
		}
		names = append(names, name)
		switch name {
		case "requirepass":
			password = &value
			apply = append(apply, func() { s.pass.Store(&value) })
		case "appendfsync":
			p, err := bitcask.ParseSyncPolicy(value)
			if err != nil {
				return errorReply("ERR CONFIG SET failed (possibly related to argument 'appendfsync') - argument must be one of the following: always, everysec, no")
			}
			apply = append(apply, func() { s.db.SetSync(p) })
		case "proto-max-bulk-len":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return errorReply("ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a positive number of bytes")
			}
			apply = append(apply, func() { s.maxBulk.Store(int64(n)) })
		default:
			return errorReply("ERR Unknown option or number of arguments for CONFIG SET - '" + truncate(args[i], 128) + "'")
		}
	}
	if password != nil {
		r := s.changeUsers(func(us *users) string {
			us.setPassword(*password)
			return ""
		})
		if r != reply(okReply) {
			return r
		}
	}
	for _, f := range apply {
		f()
	}
	s.audit(c, slog.LevelInfo, "config changed", c.user.name, "params", names)
	return okReply
}

func cmdInfo(s *Server, c *client, args [][]byte) reply {
	st := s.db.Stats()
	var b strings.Builder
	line := func(format string, a ...any) {
		fmt.Fprintf(&b, format, a...)
		b.WriteString("\r\n")
	}
	line("# Server")
	line("redis_version:%s", redisVersion)
	line("casketdb_version:%s", Version)
	line("redis_mode:standalone")
	line("os:%s %s", runtime.GOOS, runtime.GOARCH)
	line("process_id:%d", os.Getpid())
	line("uptime_in_seconds:%d", int64(time.Since(s.started).Seconds()))
	line("")
	line("# Clients")
	line("connected_clients:%d", s.clientCount())
	line("maxclients:%d", s.cfg.MaxClients)
	line("blocked_clients:%d", s.blocked.count.Load())
	line("")
	line("# Persistence")
	line("aof_enabled:1")
	line("appendfsync:%s", s.db.Options().Sync)
	line("bitcask_logs:%d", st.Logs)
	line("bitcask_data_files:%d", st.DataFiles)
	line("bitcask_total_bytes:%d", st.TotalBytes)
	line("bitcask_live_bytes:%d", st.LiveBytes)
	line("bitcask_merges:%d", st.Merges)
	line("bitcask_writes:%d", st.Writes)
	line("bitcask_fsyncs:%d", st.Fsyncs)
	line("")
	line("# Stats")
	line("total_connections_received:%d", s.connections.Load())
	line("rejected_connections:%d", s.rejected.Load())
	line("total_commands_processed:%d", s.processed.Load())
	line("expired_keys:%d", st.ExpiredKeys)
	line("")
	line("# Replication")
	if rep := s.cfg.Replica; rep != nil {
		rs := rep.Status()
		role := "slave"
		if rs.State == "Leader" {
			role = "master"
		}
		line("role:%s", role)
		line("raft_state:%s", rs.State)
		line("raft_term:%d", rs.Term)
		line("raft_applied_index:%d", rs.Applied)
		line("raft_leader_id:%s", rs.LeaderID)
		line("raft_leader_addr:%s", rs.LeaderAddr)
		line("raft_membership:%s", rs.Membership)
		line("raft_voters:%d", rs.Voters)
		line("raft_learners:%d", rs.Learners)
	} else {
		line("role:master")
	}
	line("")
	line("# Keyspace")
	if st.Keys > 0 {
		line("db0:keys=%d,expires=%d,avg_ttl=0", st.Keys, st.KeysWithTTL)
	}
	return bulkReply(b.String())
}

func cmdFlush(s *Server, c *client, args [][]byte) reply {
	if len(args) > 2 {
		return errorReply(errSyntax)
	}
	if len(args) == 2 {
		if mode := upper(args[1]); mode != "ASYNC" && mode != "SYNC" {
			return errorReply(errSyntax)
		}
	}
	if err := s.flush(); err != nil {
		return storageError(err)
	}
	s.audit(c, slog.LevelInfo, "database flushed", c.user.name)
	return okReply
}

func cmdSave(s *Server, c *client, args [][]byte) reply {
	if err := s.db.Sync(); err != nil {
		return storageError(err)
	}
	return okReply
}

func cmdBgRewriteAOF(s *Server, c *client, args [][]byte) reply {
	go func() {
		err := s.db.Merge()
		if err != nil && !errors.Is(err, bitcask.ErrMergeInProgress) && !errors.Is(err, bitcask.ErrClosed) {
			s.log.Error("merge failed", "err", err)
		}
	}()
	return statusReply("Background append only file rewriting started")
}
