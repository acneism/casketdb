package server

import (
	"bytes"
	"context"
	"log/slog"
	"strings"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/resp"
	"github.com/acneism/lua"
	libm "github.com/acneism/lua-libm"
	"github.com/acneism/lua/redis"
)

type scriptFunc func(s *Server, c *client, tx *bitcask.Tx, args [][]byte) (reply, error)

var exactMath = lua.Math{Sin: libm.Sin, Cos: libm.Cos, Tan: libm.Tan, Asin: libm.Asin, Acos: libm.Acos, Atan: libm.Atan, Atan2: libm.Atan2}

var scriptKeys = keySpec{numkeys: 2}

var scriptingCommands = map[string]command{
	"eval":       {arity: -3, kind: kindWrite, keys: scriptKeys, acl: catScripting, script: runScript},
	"evalsha":    {arity: -3, kind: kindWrite, keys: scriptKeys, acl: catScripting, script: runScript},
	"eval_ro":    {arity: -3, kind: kindRead, keys: scriptKeys, acl: catScripting, script: runScript},
	"evalsha_ro": {arity: -3, kind: kindRead, keys: scriptKeys, acl: catScripting, script: runScript},
	"fcall":      {arity: -3, kind: kindWrite, keys: scriptKeys, acl: catScripting, script: runScript},
	"fcall_ro":   {arity: -3, kind: kindRead, keys: scriptKeys, acl: catScripting, script: runScript},
	"script":     {arity: -2, kind: kindConn, acl: catScripting, conn: cmdScript},
	"function":   {arity: -2, kind: kindConn, acl: catScripting, conn: cmdFunction},
}

var scriptConnCommands = map[string]bool{"select": true, "command": true, "pubsub": true, "publish": true, "spublish": true}

func (s *Server) bind(c *client, cmd command) command {
	if cmd.script != nil {
		cmd.tx = func(tx *bitcask.Tx, args [][]byte) (reply, error) { return cmd.script(s, c, tx, args) }
	}
	return cmd
}

func runScript(s *Server, c *client, tx *bitcask.Tx, args [][]byte) (reply, error) {
	h := &scriptHost{s: s, c: c, tx: tx}
	r := s.lua.Command(args, redis.Run{Host: h, Proto: c.w.Proto})
	if r.Type == redis.AbortReply {
		return nil, r.Err
	}
	return scriptReply{Reply: r, s: s, c: c, published: h.published}, nil
}

func cmdScript(s *Server, c *client, args [][]byte) reply {
	return scriptReply{Reply: s.lua.Command(args, redis.Run{})}
}

var functionChanges = map[string]bool{"load": true, "delete": true, "flush": true, "restore": true}

func cmdFunction(s *Server, c *client, args [][]byte) reply {
	if !functionChanges[strings.ToLower(string(args[1]))] {
		return scriptReply{Reply: s.lua.Command(args, redis.Run{})}
	}
	s.aclMu.Lock()
	defer s.aclMu.Unlock()
	r := s.lua.Command(args, redis.Run{})
	if r.Type == redis.ErrorReply {
		return scriptReply{Reply: r}
	}
	if err := s.saveSystem(func(st *systemState) { st.Functions = s.lua.FunctionDump() }); err != nil {
		s.loadSystem(s.db.System())
		return storageError(err)
	}
	return scriptReply{Reply: r}
}

func (s *Server) loadFunctions(payload []byte) {
	if payload == nil {
		s.lua.FunctionFlush()
		return
	}
	if r := s.lua.FunctionRestore(payload, "FLUSH"); r.Type == redis.ErrorReply {
		s.log.Error("stored functions not loaded", "err", r.Str)
	}
}

type scriptReply struct {
	redis.Reply
	s         *Server
	c         *client
	published [][][]byte
}

func (r scriptReply) writeTo(w *resp.Writer) {
	for _, args := range r.published {
		publish(strings.EqualFold(string(args[0]), "spublish"))(r.s, r.c, args)
	}
	w.Raw(r.AppendRESP(nil, w.Proto))
}

type scriptHost struct {
	s         *Server
	c         *client
	tx        *bitcask.Tx
	out       bytes.Buffer
	w         *resp.Writer
	published [][][]byte
}

func (h *scriptHost) Lookup(argv [][]byte) (redis.Command, bool) {
	cmd, ok := lookup(argv[0])
	if !ok {
		return redis.Command{}, false
	}
	return redis.Command{
		Name:         cmd.name,
		Arity:        cmd.arity,
		Write:        cmd.kind == kindWrite && cmd.script == nil,
		MayReplicate: cmd.name == "publish" || cmd.name == "spublish" || cmd.name == "pfcount",
		NoScript:     cmd.script != nil || cmd.kind == kindConn && !scriptConnCommands[cmd.name],
	}, true
}

func (h *scriptHost) Check(argv [][]byte) *redis.Reply {
	cmd, _ := lookup(argv[0])
	if msg := h.s.permission(h.c, cmd, argv, "lua"); msg != "" {
		r := redis.Error("ERR ACL failure in script: " + strings.TrimPrefix(msg, "NOPERM "))
		return &r
	}
	return nil
}

func (h *scriptHost) Call(argv [][]byte, proto int) redis.Reply {
	cmd, _ := lookup(argv[0])
	var r reply
	switch {
	case cmd.name == "publish" || cmd.name == "spublish":
		h.published = append(h.published, argv)
		r = intReply(h.s.subs.receivers(argv[1], cmd.name == "spublish"))
	case cmd.kind == kindConn:
		r = cmd.conn(h.s, h.c, argv)
	default:
		var err error
		if r, err = cmd.tx(h.tx, argv); err == nil {
			err = h.tx.Err()
		}
		if err != nil {
			return redis.Abort(err)
		}
	}
	if h.w == nil {
		h.w = resp.NewWriter(&h.out)
	}
	h.out.Reset()
	h.w.Proto = proto
	r.writeTo(h.w)
	if err := h.w.Flush(); err != nil {
		return redis.Abort(err)
	}
	result, _, err := redis.ParseRESP(h.out.Bytes())
	if err != nil {
		return redis.Abort(err)
	}
	return result
}

func (h *scriptHost) Log(level int, msg string) {
	levels := []slog.Level{slog.LevelDebug, slog.LevelDebug, slog.LevelInfo, slog.LevelWarn}
	h.s.log.Log(context.Background(), levels[min(max(level, 0), len(levels)-1)], msg, "component", "script", "client", h.c.id)
}
