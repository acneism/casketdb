package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/clock"
	"github.com/acneism/casketdb/internal/replica"
)

func TestRaftCommandNeedsCluster(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	if got, ok := c.do("RAFT", "TRANSFER").(errReply); !ok || !strings.HasPrefix(string(got), "ERR RAFT needs a cluster") {
		t.Fatalf("RAFT TRANSFER on a single node = %v", got)
	}
	if got, ok := c.do("RAFT").(errReply); !ok || !strings.HasPrefix(string(got), "ERR wrong number of arguments") {
		t.Fatalf("RAFT without a subcommand = %v", got)
	}
	c.do("MULTI")
	if got, ok := c.do("RAFT", "TRANSFER").(errReply); !ok || !strings.Contains(string(got), "not allowed inside a transaction") {
		t.Fatalf("RAFT inside MULTI = %v", got)
	}
}

func TestReplicaErrorsUseRedisCodes(t *testing.T) {
	for err, prefix := range map[error]string{
		replica.ErrNotLeader:   "READONLY ",
		replica.ErrUnconfirmed: "TRYAGAIN ",
		replica.ErrRemoved:     "ERR this node was removed",
		errors.New("disk"):     "ERR ",
	} {
		if got := string(storageError(fmt.Errorf("op: %w", err))); !strings.HasPrefix(got, prefix) {
			t.Fatalf("%v maps to %q, want prefix %q", err, got, prefix)
		}
	}
}

var testClock = clock.NewManual(time.Now())

type status string

type errReply string

type respMap []any

type respSet []any

type respPush []any

type respDouble string

type respVerbatim string

type testConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func startServer(t testing.TB, dir string) (*Server, *bitcask.DB, string) {
	t.Helper()
	return startServerWith(t, dir, Config{})
}

func startServerWith(t testing.TB, dir string, cfg Config) (*Server, *bitcask.DB, string) {
	t.Helper()
	o := bitcask.DefaultOptions()
	o.Sync = bitcask.SyncNo
	o.MergeInterval = 0
	o.Now = testClock.Now
	db, err := bitcask.Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	srv := New(db, cfg)
	go func() { _ = srv.Serve(ln) }()
	return srv, db, ln.Addr().String()
}

func stopServer(t testing.TB, srv *Server, db *bitcask.DB) {
	t.Helper()
	srv.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func dial(t *testing.T, addr string) *testConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { conn.Close() })
	return &testConn{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func encode(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func (c *testConn) do(args ...string) any {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, encode(args...)); err != nil {
		c.t.Fatal(err)
	}
	return c.read()
}

func (c *testConn) read() any {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		c.t.Fatal("empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return status(body)
	case '-':
		return errReply(body)
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			c.t.Fatalf("bad integer reply %q", line)
		}
		return n
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			c.t.Fatalf("bad bulk reply %q", line)
		}
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatal(err)
		}
		return string(buf[:n])
	case '*', '%', '~', '>':
		n, err := strconv.Atoi(body)
		if err != nil {
			c.t.Fatalf("bad aggregate reply %q", line)
		}
		if n < 0 {
			return nil
		}
		if line[0] == '%' {
			n *= 2
		}
		out := make([]any, n)
		for i := range out {
			out[i] = c.read()
		}
		switch line[0] {
		case '%':
			return respMap(out)
		case '~':
			return respSet(out)
		case '>':
			return respPush(out)
		}
		return out
	case '_':
		return nil
	case ',':
		return respDouble(body)
	case '=':
		n, err := strconv.Atoi(body)
		if err != nil || n < 4 {
			c.t.Fatalf("bad verbatim reply %q", line)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatal(err)
		}
		return respVerbatim(buf[4:n])
	}
	c.t.Fatalf("unexpected reply %q", line)
	return nil
}

func (c *testConn) expect(want any, args ...string) {
	c.t.Helper()
	got := c.do(args...)
	if prefix, ok := want.(errReply); ok {
		if g, ok := got.(errReply); ok && strings.HasPrefix(string(g), string(prefix)) {
			return
		}
		c.t.Fatalf("%q: got %#v, want error starting with %q", args, got, prefix)
	}
	if !reflect.DeepEqual(got, want) {
		c.t.Fatalf("%q: got %#v, want %#v", args, got, want)
	}
}

func TestCommands(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	ok := status("OK")
	steps := []struct {
		want any
		args []string
	}{
		{status("PONG"), []string{"PING"}},
		{"hi", []string{"PING", "hi"}},
		{"x", []string{"ECHO", "x"}},
		{ok, []string{"SET", "k", "v"}},
		{"v", []string{"GET", "k"}},
		{nil, []string{"GET", "missing"}},
		{nil, []string{"SET", "k", "v2", "NX"}},
		{"v", []string{"SET", "k", "v2", "XX", "GET"}},
		{"v2", []string{"get", "k"}},
		{nil, []string{"SET", "nope", "v", "XX"}},
		{ok, []string{"SET", "n", "10"}},
		{int64(11), []string{"INCR", "n"}},
		{int64(16), []string{"INCRBY", "n", "5"}},
		{int64(15), []string{"DECR", "n"}},
		{int64(-5), []string{"DECRBY", "n", "20"}},
		{int64(1), []string{"INCR", "fresh"}},
		{errReply("ERR value is not an integer"), []string{"INCR", "k"}},
		{errReply("ERR value is not an integer"), []string{"INCRBY", "n", "+1"}},
		{ok, []string{"SET", "big", "9223372036854775807"}},
		{errReply("ERR increment or decrement would overflow"), []string{"INCR", "big"}},
		{int64(4), []string{"APPEND", "k", "!!"}},
		{int64(4), []string{"STRLEN", "k"}},
		{int64(0), []string{"STRLEN", "missing"}},
		{ok, []string{"MSET", "a", "1", "b", "2"}},
		{[]any{"1", "2", nil}, []string{"MGET", "a", "b", "missing"}},
		{errReply("ERR wrong number of arguments for 'mset'"), []string{"MSET", "a", "1", "b"}},
		{int64(3), []string{"EXISTS", "a", "b", "missing", "a"}},
		{int64(1), []string{"DEL", "a", "missing"}},
		{status("string"), []string{"TYPE", "b"}},
		{status("none"), []string{"TYPE", "a"}},
		{int64(0), []string{"SETNX", "b", "x"}},
		{int64(1), []string{"SETNX", "c", "x"}},
		{"x", []string{"GETDEL", "c"}},
		{nil, []string{"GET", "c"}},
		{int64(-1), []string{"TTL", "b"}},
		{int64(-2), []string{"TTL", "missing"}},
		{int64(1), []string{"EXPIRE", "b", "100"}},
		{int64(100), []string{"TTL", "b"}},
		{int64(0), []string{"EXPIRE", "b", "200", "NX"}},
		{int64(0), []string{"EXPIRE", "b", "50", "GT"}},
		{int64(1), []string{"EXPIRE", "b", "50", "LT"}},
		{int64(50), []string{"TTL", "b"}},
		{"2", []string{"GET", "b"}},
		{int64(1), []string{"PERSIST", "b"}},
		{int64(0), []string{"PERSIST", "b"}},
		{int64(-1), []string{"TTL", "b"}},
		{errReply("ERR NX and XX, GT or LT"), []string{"EXPIRE", "b", "10", "NX", "XX"}},
		{int64(0), []string{"EXPIRE", "missing", "10"}},
		{int64(1), []string{"PEXPIREAT", "b", "1"}},
		{int64(0), []string{"EXISTS", "b"}},
		{ok, []string{"SET", "e", "v", "EX", "100"}},
		{int64(100), []string{"TTL", "e"}},
		{ok, []string{"SET", "e", "v2", "KEEPTTL"}},
		{int64(100), []string{"TTL", "e"}},
		{ok, []string{"SET", "e", "v3"}},
		{int64(-1), []string{"TTL", "e"}},
		{ok, []string{"SETEX", "s", "100", "v"}},
		{int64(100), []string{"TTL", "s"}},
		{ok, []string{"PSETEX", "p", "100000", "v"}},
		{int64(100), []string{"TTL", "p"}},
		{errReply("ERR invalid expire time in 'set'"), []string{"SET", "k", "v", "EX", "0"}},
		{errReply("ERR invalid expire time in 'setex'"), []string{"SETEX", "k", "-1", "v"}},
		{errReply("ERR syntax error"), []string{"SET", "k", "v", "NX", "XX"}},
		{errReply("ERR syntax error"), []string{"SET", "k", "v", "EX", "10", "KEEPTTL"}},
		{errReply("ERR wrong number of arguments for 'get'"), []string{"GET"}},
		{errReply("ERR unknown command 'FOO', with args beginning with: 'bar'"), []string{"FOO", "bar"}},
		{ok, []string{"SELECT", "0"}},
		{errReply("ERR DB index is out of range"), []string{"SELECT", "1"}},
		{errReply("NOPROTO"), []string{"HELLO", "4"}},
		{ok, []string{"CLIENT", "SETNAME", "tester"}},
		{"tester", []string{"CLIENT", "GETNAME"}},
		{errReply("ERR Client names cannot contain spaces"), []string{"CLIENT", "SETNAME", "a b"}},
		{[]any{}, []string{"COMMAND", "DOCS"}},
		{[]any{"appendonly", "yes"}, []string{"CONFIG", "GET", "appendonly"}},
		{ok, []string{"SAVE"}},
	}
	for _, s := range steps {
		c.expect(s.want, s.args...)
	}
	hello, isArray := c.do("HELLO", "2").([]any)
	if !isArray || len(hello) != 14 || hello[0] != "server" {
		t.Fatalf("HELLO 2 = %#v", hello)
	}
	info, _ := c.do("INFO").(string)
	if !strings.Contains(info, "redis_version:") || !strings.Contains(info, "db0:keys=") {
		t.Fatalf("INFO missing fields:\n%s", info)
	}
	c.expect(ok, "QUIT")
	if _, err := c.r.ReadByte(); err == nil {
		t.Fatal("connection still open after QUIT")
	}
}

func TestExpiryThroughServer(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(status("OK"), "SET", "t", "v", "PX", "50")
	c.expect(int64(50), "PTTL", "t")
	testClock.Advance(49 * time.Millisecond)
	c.expect("v", "GET", "t")
	c.expect(int64(1), "PTTL", "t")
	testClock.Advance(time.Millisecond)
	c.expect(nil, "GET", "t")
	c.expect(int64(-2), "TTL", "t")

	c.expect(status("OK"), "SET", "sweep", "v", "PX", "10")
	testClock.Advance(20 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for db.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("active expiry left %d keys", db.Len())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if info, _ := c.do("INFO").(string); !strings.Contains(info, "expired_keys:2\r\n") {
		t.Fatalf("INFO has no expired_keys:2:\n%s", info)
	}
}

func TestTypes(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	if err := db.Update(bitcask.Keys("h"), func(tx *bitcask.Tx) error {
		tx.PutKind("h", typeHash, []byte("fields"), 0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c := dial(t, addr)
	c.expect(status("OK"), "SET", "s", "v")
	c.expect(status("hash"), "TYPE", "h")
	c.expect(status("string"), "TYPE", "s")
	c.expect(status("none"), "TYPE", "missing")
	for _, cmd := range [][]string{{"GET", "h"}, {"INCR", "h"}, {"APPEND", "h", "x"}, {"STRLEN", "h"}, {"GETDEL", "h"}, {"SET", "h", "x", "GET"},
		{"GETSET", "h", "x"}, {"GETEX", "h"}, {"GETRANGE", "h", "0", "1"}, {"SETRANGE", "h", "0", "x"}, {"INCRBYFLOAT", "h", "1"}} {
		c.expect(errReply(errWrongType), cmd...)
	}
	c.expect(errReply("ERR The specified keys must contain string values"), "LCS", "h", "s")
	c.expect([]any{"v", nil}, "MGET", "s", "h")
	c.expect(int64(1), "EXPIRE", "h", "100")
	c.expect(status("hash"), "TYPE", "h")
	c.expect(int64(1), "PERSIST", "h")
	c.expect(int64(-1), "TTL", "h")
	c.expect([]any{"0", []any{"h"}}, "SCAN", "0", "TYPE", "HASH")
	c.expect([]any{"0", []any{"s"}}, "SCAN", "0", "TYPE", "string")
	c.expect([]any{"0", []any{}}, "SCAN", "0", "TYPE", "nosuch")
	c.expect(status("OK"), "SET", "n", "123")
	c.expect(status("OK"), "SET", "long", strings.Repeat("x", 45))
	for key, want := range map[string]any{"s": "embstr", "n": "int", "long": "raw", "h": "listpack", "missing": nil} {
		c.expect(want, "OBJECT", "ENCODING", key)
	}
	c.expect(errReply("ERR unknown subcommand"), "OBJECT", "FREQ", "s")
	c.expect(status("OK"), "MULTI")
	c.expect(status("QUEUED"), "GET", "h")
	c.expect(status("QUEUED"), "SET", "s2", "x")
	c.expect([]any{errReply(errWrongType), status("OK")}, "EXEC")
	c.expect("x", "GET", "s2")
	c.expect(status("OK"), "SET", "h", "plain")
	c.expect(status("string"), "TYPE", "h")
}

func TestStringCommands(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)

	c.expect(nil, "GETSET", "k", "a")
	c.expect("a", "GETSET", "k", "b")
	c.expect("b", "GETEX", "k", "EX", "100")
	c.expect(int64(100), "TTL", "k")
	c.expect("b", "GETEX", "k", "PERSIST")
	c.expect(int64(-1), "TTL", "k")
	c.expect(nil, "GETEX", "missing", "EX", "10")
	c.expect(errReply("ERR invalid expire time in 'getex' command"), "GETEX", "k", "EX", "0")
	c.expect(errReply(errSyntax), "GETEX", "k", "EX", "10", "PERSIST")
	c.expect("b", "GETEX", "k", "PXAT", "1")
	c.expect(int64(0), "EXISTS", "k")

	c.expect(status("OK"), "SET", "s", "This is a string")
	for _, tc := range [][3]string{{"0", "3", "This"}, {"-3", "-1", "ing"}, {"0", "-1", "This is a string"}, {"10", "100", "string"}, {"-1", "-5", ""}, {"5", "3", ""}, {"-100", "2", "Thi"}} {
		c.expect(tc[2], "GETRANGE", "s", tc[0], tc[1])
	}
	c.expect("", "GETRANGE", "missing", "0", "-1")

	c.expect(status("OK"), "SET", "key1", "Hello World")
	c.expect(int64(11), "SETRANGE", "key1", "6", "Redis")
	c.expect("Hello Redis", "GET", "key1")
	c.expect(int64(11), "SETRANGE", "key2", "6", "Redis")
	c.expect("\x00\x00\x00\x00\x00\x00Redis", "GET", "key2")
	c.expect(int64(0), "SETRANGE", "key3", "5", "")
	c.expect(int64(0), "EXISTS", "key3")
	c.expect(errReply("ERR offset is out of range"), "SETRANGE", "key1", "-1", "x")
	c.expect(errReply(errTooBig), "SETRANGE", "key1", "536870911", "xx")

	c.expect(status("OK"), "SET", "f", "10.50")
	c.expect("10.6", "INCRBYFLOAT", "f", "0.1")
	c.expect("5.6", "INCRBYFLOAT", "f", "-5")
	c.expect(status("OK"), "SET", "f", "5.0e3")
	c.expect("5200", "INCRBYFLOAT", "f", "2.0e2")
	c.expect(errReply(errNotFloat), "INCRBYFLOAT", "f", "abc")
	c.expect(errReply(errFloatEdge), "INCRBYFLOAT", "f", "inf")
	c.expect("1.5", "INCRBYFLOAT", "nf", "1.5")

	c.expect(int64(1), "MSETNX", "m1", "Hello", "m2", "there")
	c.expect(int64(0), "MSETNX", "m2", "new", "m3", "world")
	c.expect([]any{"Hello", "there", nil}, "MGET", "m1", "m2", "m3")

	c.expect(status("OK"), "MSET", "key1", "ohmytext", "key2", "mynewtext")
	c.expect("mytext", "LCS", "key1", "key2")
	c.expect(int64(6), "LCS", "key1", "key2", "LEN")
	span := func(a, b, c, d int64) []any { return []any{[]any{a, b}, []any{c, d}} }
	c.expect([]any{"matches", []any{span(4, 7, 5, 8), span(2, 3, 0, 1)}, "len", int64(6)}, "LCS", "key1", "key2", "IDX")
	c.expect([]any{"matches", []any{append(span(4, 7, 5, 8), int64(4))}, "len", int64(6)}, "LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN")
	c.expect(errReply("ERR If you want both"), "LCS", "key1", "key2", "IDX", "LEN")
	c.expect("", "LCS", "missing1", "missing2")
}

func TestBitCommands(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)

	c.expect(int64(0), "SETBIT", "b", "7", "1")
	c.expect(int64(1), "SETBIT", "b", "7", "0")
	c.expect("\x00", "GET", "b")
	c.expect(int64(0), "SETBIT", "b", "7", "1")
	c.expect(int64(1), "GETBIT", "b", "7")
	c.expect(int64(0), "GETBIT", "b", "100")
	c.expect(errReply(errBitOffset), "SETBIT", "b", "-1", "1")
	c.expect(errReply(errBitOffset), "SETBIT", "b", "4294967296", "1")
	c.expect(errReply("ERR bit is not an integer or out of range"), "SETBIT", "b", "1", "2")

	c.expect(status("OK"), "SET", "s", "foobar")
	for want, cmd := range map[int64][]string{26: {}, 4: {"0", "0"}, 6: {"1", "1", "BYTE"}, 17: {"5", "30", "BIT"}, 0: {"-1", "-5"}} {
		c.expect(want, append([]string{"BITCOUNT", "s"}, cmd...)...)
	}
	c.expect(int64(0), "BITCOUNT", "missing")
	c.expect(errReply(errSyntax), "BITCOUNT", "s", "0")

	c.expect(status("OK"), "SET", "p", "\xff\xf0\x00")
	c.expect(int64(12), "BITPOS", "p", "0")
	c.expect(status("OK"), "SET", "p", "\x00\xff\xf0")
	for want, cmd := range map[int64][]string{8: {"1", "0"}, 16: {"1", "2", "-1", "BYTE"}, 20: {"0", "2", "-1"}, 9: {"1", "9", "15", "BIT"}} {
		c.expect(want, append([]string{"BITPOS", "p"}, cmd...)...)
	}
	c.expect(int64(8), "BITPOS", "p", "1", "7", "15", "BIT")
	c.expect(status("OK"), "SET", "z", "\x00\x00\x00")
	c.expect(int64(-1), "BITPOS", "z", "1")
	c.expect(int64(-1), "BITPOS", "z", "1", "7", "-3", "BIT")
	c.expect(status("OK"), "SET", "ones", "\xff\xff")
	c.expect(int64(16), "BITPOS", "ones", "0")
	c.expect(int64(-1), "BITPOS", "ones", "0", "0", "-1")
	c.expect(int64(-1), "BITPOS", "missing", "1")
	c.expect(int64(0), "BITPOS", "missing", "0")
	c.expect(errReply("ERR The bit argument must be 1 or 0."), "BITPOS", "p", "2")

	c.expect(status("OK"), "MSET", "key1", "foobar", "key2", "abcdef")
	c.expect(int64(6), "BITOP", "AND", "dest", "key1", "key2")
	c.expect("`bc`ab", "GET", "dest")
	c.expect(int64(6), "BITOP", "OR", "dest", "key1", "key2")
	c.expect("goofev", "GET", "dest")
	c.expect(int64(6), "BITOP", "XOR", "dest", "key1", "missing")
	c.expect("foobar", "GET", "dest")
	c.expect(int64(1), "BITOP", "NOT", "dest", "b")
	c.expect("\xfe", "GET", "dest")
	c.expect(int64(0), "BITOP", "NOT", "dest", "missing")
	c.expect(int64(0), "EXISTS", "dest")
	c.expect(errReply("ERR BITOP NOT must be called with a single source key."), "BITOP", "NOT", "dest", "key1", "key2")
	c.expect(errReply(errSyntax), "BITOP", "NAND", "dest", "key1")

	c.expect([]any{int64(1), int64(0)}, "BITFIELD", "f", "INCRBY", "i5", "100", "1", "GET", "u4", "0")
	sat := []string{"BITFIELD", "o", "INCRBY", "u2", "100", "1", "OVERFLOW", "SAT", "INCRBY", "u2", "102", "1"}
	for _, want := range [][2]int64{{1, 1}, {2, 2}, {3, 3}, {0, 3}} {
		c.expect([]any{want[0], want[1]}, sat...)
	}
	c.expect([]any{nil}, "BITFIELD", "o", "OVERFLOW", "FAIL", "INCRBY", "u2", "102", "1")
	c.expect([]any{int64(0), int64(0)}, "BITFIELD", "h", "SET", "i8", "#0", "100", "SET", "i8", "#1", "200")
	c.expect([]any{int64(100), int64(-56)}, "BITFIELD", "h", "GET", "i8", "#0", "GET", "i8", "#1")
	c.expect([]any{int64(0), int64(-128), int64(-128)}, "BITFIELD", "w", "SET", "i8", "0", "127", "INCRBY", "i8", "0", "1", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "-1")
	c.expect([]any{int64(127), int64(127)}, "BITFIELD", "w", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "300", "GET", "i8", "0")
	c.expect([]any{int64(0)}, "BITFIELD_RO", "nothing", "GET", "i8", "16")
	c.expect(int64(0), "EXISTS", "nothing")
	c.expect(errReply("ERR BITFIELD_RO only supports the GET subcommand"), "BITFIELD_RO", "h", "SET", "i8", "0", "1")
	c.expect(errReply(errBitType), "BITFIELD", "h", "GET", "u64", "0")
	c.expect(errReply("ERR Invalid OVERFLOW type specified"), "BITFIELD", "h", "OVERFLOW", "NONE")
	c.expect([]any{nil}, "BITFIELD", "grow", "OVERFLOW", "FAIL", "SET", "u2", "8", "7")
	c.expect("\x00\x00", "GET", "grow")
}

func TestKeysAndScan(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	for i := range 300 {
		c.expect(status("OK"), "SET", fmt.Sprintf("user:%d", i), "x")
	}
	c.expect(status("OK"), "SET", "other", "y")
	if keys, _ := c.do("KEYS", "user:1?").([]any); len(keys) != 10 {
		t.Fatalf("KEYS user:1? returned %d keys, want 10", len(keys))
	}
	seen := map[string]bool{}
	cursor := "0"
	for {
		reply, _ := c.do("SCAN", cursor, "MATCH", "user:*", "COUNT", "20").([]any)
		if len(reply) != 2 {
			t.Fatalf("bad SCAN reply %#v", reply)
		}
		cursor, _ = reply[0].(string)
		keys, _ := reply[1].([]any)
		for _, k := range keys {
			seen[k.(string)] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 300 {
		t.Fatalf("SCAN returned %d keys, want 300", len(seen))
	}
	c.expect(int64(301), "DBSIZE")
	c.expect(status("OK"), "FLUSHDB")
	c.expect(int64(0), "DBSIZE")
}

func TestPipeliningAndInline(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	raw := encode("SET", "p", "1") + encode("INCR", "p") + encode("SET", "s", "x") + encode("INCR", "s") +
		encode("GET", "p") + encode("INCR", "p") + encode("SET", "p") + "PING\r\n" + encode("GET", "p")
	if _, err := io.WriteString(c.conn, raw); err != nil {
		t.Fatal(err)
	}
	want := []any{status("OK"), int64(2), status("OK"), errReply("ERR value is not an integer or out of range"),
		"2", int64(3), errReply("ERR wrong number of arguments for 'set' command"), status("PONG"), "3"}
	for _, w := range want {
		if got := c.read(); !reflect.DeepEqual(got, w) {
			t.Fatalf("got %#v, want %#v", got, w)
		}
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	if _, err := io.WriteString(c.conn, encode("SET", "a", "1")+encode("SET", "b", "2")+"*1\r\n:5\r\n"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got := c.read(); got != status("OK") {
			t.Fatalf("got %#v before protocol error, want OK", got)
		}
	}
	if got, _ := c.read().(errReply); !strings.HasPrefix(string(got), "ERR Protocol error") {
		t.Fatalf("got %q, want protocol error", got)
	}
	if _, err := c.r.ReadByte(); err == nil {
		t.Fatal("connection still open after protocol error")
	}
}

func TestConcurrentClients(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := io.WriteString(conn, strings.Repeat(encode("INCR", "counter"), 100)); err != nil {
				t.Error(err)
				return
			}
			r := bufio.NewReader(conn)
			for range 100 {
				line, err := r.ReadString('\n')
				if err != nil || line[0] != ':' {
					t.Errorf("reply %q: %v", line, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	dial(t, addr).expect("800", "GET", "counter")
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	c := dial(t, addr)
	c.expect(status("OK"), "SET", "persist", "me")
	c.expect(status("OK"), "SET", "gone", "x")
	c.expect(int64(1), "DEL", "gone")
	c.expect(status("OK"), "SET", "ttl", "v", "EX", "1000")
	c.expect(status("Background append only file rewriting started"), "BGREWRITEAOF")
	stopServer(t, srv, db)

	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	c = dial(t, addr)
	c.expect("me", "GET", "persist")
	c.expect(nil, "GET", "gone")
	c.expect(int64(1000), "TTL", "ttl")
}

func TestMultiExec(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	ok, queued := status("OK"), status("QUEUED")
	c.expect(ok, "SET", "base", "x")
	c.expect(ok, "MULTI")
	c.expect(queued, "SET", "a", "1")
	c.expect(queued, "INCR", "a")
	c.expect(queued, "INCR", "base")
	c.expect(queued, "GET", "a")
	c.expect(queued, "DBSIZE")
	c.expect(queued, "KEYS", "a*")
	c.expect(queued, "PING")
	got := c.do("EXEC")
	want := []any{ok, int64(2), errReply(errNotInteger), "2", int64(2), []any{"a"}, status("PONG")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("EXEC = %#v, want %#v", got, want)
	}
	c.expect("2", "GET", "a")

	c.expect(errReply("ERR EXEC without MULTI"), "EXEC")
	c.expect(errReply("ERR DISCARD without MULTI"), "DISCARD")
	c.expect(ok, "MULTI")
	c.expect(errReply("ERR MULTI calls can not be nested"), "MULTI")
	c.expect(queued, "SET", "discarded", "1")
	c.expect(ok, "DISCARD")
	c.expect(nil, "GET", "discarded")

	c.expect(ok, "MULTI")
	c.expect(queued, "SET", "never", "1")
	c.expect(errReply("ERR unknown command"), "NOSUCHCMD")
	c.expect(errReply("EXECABORT"), "EXEC")
	c.expect(nil, "GET", "never")

	c.expect(ok, "MULTI")
	c.expect(errReply("ERR Command not allowed inside a transaction"), "FLUSHDB")
	c.expect(errReply("ERR wrong number of arguments"), "GET")
	c.expect(errReply("EXECABORT"), "EXEC")
	c.expect(int64(2), "DBSIZE")

	c.expect(ok, "MULTI")
	c.expect(queued, "GET", "a")
	c.expect(queued, "TTL", "a")
	c.expect([]any{"2", int64(-1)}, "EXEC")
}

func TestWatch(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c1, c2 := dial(t, addr), dial(t, addr)
	ok, queued := status("OK"), status("QUEUED")

	c1.expect(ok, "SET", "k", "orig")
	c1.expect(ok, "WATCH", "k", "absent")
	c2.expect(ok, "SET", "k", "changed")
	c1.expect(ok, "MULTI")
	c1.expect(queued, "SET", "k", "mine")
	c1.expect(nil, "EXEC")
	c1.expect("changed", "GET", "k")

	c1.expect(ok, "WATCH", "k")
	c1.expect(ok, "MULTI")
	c1.expect(queued, "SET", "k", "mine")
	c1.expect([]any{ok}, "EXEC")
	c1.expect("mine", "GET", "k")

	c1.expect(ok, "WATCH", "absent")
	c2.expect(ok, "SET", "absent", "now-present")
	c1.expect(ok, "MULTI")
	c1.expect(queued, "GET", "absent")
	c1.expect(nil, "EXEC")

	c1.expect(ok, "WATCH", "brief")
	c2.expect(ok, "SET", "brief", "v")
	c2.expect(int64(1), "DEL", "brief")
	c1.expect(ok, "MULTI")
	c1.expect(queued, "GET", "brief")
	c1.expect(nil, "EXEC")

	for i := range 200 {
		c2.expect(int64(1), "HSET", "big", fmt.Sprintf("f%d", i), "v")
	}
	c1.expect(ok, "WATCH", "big")
	c2.expect(int64(0), "HSET", "big", "f1", "changed")
	c1.expect(ok, "MULTI")
	c1.expect(queued, "HLEN", "big")
	c1.expect(nil, "EXEC")

	c1.expect(ok, "WATCH", "k")
	c2.expect(ok, "SET", "k", "again")
	c1.expect(ok, "UNWATCH")
	c1.expect(ok, "MULTI")
	c1.expect(errReply("ERR WATCH inside MULTI is not allowed"), "WATCH", "k")
	c1.expect(queued, "SET", "k", "final")
	c1.expect([]any{ok}, "EXEC")
	c1.expect("final", "GET", "k")

	c1.expect(ok, "SET", "ttl", "v", "PX", "50")
	c1.expect(ok, "WATCH", "ttl")
	testClock.Advance(100 * time.Millisecond)
	c1.expect(ok, "MULTI")
	c1.expect(queued, "SET", "ttl", "late")
	c1.expect(nil, "EXEC")
}

func TestAuth(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{RequirePass: "s3cret"})
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(errReply("NOAUTH Authentication required"), "PING")
	c.expect(errReply("NOAUTH Authentication required"), "GET", "k")
	c.expect(errReply("WRONGPASS"), "AUTH", "wrong")
	c.expect(errReply("WRONGPASS"), "AUTH", "admin", "s3cret")
	c.expect(status("OK"), "AUTH", "s3cret")
	c.expect(status("PONG"), "PING")

	c2 := dial(t, addr)
	c2.expect(errReply("NOAUTH HELLO must be called"), "HELLO", "2")
	c2.expect(errReply("NOPROTO"), "HELLO", "4", "AUTH", "default", "s3cret")
	c2.expect(errReply("WRONGPASS"), "HELLO", "2", "AUTH", "default", "nope")
	if hello, _ := c2.do("HELLO", "2", "AUTH", "default", "s3cret", "SETNAME", "app").([]any); len(hello) != 14 {
		t.Fatalf("HELLO with AUTH = %#v", hello)
	}
	c2.expect("app", "CLIENT", "GETNAME")
	c2.expect(status("OK"), "SET", "k", "v")

	c3 := dial(t, addr)
	c3.expect(status("OK"), "AUTH", "default", "s3cret")
	c3.expect("v", "GET", "k")

	srv2, db2, addr2 := startServer(t, t.TempDir())
	defer stopServer(t, srv2, db2)
	open := dial(t, addr2)
	open.expect(errReply("ERR AUTH <password> called without any password configured"), "AUTH", "x")
	open.expect(status("PONG"), "PING")
}

type remoteListener struct{ net.Listener }

func (l remoteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return remoteConn{c}, nil
}

type remoteConn struct{ net.Conn }

func (remoteConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 50000}
}

func TestProtectedMode(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{ProtectedMode: true})
	defer stopServer(t, srv, db)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(remoteListener{ln}) }()
	remote := ln.Addr().String()

	refused := dial(t, remote)
	if got, _ := refused.read().(errReply); !strings.HasPrefix(string(got), "DENIED") {
		t.Fatalf("a client from another host got %q, want DENIED", got)
	}
	if _, err := refused.r.ReadByte(); err == nil {
		t.Fatal("protected mode left the connection open")
	}

	dial(t, addr).expect(status("OK"), "CONFIG", "SET", "requirepass", "pw")
	c := dial(t, remote)
	c.expect(errReply("NOAUTH"), "PING")
	c.expect(status("OK"), "AUTH", "pw")
	c.expect(status("PONG"), "PING")
}

func TestClientLimits(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{MaxClients: 2, Timeout: 500 * time.Millisecond})
	defer stopServer(t, srv, db)
	busy, idle := dial(t, addr), dial(t, addr)
	idle.expect(status("PONG"), "PING")
	full := dial(t, addr)
	if got, _ := full.read().(errReply); got != errMaxClients {
		t.Fatalf("a client over the limit got %q", got)
	}
	for range 6 {
		time.Sleep(100 * time.Millisecond)
		busy.expect(status("PONG"), "PING")
	}
	_ = idle.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := idle.r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("an idle client stayed connected past the timeout: %v", err)
	}
	dial(t, addr).expect(status("PONG"), "PING")
}

func TestUnauthenticatedLimits(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{RequirePass: "pw"})
	defer stopServer(t, srv, db)
	for header, want := range map[string]string{
		"*11\r\n":                        "ERR Protocol error: unauthenticated multibulk length",
		"*2\r\n$4\r\nAUTH\r\n$16385\r\n": "ERR Protocol error: unauthenticated bulk length",
	} {
		c := dial(t, addr)
		if _, err := io.WriteString(c.conn, header); err != nil {
			t.Fatal(err)
		}
		if got := c.read(); got != errReply(want) {
			t.Fatalf("%q from a client that has not authenticated = %#v, want %q", header, got, want)
		}
	}
	c := dial(t, addr)
	c.expect(status("OK"), "AUTH", "pw")
	c.expect(status("OK"), "SET", "k", strings.Repeat("v", 16385))
	c.expect(status("OK"), "MSET", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")
}

func TestAuthThrottle(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{RequirePass: "pw"})
	defer stopServer(t, srv, db)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(remoteListener{ln}) }()

	c := dial(t, addr)
	for range authFailures {
		c.expect(errReply("WRONGPASS"), "AUTH", "wrong")
	}
	c.expect(errReply(errAuthLimit), "AUTH", "pw")
	c.expect(errReply(errAuthLimit), "HELLO", "2", "AUTH", "default", "pw")
	dial(t, ln.Addr().String()).expect(status("OK"), "AUTH", "pw")
	time.Sleep(authWindow)
	c.expect(status("OK"), "AUTH", "pw")
}

func TestConfigSet(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(errReply("ERR Unknown option or number of arguments for CONFIG SET - 'dir'"), "CONFIG", "SET", "dir", "/tmp")
	c.expect(errReply("ERR CONFIG SET failed (possibly related to argument 'appendfsync')"), "CONFIG", "SET", "requirepass", "s3cret", "appendfsync", "never")
	c.expect(errReply("ERR CONFIG SET failed (possibly related to argument 'requirepass') - duplicate"), "CONFIG", "SET", "requirepass", "a", "requirepass", "b")
	c.expect(errReply("ERR wrong number of arguments for 'config|set'"), "CONFIG", "SET", "requirepass")
	c.expect([]any{"requirepass", ""}, "CONFIG", "GET", "requirepass")

	c.expect(status("OK"), "CONFIG", "SET", "requirepass", "s3cret", "appendfsync", "always", "proto-max-bulk-len", "8")
	c.expect([]any{"appendfsync", "always", "proto-max-bulk-len", "8", "requirepass", "s3cret"}, "CONFIG", "GET", "appendfsync", "proto-max-bulk-len", "requirepass")
	if db.Options().Sync != bitcask.SyncAlways {
		t.Fatalf("sync policy = %v, want always", db.Options().Sync)
	}
	c.expect(status("OK"), "SET", "long", "0123456789")

	fresh := dial(t, addr)
	fresh.expect(errReply("NOAUTH"), "GET", "long")
	fresh.expect(status("OK"), "AUTH", "s3cret")
	fresh.expect(errReply("ERR Protocol error: invalid bulk length"), "SET", "long", "0123456789")
}

func TestMetrics(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{TrackLatency: true})
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(status("OK"), "SET", "k", "v")
	c.expect("v", "GET", "k")
	var b strings.Builder
	srv.WriteMetrics(&b)
	out := b.String()
	for _, line := range []string{
		`casketdb_build_info{version="` + Version + `"} 1`,
		"casketdb_connected_clients 1",
		"casketdb_commands_processed_total 2",
		`casketdb_command_duration_seconds_bucket{le="+Inf"} 2`,
		"casketdb_command_duration_seconds_count 2",
		"casketdb_keys 1",
	} {
		if !strings.Contains(out, line+"\n") {
			t.Fatalf("metrics lack %q:\n%s", line, out)
		}
	}
	if strings.Contains(out, "casketdb_raft_") {
		t.Fatalf("raft metrics on a single node:\n%s", out)
	}
}

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "abc", true},
		{"a*", "abc", true},
		{"a*c", "abc", true},
		{"a*d", "abc", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{`h\*llo`, "h*llo", true},
		{`h\*llo`, "hello", false},
		{"user:*:name", "user:42:name", true},
		{"user:*:name", "user:42:mail", false},
		{"*a*a*a*a*a*a*a*a*b", strings.Repeat("a", 60), false},
	}
	for _, tt := range tests {
		if got := matchGlob(tt.pattern, tt.s); got != tt.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}
