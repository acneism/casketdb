package server

import (
	"fmt"
	"strconv"
	"testing"
)

func TestRenameAndCopy(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	ok := status("OK")

	c.expect(ok, "SET", "a", "1")
	c.expect(ok, "RENAME", "a", "b")
	c.expect("1", "GET", "b")
	c.expect(int64(0), "EXISTS", "a")
	c.expect(errReply("ERR no such key"), "RENAME", "a", "c")
	c.expect(errReply("ERR no such key"), "RENAMENX", "a", "c")
	c.expect(ok, "RENAME", "b", "b")
	c.expect(int64(0), "RENAMENX", "b", "b")
	c.expect(ok, "SET", "c", "2")
	c.expect(int64(0), "RENAMENX", "b", "c")
	c.expect(int64(1), "RENAMENX", "b", "d")

	for i := range 200 {
		c.expect(int64(1), "HSET", "h", fmt.Sprintf("f%d", i), fmt.Sprintf("v%d", i))
	}
	c.expect(int64(1), "PEXPIRE", "h", "100000")
	c.expect(int64(1), "COPY", "h", "h2")
	c.expect(int64(0), "COPY", "h", "h2")
	c.expect(int64(200), "HLEN", "h2")
	c.expect("v150", "HGET", "h2", "f150")
	if ttl, _ := c.do("PTTL", "h2").(int64); ttl <= 0 || ttl > 100000 {
		t.Fatalf("PTTL of the copy = %d", ttl)
	}
	c.expect(int64(0), "HSET", "h2", "f0", "changed")
	c.expect("v0", "HGET", "h", "f0")
	c.expect(int64(1), "COPY", "h", "h2", "REPLACE")
	c.expect("v0", "HGET", "h2", "f0")
	c.expect(ok, "RENAME", "h", "h3")
	c.expect(int64(0), "EXISTS", "h")
	c.expect(int64(200), "HLEN", "h3")
	c.expect(ok, "RENAME", "h3", "d")
	c.expect(status("hash"), "TYPE", "d")
	c.expect("v199", "HGET", "d", "f199")

	for i := range 200 {
		c.expect(int64(1), "ZADD", "z", strconv.Itoa(i), fmt.Sprintf("m%d", i))
		c.expect(int64(i+1), "RPUSH", "l", strconv.Itoa(i))
	}
	c.expect(int64(1), "COPY", "z", "z2")
	c.expect([]any{"m198", "198", "m199", "199"}, "ZRANGE", "z2", "-2", "-1", "WITHSCORES")
	c.expect(ok, "RENAME", "l", "l2")
	c.expect([]any{"0", "1"}, "LRANGE", "l2", "0", "1")
	c.expect(int64(200), "LLEN", "l2")

	c.expect("1-0", "XADD", "s", "1-0", "f", "v")
	c.expect(ok, "XGROUP", "CREATE", "s", "g", "0")
	c.expect(int64(1), "COPY", "s", "s2")
	c.expect([]any{[]any{"s2", []any{[]any{"1-0", []any{"f", "v"}}}}}, "XREADGROUP", "GROUP", "g", "c", "STREAMS", "s2", ">")

	c.expect(errReply("ERR source and destination objects are the same"), "COPY", "z", "z")
	c.expect(errReply("ERR DB index is out of range"), "COPY", "z", "z3", "DB", "1")
	c.expect(int64(1), "COPY", "z", "z3", "DB", "0")
	c.expect(errReply("ERR syntax error"), "COPY", "z", "z4", "SIDEWAYS")
	c.expect(int64(0), "COPY", "missing", "z5")
}
