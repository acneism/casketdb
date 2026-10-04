package server

import (
	"strings"
	"testing"
)

func TestRESP3(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c, pub := dial(t, addr), dial(t, addr)
	hello, ok := c.do("HELLO", "3").(respMap)
	if !ok || hello[4] != "proto" || hello[5] != int64(3) {
		t.Fatalf("HELLO 3 = %#v", hello)
	}
	c.expect(nil, "GET", "missing")
	c.expect(int64(1), "HSET", "h", "field", "v")
	c.expect(respMap{"field", "v"}, "HGETALL", "h")
	c.expect([]any{[]any{"field", "v"}}, "HRANDFIELD", "h", "1", "WITHVALUES")
	c.expect(strs("field"), "HKEYS", "h")
	c.expect(int64(1), "SADD", "s", "a")
	c.expect(respSet{"a"}, "SMEMBERS", "s")
	c.expect(respSet{"a"}, "SINTER", "s")
	c.expect(int64(2), "ZADD", "z", "1.5", "m", "2", "n")
	c.expect(respDouble("1.5"), "ZSCORE", "z", "m")
	c.expect([]any{respDouble("1.5"), nil}, "ZMSCORE", "z", "m", "none")
	c.expect(respDouble("3"), "ZINCRBY", "z", "1.5", "m")
	c.expect([]any{[]any{"n", respDouble("2")}, []any{"m", respDouble("3")}}, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	c.expect(strs("n", "m"), "ZRANGE", "z", "0", "-1")
	c.expect([]any{"0", []any{"m", "3", "n", "2"}}, "ZSCAN", "z", "0")
	c.expect([]any{int64(1), respDouble("3")}, "ZRANK", "z", "m", "WITHSCORE")
	c.expect([]any{"n", respDouble("2")}, "ZPOPMIN", "z")
	c.expect([]any{[]any{"m", respDouble("3")}}, "ZPOPMAX", "z", "1")
	c.expect(int64(1), "GEOADD", "g", "13.361389", "38.115556", "Palermo")
	c.expect([]any{[]any{respDouble("13.36138933897018433"), respDouble("38.11555639549629859")}}, "GEOPOS", "g", "Palermo")
	c.expect("1-0", "XADD", "x", "1-0", "f", "v")
	c.expect(respMap{"x", []any{[]any{"1-0", strs("f", "v")}}}, "XREAD", "STREAMS", "x", "0")
	c.expect(nil, "XREAD", "STREAMS", "x", "$")
	c.expect(respMap{"appendonly", "yes"}, "CONFIG", "GET", "appendonly")
	if info, ok := c.do("INFO").(respVerbatim); !ok || !strings.HasPrefix(string(info), "# Server") {
		t.Fatalf("INFO = %#v", info)
	}
	if groups, ok := c.do("XINFO", "STREAM", "x").(respMap); !ok || groups[0] != "length" {
		t.Fatalf("XINFO STREAM = %#v", groups)
	}

	c.expect(respPush{"subscribe", "ch", int64(1)}, "SUBSCRIBE", "ch")
	c.expect("v", "HGET", "h", "field")
	c.expect(status("PONG"), "PING")
	pub.expect(int64(1), "PUBLISH", "ch", "hi")
	c.expectRead(respPush{"message", "ch", "hi"})
	c.expect(respPush{"unsubscribe", "ch", int64(0)}, "UNSUBSCRIBE", "ch")

	if hello, ok := c.do("HELLO", "2").([]any); !ok || hello[5] != int64(2) {
		t.Fatalf("HELLO 2 = %#v", hello)
	}
	c.expect(nil, "XREAD", "STREAMS", "x", "$")
	c.expect([]any{"field", "v"}, "HGETALL", "h")
}
