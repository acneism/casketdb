package server

import (
	"testing"
	"time"
)

func TestPubSub(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	sub, pub := dial(t, addr), dial(t, addr)
	sub.send("SUBSCRIBE", "a", "b")
	sub.expectRead([]any{"subscribe", "a", int64(1)})
	sub.expectRead([]any{"subscribe", "b", int64(2)})
	pub.expect(int64(1), "PUBLISH", "a", "hello")
	sub.expectRead([]any{"message", "a", "hello"})
	sub.send("PSUBSCRIBE", "a*")
	sub.expectRead([]any{"psubscribe", "a*", int64(3)})
	pub.expect(int64(2), "PUBLISH", "a", "twice")
	sub.expectRead([]any{"message", "a", "twice"})
	sub.expectRead([]any{"pmessage", "a*", "a", "twice"})
	pub.expect(int64(1), "PUBLISH", "abc", "x")
	sub.expectRead([]any{"pmessage", "a*", "abc", "x"})
	pub.expect(int64(0), "PUBLISH", "zzz", "nobody")

	sub.expect(errReply("ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"), "GET", "k")
	sub.expect([]any{"pong", ""}, "PING")
	sub.expect([]any{"pong", "x"}, "PING", "x")
	pub.expect(strs("a", "b"), "PUBSUB", "CHANNELS")
	pub.expect(strs("b"), "PUBSUB", "CHANNELS", "b*")
	pub.expect([]any{"a", int64(1), "c", int64(0)}, "PUBSUB", "NUMSUB", "a", "c")
	pub.expect(int64(1), "PUBSUB", "NUMPAT")

	sub.expect([]any{"unsubscribe", "a", int64(2)}, "UNSUBSCRIBE", "a")
	sub.send("UNSUBSCRIBE")
	sub.expectRead([]any{"unsubscribe", "b", int64(1)})
	sub.expect([]any{"punsubscribe", "a*", int64(0)}, "PUNSUBSCRIBE")
	sub.expect([]any{"unsubscribe", nil, int64(0)}, "UNSUBSCRIBE")
	sub.expect(nil, "GET", "k")
	pub.expect(strs(), "PUBSUB", "CHANNELS")

	sub.expect([]any{"ssubscribe", "s1", int64(1)}, "SSUBSCRIBE", "s1")
	pub.expect(int64(0), "PUBLISH", "s1", "plain")
	pub.expect(int64(1), "SPUBLISH", "s1", "sharded")
	sub.expectRead([]any{"smessage", "s1", "sharded"})
	pub.expect(strs("s1"), "PUBSUB", "SHARDCHANNELS")
	pub.expect([]any{"s1", int64(1)}, "PUBSUB", "SHARDNUMSUB", "s1")
	pub.expect(status("OK"), "MULTI")
	pub.expect(errReply("ERR Command not allowed inside a transaction"), "PUBLISH", "a", "x")
	pub.expect(errReply("EXECABORT"), "EXEC")

	gone := dial(t, addr)
	gone.expect([]any{"subscribe", "g", int64(1)}, "SUBSCRIBE", "g")
	gone.conn.Close()
	for i := 0; ; i++ {
		if n, _ := pub.do("PUBSUB", "NUMSUB", "g").([]any); n[1] == int64(0) {
			break
		}
		if i == 500 {
			t.Fatal("a closed subscriber stayed subscribed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSubscriberIgnoresIdleTimeout(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{Timeout: 100 * time.Millisecond})
	defer stopServer(t, srv, db)
	sub := dial(t, addr)
	sub.expect([]any{"subscribe", "c", int64(1)}, "SUBSCRIBE", "c")
	time.Sleep(300 * time.Millisecond)
	dial(t, addr).expect(int64(1), "PUBLISH", "c", "still here")
	sub.expectRead([]any{"message", "c", "still here"})
}
