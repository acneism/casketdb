package server

import (
	"strconv"
	"strings"
	"testing"
)

func resp3(t *testing.T, addr string) *testConn {
	t.Helper()
	c := dial(t, addr)
	if _, ok := c.do("HELLO", "3").(respMap); !ok {
		t.Fatal("HELLO 3 did not return a map")
	}
	return c
}

func TestClientTracking(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	w := dial(t, addr)
	c := resp3(t, addr)
	c.expect(status("OK"), "CLIENT", "TRACKING", "ON")
	c.expect(respMap{"flags", strs("on"), "redirect", int64(0), "prefixes", strs()}, "CLIENT", "TRACKINGINFO")
	c.expect(int64(0), "CLIENT", "GETREDIR")
	w.expect(status("OK"), "SET", "k", "v")
	c.expect("v", "GET", "k")
	w.expect(status("OK"), "SET", "k", "v2")
	c.expectRead(respPush{"invalidate", strs("k")})
	w.expect(status("OK"), "SET", "k", "v3")
	c.expect(status("PONG"), "PING")
	c.send("GET", "k")
	c.send("SET", "k", "mine")
	c.expectRead("v3")
	c.expectRead(status("OK"))
	c.expectRead(respPush{"invalidate", strs("k")})
	c.expect("mine", "GET", "k")
	w.expect(status("OK"), "FLUSHALL")
	c.expectRead(respPush{"invalidate", nil})
	c.expect(status("OK"), "CLIENT", "TRACKING", "OFF")
	c.expect(respMap{"flags", strs("off"), "redirect", int64(-1), "prefixes", strs()}, "CLIENT", "TRACKINGINFO")

	b := resp3(t, addr)
	b.expect(status("OK"), "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:")
	w.expect(status("OK"), "SET", "user:1", "a")
	b.expectRead(respPush{"invalidate", strs("user:1")})
	w.expect(status("OK"), "SET", "other", "x")
	b.expect(status("PONG"), "PING")

	o := resp3(t, addr)
	o.expect(status("OK"), "CLIENT", "TRACKING", "ON", "OPTIN")
	o.expect(nil, "GET", "a")
	o.expect(status("OK"), "CLIENT", "CACHING", "YES")
	o.expect(nil, "GET", "b")
	w.expect(status("OK"), "SET", "a", "1")
	w.expect(status("OK"), "SET", "b", "1")
	o.expectRead(respPush{"invalidate", strs("b")})
	o.expect(status("PONG"), "PING")

	r := dial(t, addr)
	id, _ := r.do("CLIENT", "ID").(int64)
	r.expect([]any{"subscribe", invalidationChannel, int64(1)}, "SUBSCRIBE", invalidationChannel)
	tr := dial(t, addr)
	tr.expect(status("OK"), "CLIENT", "TRACKING", "ON", "REDIRECT", strconv.FormatInt(id, 10))
	tr.expect(id, "CLIENT", "GETREDIR")
	tr.expect(nil, "GET", "k2")
	w.expect(status("OK"), "SET", "k2", "v")
	r.expectRead([]any{"message", invalidationChannel, strs("k2")})

	h := dial(t, addr)
	h.expect(status("OK"), "CLIENT", "TRACKING", "ON")
	h.do("HELLO", "3")
	h.expect(nil, "GET", "h")
	w.expect(status("OK"), "SET", "h", "1")
	h.expectRead(respPush{"invalidate", strs("h")})

	e := dial(t, addr)
	e.expect(errReply("ERR PREFIX option requires BCAST mode to be enabled"), "CLIENT", "TRACKING", "ON", "PREFIX", "x")
	e.expect(errReply("ERR You can't use both OPTIN and OPTOUT"), "CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT")
	e.expect(errReply("ERR OPTIN and OPTOUT are not compatible with BCAST"), "CLIENT", "TRACKING", "ON", "BCAST", "OPTIN")
	e.expect(errReply("ERR The client ID you want redirect to does not exist"), "CLIENT", "TRACKING", "ON", "REDIRECT", "999999")
	e.expect(errReply("ERR Prefix 'ab' overlaps"), "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a", "PREFIX", "ab")
	e.expect(errReply("ERR CLIENT CACHING can be called only"), "CLIENT", "CACHING", "YES")
	e.expect(int64(-1), "CLIENT", "GETREDIR")
}

func TestTrackingReplyOverPubSubLimit(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := resp3(t, addr)
	c.expect(status("OK"), "CLIENT", "TRACKING", "ON")
	big := strings.Repeat("x", pubsubLimit+1)
	c.expect(status("OK"), "SET", "big", big)
	c.expect(big, "GET", "big")
}
