package server

import (
	"reflect"
	"testing"
)

func pendingWithoutIdle(t *testing.T, c *testConn, args ...string) []any {
	t.Helper()
	got, ok := c.do(args...).([]any)
	if !ok {
		t.Fatalf("%q did not return an array", args)
	}
	for _, p := range got {
		item := p.([]any)
		if idle, _ := item[2].(int64); idle < 0 {
			t.Fatalf("%q: negative idle %d", args, idle)
		}
		item[2] = int64(0)
	}
	return got
}

func TestStreamGroups(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	a, c := dial(t, addr), dial(t, addr)
	expectPending := func(want []any, args ...string) {
		t.Helper()
		if got := pendingWithoutIdle(t, c, args...); !reflect.DeepEqual(got, want) {
			t.Fatalf("%q = %#v, want %#v", args, got, want)
		}
	}
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		c.expect(id, "XADD", "s", id, "n", id[:1])
	}
	c.expect(status("OK"), "XGROUP", "CREATE", "s", "g", "0")
	c.expect(errReply(errBusyGroup), "XGROUP", "CREATE", "s", "g", "0")
	c.expect(errReply("ERR The XGROUP subcommand requires the key to exist"), "XGROUP", "CREATE", "missing", "g", "0")
	c.expect(status("OK"), "XGROUP", "CREATE", "fresh", "g", "$", "MKSTREAM")
	c.expect(int64(0), "XLEN", "fresh")
	c.expect([]any{[]any{"name", "g", "consumers", int64(0), "pending", int64(0), "last-delivered-id", "0-0", "entries-read", nil, "lag", int64(3)}}, "XINFO", "GROUPS", "s")

	c.expect([]any{[]any{"s", []any{entry("1-0", "n", "1")}}}, "XREADGROUP", "GROUP", "g", "Alice", "COUNT", "1", "STREAMS", "s", ">")
	c.expect([]any{[]any{"s", []any{entry("2-0", "n", "2"), entry("3-0", "n", "3")}}}, "XREADGROUP", "GROUP", "g", "Bob", "STREAMS", "s", ">")
	c.expect(nil, "XREADGROUP", "GROUP", "g", "Bob", "STREAMS", "s", ">")
	c.expect([]any{int64(3), "1-0", "3-0", []any{strs("Alice", "1"), strs("Bob", "2")}}, "XPENDING", "s", "g")
	expectPending([]any{
		[]any{"1-0", "Alice", int64(0), int64(1)},
		[]any{"2-0", "Bob", int64(0), int64(1)},
		[]any{"3-0", "Bob", int64(0), int64(1)},
	}, "XPENDING", "s", "g", "-", "+", "10")
	expectPending([]any{[]any{"3-0", "Bob", int64(0), int64(1)}}, "XPENDING", "s", "g", "(2-0", "+", "10", "Bob")
	c.expect([]any{[]any{"s", []any{entry("2-0", "n", "2"), entry("3-0", "n", "3")}}}, "XREADGROUP", "GROUP", "g", "Bob", "STREAMS", "s", "0")
	expectPending([]any{[]any{"2-0", "Bob", int64(0), int64(2)}}, "XPENDING", "s", "g", "IDLE", "0", "-", "+", "1", "Bob")
	c.expect([]any{[]any{"name", "g", "consumers", int64(2), "pending", int64(3), "last-delivered-id", "3-0", "entries-read", int64(3), "lag", int64(0)}}, "XINFO", "GROUPS", "s")

	c.expect(int64(1), "XACK", "s", "g", "2-0", "9-9")
	c.expect([]any{int64(2), "1-0", "3-0", []any{strs("Alice", "1"), strs("Bob", "1")}}, "XPENDING", "s", "g")
	c.expect([]any{entry("1-0", "n", "1")}, "XCLAIM", "s", "g", "Carol", "0", "1-0")
	expectPending([]any{[]any{"1-0", "Carol", int64(0), int64(2)}}, "XPENDING", "s", "g", "-", "+", "10", "Carol")
	c.expect(strs("3-0"), "XCLAIM", "s", "g", "Dave", "0", "3-0", "JUSTID")
	c.expect([]any{}, "XCLAIM", "s", "g", "Dave", "3600000", "1-0")
	c.expect([]any{"3-0", []any{entry("1-0", "n", "1")}, []any{}}, "XAUTOCLAIM", "s", "g", "Eve", "0", "0", "COUNT", "1")
	c.expect([]any{"0-0", []any{entry("3-0", "n", "3")}, []any{}}, "XAUTOCLAIM", "s", "g", "Eve", "0", "3-0")
	c.expect(int64(1), "XDEL", "s", "3-0")
	c.expect([]any{"0-0", strs("1-0"), strs("3-0")}, "XAUTOCLAIM", "s", "g", "Eve", "0", "0", "JUSTID")
	expectPending([]any{[]any{"1-0", "Eve", int64(0), int64(3)}}, "XPENDING", "s", "g", "-", "+", "10")
	consumers, _ := c.do("XINFO", "CONSUMERS", "s", "g").([]any)
	var names []string
	for _, info := range consumers {
		names = append(names, info.([]any)[1].(string))
	}
	if !reflect.DeepEqual(names, []string{"Alice", "Bob", "Carol", "Dave", "Eve"}) {
		t.Fatalf("XINFO CONSUMERS names = %v", names)
	}
	c.expect(int64(1), "XGROUP", "DELCONSUMER", "s", "g", "Eve")
	c.expect([]any{int64(0), nil, nil, nil}, "XPENDING", "s", "g")
	c.expect(int64(1), "XGROUP", "CREATECONSUMER", "s", "g", "Zed")
	c.expect(int64(0), "XGROUP", "CREATECONSUMER", "s", "g", "Zed")

	c.expect([]any{entry("2-0", "n", "2")}, "XCLAIM", "s", "g", "Zed", "0", "2-0", "FORCE", "RETRYCOUNT", "7")
	expectPending([]any{[]any{"2-0", "Zed", int64(0), int64(7)}}, "XPENDING", "s", "g", "-", "+", "10")
	c.expect(status("OK"), "XGROUP", "SETID", "s", "g", "0")
	c.expect([]any{[]any{"s", []any{entry("1-0", "n", "1"), entry("2-0", "n", "2")}}}, "XREADGROUP", "GROUP", "g", "Zed", "NOACK", "STREAMS", "s", ">")
	expectPending([]any{[]any{"2-0", "Zed", int64(0), int64(7)}}, "XPENDING", "s", "g", "-", "+", "10")

	full, _ := c.do("XINFO", "STREAM", "s", "FULL").([]any)
	groups, _ := full[len(full)-1].([]any)
	if len(groups) != 1 || groups[0].([]any)[1] != "g" || groups[0].([]any)[9] != int64(1) {
		t.Fatalf("XINFO STREAM FULL groups = %#v", groups)
	}

	c.expect(status("OK"), "XGROUP", "CREATE", "s", "w", "$")
	a.send("XREADGROUP", "GROUP", "w", "waiter", "BLOCK", "0", "STREAMS", "s", ">")
	waitBlocked(t, c, 1)
	c.expect("4-0", "XADD", "s", "4-0", "n", "4")
	a.expectRead([]any{[]any{"s", []any{entry("4-0", "n", "4")}}})

	c.expect(errReply("NOGROUP No such key 's' or consumer group 'nosuch' in XREADGROUP with GROUP option"), "XREADGROUP", "GROUP", "nosuch", "c", "STREAMS", "s", ">")
	c.expect(errReply("ERR The $ ID is meaningless"), "XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "$")
	c.expect(errReply("ERR Missing GROUP option for XREADGROUP"), "XREADGROUP", "COUNT", "1", "BLOCK", "1", "STREAMS", "s", ">")
	c.expect(errReply("NOGROUP No such key 's' or consumer group 'nosuch'"), "XPENDING", "s", "nosuch")
	c.expect(errReply("NOGROUP No such consumer group 'nosuch' for key name 's'"), "XGROUP", "SETID", "s", "nosuch", "0")
	c.expect(errReply("ERR Unrecognized XCLAIM option 'BOGUS'"), "XCLAIM", "s", "g", "c", "0", "1-0", "BOGUS")
	c.expect(int64(0), "XACK", "s", "nosuch", "1-0")

	check := func() {
		t.Helper()
		c := dial(t, addr)
		got := pendingWithoutIdle(t, c, "XPENDING", "s", "g", "-", "+", "10")
		if !reflect.DeepEqual(got, []any{[]any{"2-0", "Zed", int64(0), int64(7)}}) {
			t.Fatalf("XPENDING after restart = %#v", got)
		}
		c.expect([]any{int64(1), "4-0", "4-0", []any{strs("waiter", "1")}}, "XPENDING", "s", "w")
	}
	check()
	stopServer(t, srv, db)
	srv, db, addr = startServer(t, dir)
	check()
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	stopServer(t, srv, db)
	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	check()
	c = dial(t, addr)
	c.expect(int64(1), "XGROUP", "DESTROY", "s", "g")
	c.expect(int64(0), "XGROUP", "DESTROY", "s", "g")
	c.expect([]any{[]any{"name", "w", "consumers", int64(1), "pending", int64(1), "last-delivered-id", "4-0", "entries-read", int64(4), "lag", int64(0)}}, "XINFO", "GROUPS", "s")
}
