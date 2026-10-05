package main

import (
	"bufio"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func blockOn(t *testing.T, addr string, args ...string) func() any {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	return func() any {
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		reply, err := readReply(r)
		if err != nil {
			return err
		}
		return reply
	}
}

func (h *harness) waitBlocked(addr string, n int) {
	h.t.Helper()
	want := fmt.Sprintf("blocked_clients:%d\r\n", n)
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if info, err := h.pool.one(addr, "INFO"); err == nil && strings.Contains(info.(string), want) {
			return
		}
	}
	h.t.Fatalf("%s never had %d blocked clients", addr, n)
}

func (h *harness) waitLeader() *proc {
	h.t.Helper()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if l := h.leaderProc(); l != nil {
			if _, err := h.pool.one(l.client, "SET", "probe", "1"); err == nil {
				return l
			}
		}
	}
	h.t.Fatal("no leader accepted writes")
	return nil
}

func TestBlockingAcrossLeaderChange(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a three-node cluster")
	}
	h := newHarness(t)
	leader := h.waitLeader()
	var follower *proc
	for _, p := range h.live() {
		if p != leader {
			follower = p
			break
		}
	}
	if _, err := h.pool.one(follower.client, "BLPOP", "q", "0"); err == nil || !strings.HasPrefix(err.Error(), "READONLY") {
		t.Fatalf("BLPOP on a follower = %v, want READONLY", err)
	}

	blocked := blockOn(t, leader.client, "BLPOP", "q", "0")
	h.waitBlocked(leader.client, 1)
	for attempt := 1; ; attempt++ {
		_, err := h.pool.one(leader.client, "RAFT", "TRANSFER", follower.id)
		if err == nil || strings.Contains(err.Error(), "not the leader") {
			break
		}
		if attempt == 5 {
			t.Fatalf("RAFT TRANSFER: %v", err)
		}
		t.Logf("RAFT TRANSFER: %v; retrying", err)
		time.Sleep(200 * time.Millisecond)
	}
	if got := blocked(); fmt.Sprint(got) != "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)" {
		t.Fatalf("a client blocked on the old leader got %#v", got)
	}

	leader = h.waitLeader()
	blocked = blockOn(t, leader.client, "BLPOP", "q", "0")
	h.waitBlocked(leader.client, 1)
	if _, err := h.pool.one(leader.client, "RPUSH", "q", "x"); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if got := blocked(); !reflect.DeepEqual(got, []any{"q", "x"}) {
		t.Fatalf("a client blocked on the new leader got %#v", got)
	}
}
