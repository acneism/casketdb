package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestPubSubAcrossCluster(t *testing.T) {
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
	onLeader := blockOn(t, leader.client, "SUBSCRIBE", "news")
	onFollower := blockOn(t, follower.client, "SUBSCRIBE", "news")
	for _, read := range []func() any{onLeader, onFollower} {
		if got := read(); !reflect.DeepEqual(got, []any{"subscribe", "news", int64(1)}) {
			t.Fatalf("SUBSCRIBE replied %#v", got)
		}
	}
	if n, err := h.pool.one(leader.client, "PUBLISH", "news", "hello"); err != nil || n != int64(1) {
		t.Fatalf("PUBLISH on the leader = %v, %v", n, err)
	}
	for _, read := range []func() any{onLeader, onFollower} {
		if got := read(); !reflect.DeepEqual(got, []any{"message", "news", "hello"}) {
			t.Fatalf("a subscriber got %#v", got)
		}
	}
	if _, err := h.pool.one(follower.client, "PUBLISH", "news", "x"); err == nil || !strings.HasPrefix(err.Error(), "READONLY") {
		t.Fatalf("PUBLISH on a follower = %v, want READONLY", err)
	}
}
