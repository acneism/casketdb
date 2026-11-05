package main

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestScriptsAcrossCluster(t *testing.T) {
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
	onFollower := blockOn(t, follower.client, "SUBSCRIBE", "news")
	if got := onFollower(); !reflect.DeepEqual(got, []any{"subscribe", "news", int64(1)}) {
		t.Fatalf("SUBSCRIBE replied %#v", got)
	}
	script := "redis.call('SET', KEYS[1], 'v') redis.call('INCR', ARGV[1]) return redis.call('PUBLISH', 'news', 'hi')"
	if n, err := h.pool.one(leader.client, "EVAL", script, "1", "k", "other"); err != nil || n != int64(0) {
		t.Fatalf("EVAL on the leader = %v, %v", n, err)
	}
	if got := onFollower(); !reflect.DeepEqual(got, []any{"message", "news", "hi"}) {
		t.Fatalf("a subscriber on a follower got %#v", got)
	}
	library := "#!lua name=lib\nredis.register_function{function_name='peek', callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}"
	if name, err := h.pool.one(leader.client, "FUNCTION", "LOAD", library); err != nil || name != "lib" {
		t.Fatalf("FUNCTION LOAD on the leader = %v, %v", name, err)
	}
	var got any
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if got, err = h.pool.one(follower.client, "FCALL_RO", "peek", "1", "k"); err == nil && got == "v" {
			break
		}
	}
	if err != nil || got != "v" {
		t.Fatalf("FCALL_RO on a follower = %v, %v", got, err)
	}
	for _, args := range [][]string{{"FUNCTION", "LOAD", "REPLACE", library}, {"EVAL", "return redis.call('SET', KEYS[1], 'x')", "1", "k"}} {
		if _, err := h.pool.one(follower.client, args...); err == nil || !strings.HasPrefix(err.Error(), "READONLY") {
			t.Fatalf("%s on a follower = %v, want READONLY", args[0], err)
		}
	}
	if got, err := h.pool.one(follower.client, "FCALL_RO", "peek", "1", "k"); err != nil || got != "v" {
		t.Fatalf("FCALL_RO after a refused FUNCTION LOAD on the follower = %v, %v", got, err)
	}
}
