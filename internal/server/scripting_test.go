package server

import (
	"fmt"
	"strings"
	"testing"
)

func TestScripts(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	ok := status("OK")
	c.expect(ok, "EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "k", "v")
	c.expect("v", "EVAL", "return redis.call('GET', KEYS[1])", "1", "k")
	c.expect([]any{int64(1), "v", nil}, "EVAL", "return {1, redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}", "2", "k", "missing")
	c.expect(errReply("ERR Error compiling script"), "EVAL", "return +", "0")
	c.expect(errReply("WRONGTYPE"), "EVAL", "redis.call('LPUSH', KEYS[1], 'x') return redis.call('GET', KEYS[1])", "1", "list")
	c.expect(errReply("ERR user_script:1: boom script:"), "EVAL", "redis.call('SET', KEYS[1], 'kept') error('boom')", "1", "e")
	c.expect("kept", "GET", "e")

	sha := c.do("SCRIPT", "LOAD", "return redis.call('INCR', KEYS[1])").(string)
	c.expect(int64(1), "EVALSHA", sha, "1", "n")
	c.expect(int64(2), "EVALSHA", strings.ToUpper(sha), "1", "n")
	c.expect([]any{int64(1), int64(0)}, "SCRIPT", "EXISTS", sha, strings.Repeat("0", 40))
	c.expect(errReply("NOSCRIPT"), "EVALSHA", strings.Repeat("0", 40), "0")
	c.expect(ok, "SCRIPT", "FLUSH")
	c.expect(errReply("NOSCRIPT"), "EVALSHA", sha, "1", "n")

	c.expect("v", "EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "k")
	c.expect(errReply("ERR Write commands are not allowed from read-only scripts."), "EVAL_RO", "return redis.call('SET', KEYS[1], 'x')", "1", "k")
	c.expect(errReply("ERR Write commands are not allowed from read-only scripts."), "EVAL", "#!lua flags=no-writes\nreturn redis.call('SET', KEYS[1], 'x')", "1", "k")
	c.expect(errReply("ERR This Redis command is not allowed from script"), "EVAL", "return redis.call('CONFIG', 'GET', 'x')", "0")
	c.expect(errReply("ERR This Redis command is not allowed from script"), "EVAL", "return redis.call('EVAL', 'return 1', '0')", "0")
	c.expect(errReply("ERR Unknown Redis command called from script"), "EVAL", "return redis.call('NOSUCH')", "0")
	c.expect(int64(2), "EVAL", "return #redis.call('TIME')", "0")
	c.expect(ok, "EVAL", "return redis.call('SELECT', 0)", "0")
	c.expect(errReply("ERR Number of keys can't be greater than number of args"), "EVAL", "return 1", "2", "a")
}

func TestScriptsOutsideTheirKeys(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	for i := range 20 {
		key := fmt.Sprintf("undeclared:%d", i)
		c.expect(int64(1), "EVAL", "redis.call('INCR', KEYS[1]) return redis.call('INCR', ARGV[1])", "1", "declared", key)
		c.expect("1", "GET", key)
	}
	c.expect("20", "GET", "declared")
	c.expect(int64(21), "EVAL", "return redis.call('DBSIZE')", "0")
	c.expect(int64(20), "EVAL", "return #redis.call('KEYS', 'undeclared:*')", "0")
}

func TestScriptsInTransactionsAndPipelines(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(status("OK"), "MULTI")
	c.expect(status("QUEUED"), "EVAL", "return redis.call('INCR', KEYS[1])", "1", "m")
	c.expect(status("QUEUED"), "EVAL", "return redis.call('INCR', ARGV[1])", "0", "m")
	c.expect(status("QUEUED"), "GET", "m")
	c.expect([]any{int64(1), int64(2), "2"}, "EXEC")

	var pipeline strings.Builder
	for i := range 10 {
		pipeline.WriteString(encode("EVAL", "return redis.call('INCRBY', KEYS[1], ARGV[1])", "1", "p", fmt.Sprint(i)))
		pipeline.WriteString(encode("EVAL", "return redis.call('INCR', ARGV[1])", "0", fmt.Sprintf("q%d", i)))
	}
	if _, err := c.conn.Write([]byte(pipeline.String())); err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for i := range 10 {
		total += int64(i)
		if got := c.read(); got != total {
			t.Fatalf("pipelined INCRBY %d: got %#v, want %d", i, got, total)
		}
		if got := c.read(); got != int64(1) {
			t.Fatalf("pipelined INCR of q%d: got %#v, want 1", i, got)
		}
	}
}

func TestScriptsUseCallerProtocol(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.do("HELLO", "3")
	c.expect(respMap{}, "EVAL", "redis.setresp(3) return redis.call('HGETALL', KEYS[1])", "1", "h")
	c.do("HSET", "h", "a", "1")
	c.expect(respMap{"a", "1"}, "EVAL", "redis.setresp(3) return redis.call('HGETALL', KEYS[1])", "1", "h")
	c.expect(int64(1), "EVAL", "return true", "0")
	c.expect(respDouble("1.5"), "EVAL", "return {double=1.5}", "0")
}

func TestScriptACL(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	admin := dial(t, addr)
	admin.expect(status("OK"), "ACL", "SETUSER", "u", "on", ">pw", "+eval", "+get", "~a*")
	u := dial(t, addr)
	u.expect(status("OK"), "AUTH", "u", "pw")
	u.expect(errReply("ERR ACL failure in script: User u has no permissions to run the 'set' command script:"), "EVAL", "return redis.call('set', KEYS[1], 1)", "1", "a1")
	u.expect(errReply("NOPERM No permissions to access a key"), "EVAL", "return redis.call('get', KEYS[1])", "1", "b1")
	u.expect(errReply("ERR ACL failure in script: No permissions to access a key"), "EVAL", "return redis.pcall('get', 'b2')", "0")
	log := admin.do("ACL", "LOG", "1").([]any)[0].([]any)
	if log[5] != "lua" || log[7] != "b2" {
		t.Fatalf("ACL LOG entry for a key denied in a script: %#v", log)
	}
	admin.expect(status("OK"), "ACL", "SETUSER", "w", "on", ">pw", "allkeys", "+@all", "-@write")
	w := dial(t, addr)
	w.expect(status("OK"), "AUTH", "w", "pw")
	w.expect(int64(1), "EVAL", "return 1", "0")
}

func TestScriptPublishesAfterCommit(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	sub := dial(t, addr)
	sub.expect([]any{"subscribe", "news", int64(1)}, "SUBSCRIBE", "news")
	c := dial(t, addr)
	c.expect(int64(1), "EVAL", "redis.call('SET', KEYS[1], 'v') return redis.call('PUBLISH', 'news', 'hi')", "1", "k")
	if got := sub.read(); fmt.Sprint(got) != "[message news hi]" {
		t.Fatalf("subscriber got %#v", got)
	}
	c.expect(int64(1), "EVAL", "redis.call('INCR', ARGV[1]) return redis.call('PUBLISH', 'news', 'once')", "0", "elsewhere")
	if got := sub.read(); fmt.Sprint(got) != "[message news once]" {
		t.Fatalf("subscriber got %#v", got)
	}
	c.expect(int64(1), "PUBLISH", "news", "last")
	if got := sub.read(); fmt.Sprint(got) != "[message news last]" {
		t.Fatalf("a message published in a retried script arrived twice, then %#v", got)
	}
}

func TestFunctionsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	c := dial(t, addr)
	library := "#!lua name=lib\nredis.register_function('bump', function(keys) return redis.call('INCR', keys[1]) end)\nredis.register_function{function_name='peek', callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}"
	c.expect("lib", "FUNCTION", "LOAD", library)
	c.expect(errReply("ERR Library 'lib' already exists"), "FUNCTION", "LOAD", library)
	c.expect(int64(1), "FCALL", "bump", "1", "f")
	c.expect("1", "FCALL_RO", "peek", "1", "f")
	c.expect(errReply("ERR Can not execute a script with write flag using *_ro command."), "FCALL_RO", "bump", "1", "f")
	stopServer(t, srv, db)

	srv, db, addr = startServer(t, dir)
	c = dial(t, addr)
	c.expect(int64(2), "FCALL", "bump", "1", "f")
	c.expect(status("OK"), "FUNCTION", "DELETE", "lib")
	c.expect(errReply("ERR Function not found"), "FCALL", "bump", "1", "f")
	c.expect(status("OK"), "ACL", "SETUSER", "default", "on", "nopass", "allkeys", "allcommands")
	stopServer(t, srv, db)

	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	c = dial(t, addr)
	c.expect(errReply("ERR Function not found"), "FCALL", "bump", "1", "f")
	c.expect([]any{}, "FUNCTION", "LIST")
}
