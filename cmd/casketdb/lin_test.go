package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

var (
	faultDuration  = flag.Duration("fault.duration", 0, "load duration of TestFaults; 0 skips it")
	memberDuration = flag.Duration("member.duration", 0, "load duration of TestMembershipChanges; 0 skips it")
	faultOut       = flag.String("fault.out", "", "file for the Porcupine visualization when the check fails")
)

const (
	keys      = 50
	collKeys  = 10
	hotFields = 5
	fillers   = 200
	minEpoch  = 2 * time.Minute
)

func epochKey(epoch int, typ byte, k int) string {
	return fmt.Sprintf("e%d-%c%d", epoch, typ, k)
}

func keysOf(typ byte) int {
	if strings.IndexByte("hzs", typ) >= 0 {
		return collKeys
	}
	return keys
}

type kvInput struct {
	typ   byte
	op    byte
	key   string
	field string
	value int64
}

func (in kvInput) read() bool {
	return in.op == 'g' || in.op == 'n'
}

type kvOutput struct {
	exists  bool
	value   int64
	n       int64
	unknown bool
}

type kvState struct {
	init   bool
	exists bool
	value  int64
	n      int
	list   string
}

var listFill = func() string {
	var b strings.Builder
	for i := range fillers {
		fmt.Fprintf(&b, "%d ", -i-1)
	}
	return b.String()
}()

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			in := op.Input.(kvInput)
			byKey[in.key+"\x00"+in.field] = append(byKey[in.key+"\x00"+in.field], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return kvState{} },
	Step: func(state, input, output any) (bool, any) {
		st, in, out := state.(kvState), input.(kvInput), output.(kvOutput)
		if !st.init {
			st.init = true
			if in.typ == 'l' {
				st.list, st.n = listFill, fillers
			}
		}
		switch in.typ {
		case 'l':
			return listStep(st, in, out)
		case 'x':
			return streamStep(st, in, out)
		}
		read := out.exists == st.exists && (!st.exists || out.value == st.value)
		switch in.op {
		case 'g':
			return read, st
		case 's':
			return true, kvState{init: true, exists: true, value: in.value}
		case 'i':
			next := kvState{init: true, exists: true, value: st.value + 1}
			return out.unknown || out.value == next.value, next
		case 'd':
			return out.unknown || out.exists == st.exists, kvState{init: true}
		}
		return out.unknown || read, kvState{init: true, exists: true, value: in.value}
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(kvInput), output.(kvOutput)
		res := "nil"
		switch {
		case out.unknown:
			res = "?"
		case out.exists:
			res = strconv.FormatInt(out.value, 10)
		}
		if out.n != 0 {
			res += fmt.Sprintf(" (length %d)", out.n)
		}
		var cmds []string
		for _, cmd := range commands(in) {
			cmds = append(cmds, strings.Join(cmd, " "))
		}
		return strings.Join(cmds, "; ") + " -> " + res
	},
}

func listStep(st kvState, in kvInput, out kvOutput) (bool, any) {
	v := strconv.FormatInt(in.value, 10) + " "
	list := st.list
	if in.op == 'm' {
		list += v
	}
	head, rest, ok := strings.Cut(list, " ")
	first, _ := strconv.ParseInt(head, 10, 64)
	matches := out.exists == ok && (!ok || out.value == first)
	switch in.op {
	case 'g':
		return matches, st
	case 'n':
		return out.value == int64(st.n), st
	case 's':
		next := kvState{init: true, list: st.list + v, n: st.n + 1}
		return out.unknown || out.value == int64(next.n), next
	case 'd':
		if !ok {
			return out.unknown || matches, st
		}
		return out.unknown || matches, kvState{init: true, list: rest, n: st.n - 1}
	}
	return out.unknown || matches && out.n == int64(st.n+1), kvState{init: true, list: rest, n: st.n}
}

func streamStep(st kvState, in kvInput, out kvOutput) (bool, any) {
	next := kvState{init: true, exists: true, value: in.value, n: st.n + 1}
	switch in.op {
	case 'g':
		return out.exists == st.exists && (!st.exists || out.value == st.value), st
	case 'n':
		return out.value == int64(st.n), st
	case 's':
		return true, next
	}
	return out.unknown || out.value == int64(next.n), next
}

func commands(in kvInput) [][]string {
	k, f, v := in.key, in.field, strconv.FormatInt(in.value, 10)
	var ops map[byte][]string
	switch in.typ {
	case 'h':
		ops = map[byte][]string{'g': {"HGET", k, f}, 's': {"HSET", k, f, v}, 'i': {"HINCRBY", k, f, "1"}, 'd': {"HDEL", k, f}}
	case 'z':
		ops = map[byte][]string{'g': {"ZSCORE", k, f}, 's': {"ZADD", k, v, f}, 'i': {"ZINCRBY", k, "1", f}, 'd': {"ZREM", k, f}}
	case 's':
		ops = map[byte][]string{'g': {"SISMEMBER", k, f}, 's': {"SADD", k, f}, 'd': {"SREM", k, f}}
	case 'l':
		ops = map[byte][]string{'g': {"LINDEX", k, "0"}, 'n': {"LLEN", k}, 's': {"RPUSH", k, v}, 'd': {"LPOP", k}, 'm': {"LPOP", k}}
	case 'x':
		ops = map[byte][]string{'g': {"XREVRANGE", k, "+", "-", "COUNT", "1"}, 'n': {"XLEN", k}, 's': {"XADD", k, "*", "v", v}, 'm': {"XLEN", k}}
	default:
		ops = map[byte][]string{'g': {"GET", k}, 's': {"SET", k, v}, 'i': {"INCR", k}, 'd': {"DEL", k}}
	}
	switch {
	case in.op != 'm':
		return [][]string{ops[in.op]}
	case ops['m'] != nil:
		return [][]string{{"MULTI"}, ops['s'], ops['m'], {"EXEC"}}
	}
	return [][]string{{"MULTI"}, ops['g'], ops['s'], {"EXEC"}}
}

type outcome int

const (
	done outcome = iota
	retry
	unknown
)

func writeFailed(err error) outcome {
	var re respError
	switch {
	case errors.Is(err, errNotSent):
		return retry
	case errors.As(err, &re) && strings.HasPrefix(string(re), "READONLY "):
		return retry
	}
	return unknown
}

func bulkValue(reply any) (kvOutput, bool) {
	if reply == nil {
		return kvOutput{}, true
	}
	s, ok := reply.(string)
	if !ok {
		return kvOutput{}, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		n = math.MinInt64
	}
	return kvOutput{exists: true, value: n}, true
}

func output(in kvInput, reply any) (kvOutput, bool) {
	switch r := reply.(type) {
	case int64:
		if in.op == 'd' || in.typ == 's' {
			return kvOutput{exists: r == 1}, true
		}
		return kvOutput{exists: true, value: r}, true
	case []any:
		if len(r) == 0 {
			return kvOutput{}, true
		}
		entry, _ := r[0].([]any)
		if len(entry) != 2 {
			return kvOutput{}, false
		}
		fields, _ := entry[1].([]any)
		if len(fields) != 2 {
			return kvOutput{}, false
		}
		return bulkValue(fields[1])
	}
	return bulkValue(reply)
}

func (h *harness) execute(addr string, in kvInput) (kvOutput, outcome) {
	cmds := commands(in)
	if in.op != 'm' {
		reply, err := h.pool.one(addr, cmds[0]...)
		switch {
		case err != nil && in.read():
			return kvOutput{}, retry
		case err != nil:
			return kvOutput{}, writeFailed(err)
		}
		out, ok := output(in, reply)
		switch {
		case !ok && in.read():
			return kvOutput{}, retry
		case !ok:
			return kvOutput{}, unknown
		}
		return out, done
	}
	replies, err := h.pool.do(addr, cmds...)
	if err != nil {
		return kvOutput{}, writeFailed(err)
	}
	if re, ok := replies[3].(respError); ok {
		return kvOutput{}, writeFailed(re)
	}
	results, ok := replies[3].([]any)
	if !ok || len(results) != 2 || slices.ContainsFunc(results, func(r any) bool { _, failed := r.(respError); return failed }) {
		return kvOutput{}, unknown
	}
	reply := results[0]
	if in.typ == 'l' || in.typ == 'x' {
		reply = results[1]
	}
	out, ok := output(in, reply)
	if in.typ == 'l' {
		out.n, _ = results[0].(int64)
	}
	if !ok {
		return kvOutput{}, unknown
	}
	return out, done
}

func randomInput(epoch, client, seq int) kvInput {
	in := kvInput{typ: "khzslx"[rand.IntN(6)], value: int64(client*1_000_000 + seq)}
	switch r := rand.IntN(20); {
	case r < 9:
		in.op = 'g'
	case r < 13:
		in.op = 's'
	case r < 16:
		in.op = 'i'
	case r < 17:
		in.op = 'd'
	default:
		in.op = 'm'
	}
	in.key = epochKey(epoch, in.typ, rand.IntN(keysOf(in.typ)))
	if keysOf(in.typ) == collKeys {
		in.field = "f" + strconv.Itoa(rand.IntN(hotFields))
	}
	switch {
	case in.typ == 's' && in.op == 'i', in.typ == 'x' && in.op == 'd':
		in.op = 'g'
	case (in.typ == 'l' || in.typ == 'x') && in.op == 'i':
		in.op = 'n'
	}
	if in.typ == 's' {
		in.value = 0
	}
	return in
}

func (h *harness) populate(epoch int) {
	deadline := time.Now().Add(time.Minute)
	for _, typ := range []byte("hzsl") {
		for k := range keysOf(typ) {
			key := epochKey(epoch, typ, k)
			fill := []string{map[byte]string{'h': "HSET", 'z': "ZADD", 's': "SADD", 'l': "RPUSH"}[typ], key}
			for i := range fillers {
				switch x := "x" + strconv.Itoa(i); typ {
				case 'h':
					fill = append(fill, x, "0")
				case 'z':
					fill = append(fill, "0", x)
				case 's':
					fill = append(fill, x)
				default:
					fill = append(fill, strconv.Itoa(-i-1))
				}
			}
			for target := h.pick(); ; {
				replies, err := h.pool.do(target, []string{"MULTI"}, []string{"DEL", key}, fill, []string{"EXEC"})
				if err == nil {
					if results, ok := replies[3].([]any); ok && len(results) == 2 {
						break
					}
				}
				if time.Now().After(deadline) {
					h.t.Fatalf("could not fill %s: %v %v", key, err, replies)
				}
				if target = h.leaderOf(h.pick()); target == "" {
					target = h.pick()
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
}

func finalReads(epoch int) []kvInput {
	var out []kvInput
	for _, typ := range []byte("khzslx") {
		for k := range keysOf(typ) {
			key := epochKey(epoch, typ, k)
			switch typ {
			case 'k':
				out = append(out, kvInput{typ: typ, op: 'g', key: key})
			case 'l', 'x':
				out = append(out, kvInput{typ: typ, op: 'g', key: key}, kvInput{typ: typ, op: 'n', key: key})
			default:
				for f := range hotFields {
					out = append(out, kvInput{typ: typ, op: 'g', key: key, field: "f" + strconv.Itoa(f)})
				}
			}
		}
	}
	return out
}

func (h *harness) linearizability(d time.Duration, nemesis func(until time.Time)) {
	epochs := max(1, int(d/minEpoch))
	for epoch := range epochs {
		h.checkEpoch(epoch, d/time.Duration(epochs), nemesis)
	}
}

func (h *harness) checkEpoch(epoch int, d time.Duration, nemesis func(until time.Time)) {
	t := h.t
	h.populate(epoch)
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	deadline := start.Add(d)
	var mu sync.Mutex
	var history []porcupine.Operation
	var clients, completed, unknowns atomic.Int64
	record := func(op porcupine.Operation) {
		mu.Lock()
		history = append(history, op)
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := int(clients.Add(1) - 1)
			writer := h.pick()
			for seq := 0; time.Now().Before(deadline); seq++ {
				in := randomInput(epoch, client, seq)
				target := writer
				if in.read() {
					target = h.pick()
				}
				call := now()
				out, res := h.execute(target, in)
				ret := now()
				switch res {
				case done:
					record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
					completed.Add(1)
				case unknown:
					record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: kvOutput{unknown: true}, Return: math.MaxInt64})
					unknowns.Add(1)
					client = int(clients.Add(1) - 1)
					writer = h.pick()
				default:
					if in.read() {
						continue
					}
					if writer = h.leaderOf(h.pick()); writer == "" {
						writer = h.pick()
						time.Sleep(20 * time.Millisecond)
					}
				}
			}
		}()
	}
	nemesis(deadline)
	wg.Wait()

	final := int(clients.Add(1) - 1)
	finalStart := time.Now()
	for _, in := range finalReads(epoch) {
		for {
			call := now()
			out, res := h.execute(h.pick(), in)
			if res == done {
				record(porcupine.Operation{ClientId: final, Input: in, Call: call, Output: out, Return: now()})
				break
			}
			if time.Since(finalStart) > time.Minute {
				for _, p := range h.live() {
					info := h.replication(p.client)
					t.Logf("%s running=%v state=%s leader=%s membership=%s applied=%s", p.id, p.cmd != nil,
						info["raft_state"], info["raft_leader_id"], info["raft_membership"], info["raft_applied_index"])
				}
				t.Fatalf("the cluster did not answer reads within a minute after the faults stopped; logs in %s", h.dir)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	checkStart := time.Now()
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 5*time.Minute)
	t.Logf("epoch %d: %d operations completed, %d with unknown outcome; checked in %v: %s", epoch, completed.Load(), unknowns.Load(), time.Since(checkStart).Round(time.Millisecond), res)
	if completed.Load() == 0 {
		t.Fatal("no operation completed")
	}
	if res != porcupine.Ok {
		path := *faultOut
		if path == "" {
			path = filepath.Join(h.dir, "linearizability.html")
		}
		if err := porcupine.VisualizePath(kvModel, info, path); err != nil {
			t.Log(err)
		}
		h.dumpIllegalKeys(history, start)
		t.Fatalf("history is %s; visualization in %s, node logs and illegal-*.txt in %s", res, path, h.dir)
	}
}

func (h *harness) dumpIllegalKeys(history []porcupine.Operation, start time.Time) {
	for _, ops := range kvModel.Partition(history) {
		res := porcupine.CheckOperationsTimeout(kvModel, ops, time.Minute)
		if res == porcupine.Ok {
			continue
		}
		slices.SortFunc(ops, func(a, b porcupine.Operation) int { return int(a.Call - b.Call) })
		var b strings.Builder
		for _, op := range ops {
			ret := "never"
			if op.Return != math.MaxInt64 {
				ret = time.Duration(op.Return).String()
			}
			fmt.Fprintf(&b, "%-14v %-14s client %-4d %s\n", time.Duration(op.Call), ret, op.ClientId, kvModel.DescribeOperation(op.Input, op.Output))
		}
		key := ops[0].Input.(kvInput).key
		if field := ops[0].Input.(kvInput).field; field != "" {
			key += "-" + field
		}
		if err := os.WriteFile(filepath.Join(h.dir, "illegal-"+key+".txt"), []byte(b.String()), 0o600); err != nil {
			h.t.Log(err)
		}
		h.t.Logf("key %s: %s; started at %s", key, res, start.Format(time.RFC3339Nano))
	}
}

func TestKVModel(t *testing.T) {
	in := func(typ, op byte, value int64) kvInput { return kvInput{typ: typ, op: op, key: "k", value: value} }
	some := func(v int64) kvOutput { return kvOutput{exists: true, value: v} }
	for _, c := range []struct {
		name  string
		legal bool
		steps []any
	}{
		{"list", true, []any{in('l', 's', 5), some(201), in('l', 'g', 0), some(-1), in('l', 'd', 0), some(-1), in('l', 'n', 0), some(200), in('l', 'm', 6), kvOutput{exists: true, value: -2, n: 201}, in('l', 'g', 0), some(-3), in('l', 'n', 0), some(200)}},
		{"list order", false, []any{in('l', 'd', 0), some(-2)}},
		{"stream", true, []any{in('x', 's', 7), kvOutput{}, in('x', 'n', 0), some(1), in('x', 'g', 0), some(7), in('x', 'm', 8), some(2), in('x', 'g', 0), some(8)}},
		{"stream length", false, []any{in('x', 'n', 0), some(1)}},
		{"hash field", true, []any{in('h', 'g', 0), kvOutput{}, in('h', 's', 3), kvOutput{}, in('h', 'i', 0), some(4), in('h', 'g', 0), some(4), in('h', 'd', 0), kvOutput{exists: true}, in('h', 'g', 0), kvOutput{}}},
		{"set member", false, []any{in('s', 's', 0), kvOutput{}, in('s', 'g', 0), kvOutput{}}},
	} {
		var ops []porcupine.Operation
		for i := 0; i < len(c.steps); i += 2 {
			ops = append(ops, porcupine.Operation{Input: c.steps[i], Output: c.steps[i+1], Call: int64(i), Return: int64(i + 1)})
		}
		if got := porcupine.CheckOperations(kvModel, ops); got != c.legal {
			t.Errorf("%s: linearizable %v, want %v", c.name, got, c.legal)
		}
	}
	entry := []any{[]any{"1-0", []any{"v", "7"}}}
	if out, ok := output(in('x', 'g', 0), entry); !ok || out != some(7) {
		t.Errorf("XREVRANGE reply read as %+v, %v", out, ok)
	}
}

func TestFaults(t *testing.T) {
	if *faultDuration == 0 {
		t.Skip("run with -fault.duration=D")
	}
	h := newHarness(t)
	h.linearizability(*faultDuration, h.faults)
	s := h.stats
	t.Logf("%d kill -9, %d partitions, %d of %d leadership transfers done", s.kills, s.partitions, s.transferred, s.transfers)
	if s.kills == 0 || s.partitions == 0 || s.transfers == 0 {
		t.Fatal("the run was too short to inject every kind of fault")
	}
}

func TestMembershipChanges(t *testing.T) {
	if *memberDuration == 0 {
		t.Skip("run with -member.duration=D")
	}
	h := newHarness(t)
	h.linearizability(*memberDuration, h.membership)
	var final []string
	if l := h.leaderProc(); l != nil {
		final = h.voters(l.client)
		slices.Sort(final)
	}
	s := h.stats
	t.Logf("%d members added (%d failed), %d removed (%d failed); voters at the end: %v", s.added, s.addFailed, s.removed, s.remFailed, final)
	if s.added == 0 || s.removed == 0 {
		t.Fatal("membership did not change")
	}
}
