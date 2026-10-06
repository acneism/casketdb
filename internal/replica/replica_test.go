package replica

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
	"github.com/acneism/raft/transport"
	raftwal "github.com/acneism/raft/wal"
)

type testNode struct {
	id      string
	dir     string
	peers   map[string]string
	db      *bitcask.DB
	node    *Node
	unsafe  bool
	reads   ReadMode
	join    bool
	tls     *tls.Config
	tune    func(*node.Config)
	cluster string
}

var tuning = testTuning

func testTuning(c *node.Config) {
	c.TickInterval = time.Millisecond
	c.ElectionTicks = 500
	if n, err := strconv.Atoi(os.Getenv("CASKETDB_TEST_ELECTION_TICKS")); err == nil {
		c.ElectionTicks = n
	}
	c.CompactEntries = 16
	c.TrailingEntries = 4
	c.SegmentSize = 8 << 10
}

func (tn *testNode) start(t testing.TB) {
	t.Helper()
	opts := bitcask.DefaultOptions()
	opts.Logs = 2
	db, err := bitcask.Open(filepath.Join(tn.dir, "data"), opts)
	if err != nil {
		t.Fatal(err)
	}
	n, err := open(db, Config{ID: tn.id, Peers: tn.peers, Dir: filepath.Join(tn.dir, "raft"), UnsafeNoFsync: tn.unsafe, TLS: tn.tls, Reads: tn.reads, Join: tn.join, ClusterID: tn.cluster}, func(c *node.Config) {
		tuning(c)
		if tn.tune != nil {
			tn.tune(c)
		}
	})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	tn.db, tn.node = db, n
}

func (tn *testNode) stop(t testing.TB) {
	t.Helper()
	if tn.node == nil {
		return
	}
	if err := tn.node.Close(); err != nil && !errors.Is(err, node.ErrRemoved) {
		t.Error(err)
	}
	if err := tn.db.Close(); err != nil {
		t.Error(err)
	}
	tn.node = nil
}

func newCluster(t testing.TB, size int, unsafe bool, setup ...func(*testNode)) []*testNode {
	peers := make(map[string]string)
	nodes := make([]*testNode, size)
	for i := range nodes {
		id := "n" + strconv.Itoa(i)
		peers[id] = freeAddr(t)
		nodes[i] = &testNode{id: id, dir: t.TempDir(), peers: peers, unsafe: unsafe}
		for _, fn := range setup {
			fn(nodes[i])
		}
	}
	for _, tn := range nodes {
		tn.start(t)
	}
	t.Cleanup(func() {
		for _, tn := range nodes {
			tn.stop(t)
		}
	})
	return nodes
}

func eachMode(t *testing.T, fn func(t *testing.T, unsafe bool)) {
	for _, unsafe := range []bool{false, true} {
		t.Run("unsafe-no-fsync="+strconv.FormatBool(unsafe), func(t *testing.T) { fn(t, unsafe) })
	}
}

var usedPorts sync.Map

func freeAddr(t testing.TB) string {
	for range 1000 {
		port := 20000 + rand.IntN(12000)
		if _, taken := usedPorts.LoadOrStore(port, true); taken {
			continue
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		ln.Close()
		return ln.Addr().String()
	}
	t.Fatal("no free port outside the ephemeral range")
	return ""
}

func eventually(t testing.TB, what string, cond func() bool, explain ...func() string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			var details strings.Builder
			for _, fn := range explain {
				details.WriteString(fn())
			}
			t.Fatalf("timed out waiting for %s%s", what, details.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func nodeState(tn *testNode) string {
	if tn.node == nil {
		return "\n" + tn.id + ": stopped"
	}
	var snaps []string
	des, _ := os.ReadDir(filepath.Join(tn.dir, "raft", "snap"))
	for _, de := range des {
		snaps = append(snaps, de.Name())
	}
	return fmt.Sprintf("\n%s: %+v durable=%d snap=%v", tn.id, tn.node.Status(), tn.db.DurableIndex(), snaps)
}

func clusterState(nodes []*testNode) func() string {
	return func() string {
		var b strings.Builder
		for _, tn := range nodes {
			b.WriteString(nodeState(tn))
		}
		return b.String()
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func leader(t testing.TB, nodes []*testNode) *testNode {
	t.Helper()
	var found *testNode
	eventually(t, "a ready leader", func() bool {
		for _, tn := range nodes {
			if tn.node != nil && tn.node.ready.Load() != 0 {
				found = tn
				return true
			}
		}
		return false
	})
	return found
}

func follower(nodes []*testNode, l *testNode) *testNode {
	if nodes[0] != l {
		return nodes[0]
	}
	return nodes[1]
}

func get(tn *testNode, key string) string {
	var v []byte
	_ = tn.db.View(bitcask.Keys(key), func(tx *bitcask.Tx) error {
		var err error
		v, _, err = tx.Get(key)
		return err
	})
	return string(v)
}

func put(tn *testNode, key, value string) error {
	return tn.node.Update(bitcask.Keys(key), func(tx *bitcask.Tx) error {
		tx.Put(key, []byte(value), 0)
		return nil
	})
}

func incr(tn *testNode, key string) error {
	return tn.node.Update(bitcask.Keys(key), func(tx *bitcask.Tx) error {
		v, _, err := tx.Get(key)
		if err != nil {
			return err
		}
		n, _ := strconv.Atoi(string(v))
		tx.Put(key, []byte(strconv.Itoa(n+1)), 0)
		return nil
	})
}

func converged(nodes []*testNode, key, want string, keys int) func() bool {
	return func() bool {
		for _, tn := range nodes {
			if tn.node != nil && (get(tn, key) != want || tn.db.Len() != keys) {
				return false
			}
		}
		return true
	}
}

func latestSnapshot(tn *testNode) (raft.SnapshotMeta, string, bool) {
	raftDir := filepath.Join(tn.dir, "raft")
	des, _ := os.ReadDir(filepath.Join(raftDir, "snap"))
	var best raft.SnapshotMeta
	var path string
	for _, de := range des {
		var term, index uint64
		if _, err := fmt.Sscanf(de.Name(), "%016x-%016x", &term, &index); err != nil || len(de.Name()) != 33 {
			continue
		}
		if index > best.Index {
			best, path = raft.SnapshotMeta{Index: index, Term: term}, filepath.Join(raftDir, "snap", de.Name())
		}
	}
	return best, path, path != ""
}

func voters(tn *testNode) raft.ConfState {
	cs := raft.ConfState{Addrs: map[raft.NodeID]string{}}
	for id, addr := range tn.peers {
		cs.Voters = append(cs.Voters, raft.NodeID(id))
		cs.Addrs[raft.NodeID(id)] = addr
	}
	slices.Sort(cs.Voters)
	return cs
}

func compact(t *testing.T, nodes []*testNode, l *testNode, prefix string) {
	t.Helper()
	for _, tn := range nodes {
		if tn.node != nil {
			must(t, tn.db.Sync())
		}
	}
	durable := l.db.DurableIndex()
	for i := range 20 {
		must(t, put(l, prefix+strconv.Itoa(i), "v"))
	}
	eventually(t, "the leader to compact its log", func() bool { return l.node.Status().FirstIndex+4 > durable }, clusterState(nodes))
}

func stopFollower(t *testing.T, nodes []*testNode, l, f *testNode) {
	t.Helper()
	f.stop(t)
	must(t, put(l, "stopped", f.id))
	eventually(t, "the leader to notice that "+f.id+" stopped", func() bool {
		return l.node.rn.Status().Progress[raft.NodeID(f.id)].State != raft.ProgressReplicate
	}, clusterState(nodes))
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	must(t, filepath.WalkDir(from, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0o644)
	}))
}

func entry(index uint64, kind byte, ops ...bitcask.Op) raft.Entry {
	return raft.Entry{Index: index, Term: 1, Data: encodeEntry(kind, ops)}
}

func FuzzEntry(f *testing.F) {
	f.Add(false, encodeEntry(kindOps, []bitcask.Op{{Key: "k", Value: []byte("v"), ExpireAt: 5}, {Key: "d", Delete: true}})[1:])
	f.Add(true, encodeEntry(kindOps, []bitcask.Op{{Key: "h", Value: []byte("fields"), Kind: 1}, {Key: "s", Value: []byte("v")}})[1:])
	f.Add(true, encodeEntry(kindOps, []bitcask.Op{{Key: "t", Value: []byte("gen00000"), Kind: 0x84}, {Key: "t", Member: "f", IsMember: true, Value: []byte("v")}, {Key: "t", Member: "", IsMember: true, Delete: true}})[1:])
	f.Add(false, []byte{0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, typed bool, data []byte) {
		ops, err := decodeOps(bytes.NewReader(data), typed)
		if err != nil {
			return
		}
		entry := encodeEntry(kindOps, ops)
		again, err := decodeOps(bytes.NewReader(entry[1:]), entry[0] == kindTypedOps)
		if err != nil || !reflect.DeepEqual(again, ops) {
			t.Fatalf("ops %#v came back as %#v, %v", ops, again, err)
		}
	})
}

func FuzzSnapshotInfo(f *testing.F) {
	info := appendFileList(nil, 4, 2)
	info = appendFileEntry(info, bitcask.SnapshotFile{Path: "log-000/000001.data", Size: 100})
	info = appendFileEntry(info, bitcask.SnapshotFile{Path: "SYSTEM", Size: 12})
	f.Add(info)
	f.Add(append(appendFileList(nil, 1, 1), 5, '.', '.', '/', 'x', 'y', 1))
	f.Fuzz(func(t *testing.T, data []byte) {
		logs, files, err := readFileList(bytes.NewReader(data))
		if err != nil {
			return
		}
		b := appendFileList(nil, logs, len(files))
		for _, sf := range files {
			if !filepath.IsLocal(filepath.FromSlash(sf.Path)) || sf.Size < 0 {
				t.Fatalf("accepted %#v", sf)
			}
			b = appendFileEntry(b, sf)
		}
		again, files2, err := readFileList(bytes.NewReader(b))
		if err != nil || again != logs || !slices.Equal(files2, files) {
			t.Fatalf("%d logs %#v came back as %d logs %#v, %v", logs, files, again, files2, err)
		}
	})
}

func TestConcurrentWritesReplicate(t *testing.T) {
	eachMode(t, testConcurrentWrites)
}

func testConcurrentWrites(t *testing.T, unsafe bool) {
	nodes := newCluster(t, 3, unsafe)
	l := leader(t, nodes)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				if err := incr(l, "counter"); err != nil {
					errs <- err
					return
				}
				if err := put(l, fmt.Sprintf("k%d-%d", w, i), "v"); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	eventually(t, "replicas to converge", converged(nodes, "counter", "400", 401))
	if st := l.node.Status(); st.Applied == 0 || st.Commit < st.Applied || st.LastIndex < st.Commit {
		t.Fatalf("leader indexes: commit %d, applied %d, last %d", st.Commit, st.Applied, st.LastIndex)
	}
	for _, tn := range nodes {
		if st := tn.node.Status(); st.LeaderID != l.id || st.LeaderAddr != l.peers[l.id] {
			t.Fatalf("%s reports leader %s at %q, want %s at %q", tn.id, st.LeaderID, st.LeaderAddr, l.id, l.peers[l.id])
		}
		if tn == l {
			continue
		}
		if err := put(tn, "x", "y"); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("write on follower %s: %v", tn.id, err)
		}
	}
}

func TestCatchUpAndFailover(t *testing.T) {
	eachMode(t, testCatchUpAndFailover)
}

func testCatchUpAndFailover(t *testing.T, unsafe bool) {
	nodes := newCluster(t, 3, unsafe)
	l := leader(t, nodes)
	for range 50 {
		must(t, incr(l, "counter"))
	}
	eventually(t, "initial replication", converged(nodes, "counter", "50", 1))
	lagging := follower(nodes, l)
	lagging.stop(t)
	for i := range 100 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
		must(t, incr(l, "counter"))
	}
	lagging.start(t)
	eventually(t, "restarted replica to catch up", converged(nodes, "counter", "150", 101))

	l.stop(t)
	next := leader(t, nodes)
	must(t, incr(next, "counter"))
	must(t, next.node.Flush())
	must(t, put(next, "after", "flush"))
	eventually(t, "flush to replicate", converged(nodes, "after", "flush", 1))
	l.start(t)
	eventually(t, "old leader to rejoin", converged(nodes, "after", "flush", 1))
}

func TestHotKeyFailover(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	var acked, failed atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := incr(l, "counter"); err != nil {
					failed.Add(1)
					return
				}
				acked.Add(1)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	must(t, l.node.Close())
	wg.Wait()
	l.stop(t)
	next := leader(t, nodes)
	var got int64
	eventually(t, "survivors to agree", func() bool {
		v := get(next, "counter")
		for _, tn := range nodes {
			if tn.node != nil && get(tn, "counter") != v {
				return false
			}
		}
		got, _ = strconv.ParseInt(v, 10, 64)
		return true
	})
	if got < acked.Load() || got > acked.Load()+failed.Load() {
		t.Fatalf("counter = %d, acked %d, failed %d", got, acked.Load(), failed.Load())
	}
	must(t, incr(next, "counter"))
	l.start(t)
	eventually(t, "old leader to rejoin", converged(nodes, "counter", strconv.FormatInt(got+1, 10), 1))
}

func TestApplyKeepsLogOrder(t *testing.T) {
	dir := t.TempDir()
	db, err := bitcask.Open(filepath.Join(dir, "data"), bitcask.DefaultOptions())
	must(t, err)
	defer db.Close()
	f, err := newBitcaskFSM(db, filepath.Join(dir, "raft"))
	must(t, err)
	set := func(k, v string) bitcask.Op { return bitcask.Op{Key: k, Value: []byte(v)} }
	must(t, f.Apply([]raft.Entry{
		entry(1, kindOps, set("a", "1"), set("b", "1"), set("x", "1")),
		{Index: 2, Term: 1, Type: raft.EntryNoop},
		entry(3, kindOps, set("a", "2"), bitcask.Op{Key: "b", Delete: true}),
		entry(4, kindOps, set("b", "3")),
		entry(5, kindFlush),
		entry(6, kindOps, set("a", "4"), bitcask.Op{Key: "h", Value: []byte("fields"), Kind: 3},
			bitcask.Op{Key: "t", Value: []byte("gen00000"), Kind: bitcask.Table | 4}, bitcask.Op{Key: "t", Member: "m", IsMember: true, Value: []byte("member")}),
		{Index: 7, Term: 1, Type: raft.EntryConfChange, Data: []byte{9}},
		entry(8, kindOps, set("b", "5"), bitcask.Op{Key: "a", Delete: true}),
	}))
	tn := &testNode{db: db}
	for k, want := range map[string]string{"a": "", "b": "5", "x": ""} {
		if got := get(tn, k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
	must(t, db.View(bitcask.Keys("h", "t"), func(tx *bitcask.Tx) error {
		if v, kind, ok, err := tx.GetKind("h"); string(v) != "fields" || kind != 3 || !ok || err != nil {
			t.Fatalf("typed value = %q, %d, %v, %v", v, kind, ok, err)
		}
		if v, ok, err := tx.GetMember("t", "m"); string(v) != "member" || !ok || err != nil {
			t.Fatalf("member = %q, %v, %v", v, ok, err)
		}
		return nil
	}))
	if f.applied.Load() != 8 {
		t.Fatalf("applied %d, want 8", f.applied.Load())
	}
	if err := f.Apply([]raft.Entry{{Index: 9, Term: 1, Data: []byte{9}}}); !errors.Is(err, errEntry) {
		t.Fatalf("malformed entry: %v", err)
	}
}

func TestRefusesHashicorpRaftDir(t *testing.T) {
	for _, name := range []string{"raft.db", filepath.Join("wal", "wal-meta.db")} {
		dir := t.TempDir()
		db, err := bitcask.Open(filepath.Join(dir, "data"), bitcask.DefaultOptions())
		must(t, err)
		raftDir := filepath.Join(dir, "raft")
		must(t, os.MkdirAll(filepath.Join(raftDir, "wal"), 0o755))
		must(t, os.WriteFile(filepath.Join(raftDir, name), nil, 0o644))
		if _, err := Open(db, Config{ID: "n0", Peers: map[string]string{"n0": freeAddr(t)}, Dir: raftDir}); !errors.Is(err, ErrOldRaftLog) {
			t.Fatalf("open over %s: %v, want ErrOldRaftLog", name, err)
		}
		must(t, db.Close())
	}
}

func TestRejectsShortElectionTimeout(t *testing.T) {
	db, err := bitcask.Open(filepath.Join(t.TempDir(), "data"), bitcask.DefaultOptions())
	must(t, err)
	defer db.Close()
	cfg := Config{ID: "n0", Peers: map[string]string{"n0": freeAddr(t)}, Dir: filepath.Join(t.TempDir(), "raft"), ElectionTimeout: 50 * time.Millisecond}
	if _, err := Open(db, cfg); err == nil || !strings.Contains(err.Error(), "at least 100ms") {
		t.Fatalf("open with a 50ms election timeout: %v", err)
	}
}

func TestInterruptedRestoreResumes(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	keys := make([]string, 60)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		must(t, put(l, keys[i], "v"))
	}
	eventually(t, "replication", converged(nodes, "k59", "v", len(keys)))
	f := follower(nodes, l)
	f.stop(t)
	for i := range 40 {
		must(t, put(l, "late"+strconv.Itoa(i), "v"))
	}
	src := t.TempDir()
	snap, err := l.node.fsm.Snapshot(src)
	must(t, err)
	snap.Term = l.node.Status().Term
	snap.Conf = voters(f)
	raftDir := filepath.Join(f.dir, "raft")
	copyDir(t, src, transport.IncomingDir(filepath.Join(raftDir, "snap"), snap))
	log, err := raftwal.Open(filepath.Join(raftDir, "wal"), snap.Conf, raftwal.Options{})
	must(t, err)
	if cur, _ := log.Snapshot(); cur.Index >= snap.Index {
		t.Fatalf("follower snapshot %d is not behind the leader's %d", cur.Index, snap.Index)
	}
	must(t, log.SetRestoring(&snap))
	must(t, log.Close())
	db, err := bitcask.Open(filepath.Join(f.dir, "data"), bitcask.DefaultOptions())
	must(t, err)
	must(t, db.Update(bitcask.Keys(keys[:30]...), func(tx *bitcask.Tx) error {
		for _, k := range keys[:30] {
			tx.Delete(k)
		}
		return nil
	}))
	must(t, db.Close())
	f.start(t)
	eventually(t, "interrupted restore to resume", converged(nodes, "late39", "v", len(keys)+40))
	f.stop(t)
	log, err = raftwal.Open(filepath.Join(raftDir, "wal"), snap.Conf, raftwal.Options{})
	must(t, err)
	defer log.Close()
	if log.Restoring() != nil {
		t.Fatal("restore marker left behind")
	}
}

func TestCompactsWithoutSnapshots(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	for i := range 100 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
	}
	compact(t, nodes, l, "fill")
	must(t, put(l, "last", "v"))
	eventually(t, "replication", converged(nodes, "last", "v", 121))
	for _, tn := range nodes {
		eventually(t, tn.id+" to compact its log", func() bool { return tn.node.Status().FirstIndex >= 80 }, clusterState(nodes))
		if _, _, ok := latestSnapshot(tn); ok {
			t.Fatalf("%s took a snapshot while every follower kept up", tn.id)
		}
	}
}

func TestRestartReplaysOnlyTail(t *testing.T) {
	nodes := newCluster(t, 3, false, func(tn *testNode) {
		tn.tune = func(c *node.Config) { c.CompactEntries = 1 << 16 }
	})
	l := leader(t, nodes)
	for i := range 50 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
	}
	eventually(t, "replication", converged(nodes, "k49", "v", 50))
	f := follower(nodes, l)
	applied := f.node.fsm.applied.Load()
	f.stop(t)
	db, err := bitcask.Open(filepath.Join(f.dir, "data"), bitcask.DefaultOptions())
	must(t, err)
	durable := db.DurableIndex()
	must(t, db.Close())
	if durable != applied {
		t.Fatalf("durable index after a clean stop is %d, applied %d", durable, applied)
	}
	for i := range 10 {
		must(t, put(l, "late"+strconv.Itoa(i), "v"))
	}
	f.start(t)
	eventually(t, "restarted follower to catch up", converged(nodes, "late9", "v", 60))
	if first := f.node.fsm.first.Load(); first != durable+1 {
		t.Fatalf("the restarted follower applied from %d, want %d", first, durable+1)
	}
}

func TestPublishReachesEveryNode(t *testing.T) {
	nodes := newCluster(t, 3, false)
	var mu sync.Mutex
	delivered := map[string][]string{}
	for i, tn := range nodes {
		tn.node.WatchPublish(func(channel, message []byte, shard bool) int {
			mu.Lock()
			defer mu.Unlock()
			delivered[tn.id] = append(delivered[tn.id], fmt.Sprintf("%s=%s shard=%v", channel, message, shard))
			return i + 10
		})
	}
	l := leader(t, nodes)
	if _, err := follower(nodes, l).node.Publish([]byte("c"), []byte("m"), false); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Publish on a follower: %v, want ErrNotLeader", err)
	}
	n, err := l.node.Publish([]byte("c"), []byte("m"), true)
	must(t, err)
	if want := slices.Index(nodes, l) + 10; n != want {
		t.Fatalf("Publish counted %d receivers, want the leader's %d", n, want)
	}
	eventually(t, "every node to deliver the message", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, tn := range nodes {
			if !slices.Equal(delivered[tn.id], []string{"c=m shard=true"}) {
				return false
			}
		}
		return true
	})
}

func TestSystemStateReplicates(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	f := follower(nodes, l)
	if err := f.node.SetSystem([]byte("v1")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("SetSystem on a follower: %v, want ErrNotLeader", err)
	}
	must(t, l.node.SetSystem([]byte("v1")))
	eventually(t, "every node to store v1", func() bool {
		for _, tn := range nodes {
			if string(tn.db.System()) != "v1" {
				return false
			}
		}
		return true
	})
	stopFollower(t, nodes, l, f)
	must(t, l.node.SetSystem([]byte("v2")))
	for i := range 60 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
	}
	compact(t, nodes, l, "fill")
	f.start(t)
	eventually(t, "the lagging follower to install a snapshot", func() bool {
		_, _, ok := latestSnapshot(f)
		return ok
	}, clusterState(nodes))
	eventually(t, "the lagging follower to store v2", func() bool { return string(f.db.System()) == "v2" }, clusterState(nodes))
}

func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	f := follower(nodes, l)
	stopFollower(t, nodes, l, f)
	for i := range 60 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
	}
	compact(t, nodes, l, "fill")
	f.start(t)
	eventually(t, "lagging follower to catch up", converged(nodes, "fill19", "v", 81), clusterState(nodes))
	eventually(t, "the lagging follower to install a snapshot", func() bool {
		_, _, ok := latestSnapshot(f)
		return ok
	}, clusterState(nodes))
}
