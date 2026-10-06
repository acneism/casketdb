package replica

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
)

func leaderOf(nodes []*testNode) *testNode {
	for _, tn := range nodes {
		if tn.node != nil && tn.node.ready.Load() != 0 {
			return tn
		}
	}
	return nil
}

func handOver(t *testing.T, nodes []*testNode, from *testNode, to string) *testNode {
	t.Helper()
	must(t, from.node.TransferLeadership(to))
	var next *testNode
	eventually(t, "leadership to move", func() bool {
		next = leaderOf(nodes)
		return next != nil && next != from && from.node.ready.Load() == 0
	})
	if to != "" && next.id != to {
		t.Fatalf("leadership went to %s, want %s", next.id, to)
	}
	return next
}

func TestTransferLeadership(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	target := follower(nodes, l)
	next := handOver(t, nodes, l, target.id)
	if err := put(l, "x", "y"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("write on the old leader: %v, want ErrNotLeader", err)
	}
	must(t, put(next, "after", "transfer"))
	eventually(t, "replication", converged(nodes, "after", "transfer", 2))
	handOver(t, nodes, next, "")
}

func TestStatusReportsPeers(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	eventually(t, "the leader to see both followers caught up", func() bool {
		st := l.node.Status()
		caughtUp := 0
		for _, p := range st.Peers {
			if p.Match == st.LastIndex && p.State == "replicate" && p.Active && !p.Learner && p.Addr != "" {
				caughtUp++
			}
		}
		return len(st.Peers) == 2 && caughtUp == 2
	}, clusterState(nodes))
	if st := follower(nodes, l).node.Status(); len(st.Peers) != 0 || st.FirstIndex == 0 {
		t.Fatalf("follower status %+v", st)
	}
}

func TestClusterID(t *testing.T) {
	nodes := newCluster(t, 3, false, func(tn *testNode) { tn.cluster = "c1" })
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	eventually(t, "replication within the cluster", converged(nodes, "k", "v", 1))
	other := &testNode{id: "n3", dir: t.TempDir(), peers: map[string]string{"n3": freeAddr(t)}, join: true, cluster: "c2"}
	other.start(t)
	t.Cleanup(func() { other.stop(t) })
	must(t, l.node.AddLearner(other.id, other.peers[other.id]))
	time.Sleep(time.Second)
	if n := other.db.Len(); n != 0 {
		t.Fatalf("a node of another cluster received %d keys", n)
	}
}

func TestTransferLeadershipErrors(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	f := follower(nodes, l)
	if err := f.node.TransferLeadership(l.id); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("transfer on a follower: %v, want ErrNotLeader", err)
	}
	if err := l.node.TransferLeadership("nobody"); err == nil || !strings.Contains(err.Error(), "not a voter") {
		t.Fatalf("transfer to an unknown node: %v", err)
	}
	f.stop(t)
	if err := l.node.TransferLeadership(f.id); !errors.Is(err, ErrTransferFailed) || !strings.Contains(err.Error(), f.id+" did not") {
		t.Fatalf("transfer to a stopped node: %v, want ErrTransferFailed saying why", err)
	}
	if leaderOf(nodes) != l {
		t.Fatal("the leader lost its leadership after a failed transfer")
	}
	must(t, put(l, "still", "leader"))
}

func TestNoOpWriteWaitsForTheProposalItRead(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	for _, tn := range nodes {
		if tn != l {
			tn.stop(t)
		}
	}
	first := make(chan error, 1)
	go func() {
		first <- l.node.Update(bitcask.Keys("k"), func(tx *bitcask.Tx) error {
			tx.Delete("k")
			return nil
		})
	}()
	eventually(t, "the delete to be proposed", func() bool {
		gone := false
		_, err := l.db.Propose(bitcask.Keys("k"), l.node.ready.Load(), func(tx *bitcask.Tx) error {
			gone = !tx.Exists("k")
			return nil
		}, nil)
		return err == nil && gone
	})
	existed := true
	err := l.node.Update(bitcask.Keys("k"), func(tx *bitcask.Tx) error {
		if existed = tx.Exists("k"); existed {
			tx.Delete("k")
		}
		return nil
	})
	if err == nil && !existed {
		t.Fatal("a write that changed nothing reported the key missing because of a delete that never committed")
	}
	for _, err := range []error{err, <-first} {
		if !errors.Is(err, ErrLeadershipLost) && !errors.Is(err, ErrNotLeader) {
			t.Fatalf("write without a majority: %v, want ErrLeadershipLost or ErrNotLeader", err)
		}
	}
}

func TestWritesSurviveLeadershipTransfers(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	var acked, unknown atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				tn := leaderOf(nodes)
				if tn == nil {
					time.Sleep(time.Millisecond)
					continue
				}
				switch err := incr(tn, "counter"); {
				case err == nil:
					acked.Add(1)
				case errors.Is(err, ErrLeadershipLost):
					unknown.Add(1)
				case !errors.Is(err, ErrNotLeader):
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 6 {
		l = handOver(t, nodes, l, "")
	}
	stop.Store(true)
	wg.Wait()
	var got int64
	eventually(t, "replicas to agree", func() bool {
		v := get(nodes[0], "counter")
		for _, tn := range nodes {
			if get(tn, "counter") != v {
				return false
			}
		}
		got, _ = strconv.ParseInt(v, 10, 64)
		return true
	})
	if got < acked.Load() || got > acked.Load()+unknown.Load() {
		t.Fatalf("counter = %d, acked %d, unknown %d", got, acked.Load(), unknown.Load())
	}
	if acked.Load() == 0 {
		t.Fatal("no write succeeded")
	}
	t.Logf("%d writes acknowledged, %d with unknown outcome across 6 transfers", acked.Load(), unknown.Load())
}
