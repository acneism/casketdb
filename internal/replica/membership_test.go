package replica

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

func joinNode(t *testing.T, id string, reach ...*testNode) *testNode {
	t.Helper()
	peers := map[string]string{id: freeAddr(t)}
	for _, tn := range reach {
		peers[tn.id] = tn.peers[tn.id]
	}
	tn := &testNode{id: id, dir: t.TempDir(), peers: peers, join: true}
	tn.start(t)
	t.Cleanup(func() { tn.stop(t) })
	return tn
}

func voterIDs(members []Member) []string {
	var ids []string
	for _, m := range members {
		if m.Voter {
			ids = append(ids, m.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

func TestJoinPromoteAndRemove(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	for i := range 60 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
	}
	compact(t, nodes, l, "fill")
	fresh := joinNode(t, "n3", nodes...)
	all := append(slices.Clone(nodes), fresh)

	must(t, l.node.AddLearner(fresh.id, fresh.peers[fresh.id]))
	eventually(t, "the learner to catch up", converged(all, "fill19", "v", 80))
	eventually(t, "the learner to install a snapshot", func() bool {
		_, _, ok := latestSnapshot(fresh)
		return ok
	})
	if st := fresh.node.Status(); st.Membership != "learner" || st.Voters != 3 || st.Learners != 1 {
		t.Fatalf("learner status %+v", st)
	}
	if err := put(fresh, "x", "y"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("write on a learner: %v, want ErrNotLeader", err)
	}

	must(t, l.node.Promote(fresh.id))
	if got := voterIDs(l.node.Members()); !slices.Equal(got, []string{"n0", "n1", "n2", "n3"}) {
		t.Fatalf("voters after promotion: %v", got)
	}
	eventually(t, "the new voter to learn the configuration", func() bool {
		st := fresh.node.Status()
		return st.Membership == "voter" && st.Voters == 4 && st.LeaderID == l.id && st.LeaderAddr == l.peers[l.id]
	})
	must(t, put(l, "after", "promote"))
	eventually(t, "replication to the new voter", converged(all, "after", "promote", 81))

	fresh.stop(t)
	fresh.start(t)
	must(t, put(l, "after", "restart"))
	eventually(t, "the restarted voter to catch up", converged(all, "after", "restart", 81))

	next := handOver(t, all, l, fresh.id)
	for _, tn := range nodes {
		eventually(t, tn.id+" to report the new leader's address", func() bool {
			st := tn.node.Status()
			return st.LeaderID == fresh.id && st.LeaderAddr == fresh.peers[fresh.id]
		})
	}

	must(t, next.node.Remove(l.id))
	l.stop(t)
	if got := voterIDs(next.node.Members()); slices.Contains(got, l.id) || len(got) != 3 {
		t.Fatalf("voters after removing %s: %v", l.id, got)
	}
	must(t, put(next, "after", "remove"))
	eventually(t, "replication after the removal", converged(all, "after", "remove", 81))
}

func TestLearnerAddedBeforeItStarts(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	must(t, l.node.AddLearner("n3", freeAddr(t)))
	peers := map[string]string{}
	for _, m := range l.node.Members() {
		peers[m.ID] = m.Addr
	}
	fresh := &testNode{id: "n3", dir: t.TempDir(), peers: peers, join: true}
	fresh.start(t)
	t.Cleanup(func() { fresh.stop(t) })
	must(t, put(l, "k", "w"))
	eventually(t, "the learner to catch up", converged(append(nodes, fresh), "k", "w", 1))
}

func TestLearnersJoinOneAfterAnotherBySnapshot(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	for i := range 60 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
	}
	compact(t, nodes, l, "fill")
	all := slices.Clone(nodes)
	for _, id := range []string{"n3", "n4"} {
		fresh := joinNode(t, id, all...)
		all = append(all, fresh)
		must(t, l.node.AddLearner(fresh.id, fresh.peers[fresh.id]))
		eventually(t, id+" to catch up", converged(all, "fill19", "v", 80))
		must(t, l.node.Promote(fresh.id))
	}
}

func TestLeaderRemovesItself(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	must(t, l.node.Remove(l.id))
	var next *testNode
	eventually(t, "a new leader among the remaining voters", func() bool {
		next = leaderOf(nodes)
		return next != nil && next != l
	})
	eventually(t, "the removed node to stop", func() bool { return errors.Is(l.node.Err(), ErrRemoved) })
	if err := put(l, "x", "y"); !errors.Is(err, ErrRemoved) {
		t.Fatalf("write on a removed node: %v, want ErrRemoved", err)
	}
	if err := l.node.ReadBarrier(); !errors.Is(err, ErrRemoved) {
		t.Fatalf("read barrier on a removed node: %v, want ErrRemoved", err)
	}
	if st := l.node.Status(); st.Membership != "removed" {
		t.Fatalf("status of a removed node: %+v", st)
	}
	l.stop(t)
	must(t, put(next, "after", "remove"))
	eventually(t, "replication", converged(nodes, "after", "remove", 2))
	if got := voterIDs(next.node.Members()); slices.Contains(got, l.id) {
		t.Fatalf("voters still include the removed leader: %v", got)
	}
}

func TestMembershipErrors(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	f := follower(nodes, l)
	if err := f.node.AddLearner("n9", freeAddr(t)); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("AddLearner on a follower: %v, want ErrNotLeader", err)
	}
	for name, err := range map[string]error{
		"adding a member again": l.node.AddLearner(f.id, f.peers[f.id]),
		"promoting a stranger":  l.node.Promote("n9"),
		"removing a stranger":   l.node.Remove("n9"),
	} {
		if !errors.Is(err, raft.ErrConfChangeInvalid) {
			t.Fatalf("%s: %v, want raft.ErrConfChangeInvalid", name, err)
		}
	}
	if err := l.node.AddLearner("n9", ""); err == nil || !strings.Contains(err.Error(), "no address") {
		t.Fatalf("AddLearner without an address: %v", err)
	}
	for in, want := range map[error]error{
		raft.ErrConfChangePending:  ErrChangePending,
		node.ErrNotLeader:          ErrNotLeader,
		context.DeadlineExceeded:   ErrChangeUnknown,
		errors.New("node: closed"): ErrChangeUnknown,
	} {
		if got := membershipError(in); !errors.Is(got, want) {
			t.Fatalf("membershipError(%v) = %v, want %v", in, got, want)
		}
	}
}
