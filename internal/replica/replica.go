package replica

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

var (
	ErrNotLeader      = errors.New("replica: not the leader")
	ErrLeadershipLost = errors.New("replica: write interrupted by a leadership change, it may or may not be applied")
	ErrNotEmpty       = errors.New("replica: database has data but no raft state, start the node from an empty directory")
	ErrOldRaftLog     = errors.New("replica: the raft directory was written by hashicorp/raft (CasketDB v0.9 or older), start the node from an empty directory")
	ErrUnconfirmed    = errors.New("replica: no leader confirmed the read, retry")
	ErrTransferFailed = errors.New("replica: leadership transfer failed")
	ErrChangePending  = errors.New("replica: another membership change is in progress, retry")
	ErrChangeUnknown  = errors.New("replica: the membership change was interrupted and may or may not be applied, check RAFT MEMBERS")
)

type ReadMode int

const (
	ReadLocal ReadMode = iota
	ReadLinearizable
	ReadLease
)

func ParseReadMode(s string) (ReadMode, error) {
	switch s {
	case "local":
		return ReadLocal, nil
	case "linearizable":
		return ReadLinearizable, nil
	case "lease":
		return ReadLease, nil
	}
	return 0, fmt.Errorf("replica: unknown read mode %q, want local, linearizable or lease", s)
}

type Config struct {
	ID              string
	Peers           map[string]string
	Listen          string
	Dir             string
	Logger          *slog.Logger
	UnsafeNoFsync   bool
	TLS             *tls.Config
	Reads           ReadMode
	MaxClockDrift   float64
	Join            bool
	ElectionTimeout time.Duration
}

type Status struct {
	State      string
	Term       uint64
	Commit     uint64
	Applied    uint64
	LastIndex  uint64
	LeaderID   string
	LeaderAddr string
	Membership string
	Voters     int
	Learners   int
}

type Member struct {
	ID    string
	Addr  string
	Voter bool
}

type Node struct {
	db      *bitcask.DB
	id      raft.NodeID
	rn      *node.Node
	fsm     *bitcaskFSM
	flushMu sync.RWMutex
	ready   atomic.Uint64
	onEvent atomic.Pointer[func()]

	reads    ReadMode
	election time.Duration

	wg sync.WaitGroup
}

func ParsePeers(s string) (map[string]string, error) {
	peers := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("replica: bad peer %q, want id=host:port", part)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("replica: duplicate peer id %q", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

func Open(db *bitcask.DB, cfg Config) (*Node, error) {
	return open(db, cfg, nil)
}

func open(db *bitcask.DB, cfg Config, tune func(*node.Config)) (*Node, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("replica: node %q is not in the peer list", cfg.ID)
	}
	const tick = 10 * time.Millisecond
	electionTicks := 100
	if cfg.ElectionTimeout > 0 {
		if electionTicks = int(cfg.ElectionTimeout / tick); electionTicks < 10 {
			return nil, fmt.Errorf("replica: the election timeout must be at least %v", 10*tick)
		}
	}
	if fileExists(filepath.Join(cfg.Dir, "raft.db")) || fileExists(filepath.Join(cfg.Dir, "wal", "wal-meta.db")) {
		return nil, ErrOldRaftLog
	}
	if !fileExists(filepath.Join(cfg.Dir, "wal", "meta")) && db.Len() > 0 {
		return nil, ErrNotEmpty
	}
	fsm, err := newBitcaskFSM(db, cfg.Dir)
	if err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	peers := make(map[raft.NodeID]string, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		peers[raft.NodeID(id)] = addr
	}
	nc := node.Config{
		ID:              raft.NodeID(cfg.ID),
		Dir:             cfg.Dir,
		Listen:          cfg.Listen,
		Peers:           peers,
		Join:            cfg.Join,
		TLS:             cfg.TLS,
		StateMachine:    fsm,
		TickInterval:    tick,
		ElectionTicks:   electionTicks,
		HeartbeatTicks:  electionTicks / 10,
		PreVote:         true,
		CheckQuorum:     true,
		LeaseReads:      cfg.Reads == ReadLease,
		MaxClockDrift:   cfg.MaxClockDrift,
		NoSync:          cfg.UnsafeNoFsync,
		CompactEntries:  1 << 16,
		TrailingEntries: 1 << 16,
		Logger:          logger.With("component", "raft"),
	}
	if tune != nil {
		tune(&nc)
	}
	rn, err := node.Open(nc)
	if err != nil {
		return nil, err
	}
	n := &Node{db: db, id: nc.ID, rn: rn, fsm: fsm, reads: cfg.Reads, election: time.Duration(nc.ElectionTicks) * nc.TickInterval}
	n.wg.Add(1)
	go n.watch()
	return n, nil
}

func (n *Node) watch() {
	defer n.wg.Done()
	for e := range n.rn.Events() {
		n.ready.Store(0)
		n.db.DropProposed()
		if e.Ready && e.Leader == n.id {
			n.ready.Store(e.Term)
		}
		if fn := n.onEvent.Load(); fn != nil {
			(*fn)()
		}
	}
}

func (n *Node) WatchLeadership(fn func()) {
	n.onEvent.Store(&fn)
}

func (n *Node) WatchPublish(fn func(channel, message []byte, shard bool) int) {
	n.fsm.publish.Store(&fn)
}

func (n *Node) Publish(channel, message []byte, shard bool) (int, error) {
	term := n.ready.Load()
	if term == 0 {
		return 0, ErrNotLeader
	}
	nonce := rand.Uint64()
	done := make(chan int, 1)
	n.fsm.waiters.Store(nonce, done)
	defer n.fsm.waiters.Delete(nonce)
	p, err := n.propose(term, encodePublish(nonce, channel, message, shard))
	if err != nil {
		return 0, err
	}
	if err := n.wait(p); err != nil {
		return 0, err
	}
	select {
	case count := <-done:
		return count, nil
	case <-time.After(n.election):
		return 0, ErrLeadershipLost
	}
}

func (n *Node) propose(term uint64, data []byte) (node.Proposal, error) {
	p, err := n.rn.ProposeIn(term, data)
	switch {
	case errors.Is(err, node.ErrNotLeader):
		return p, ErrNotLeader
	case err != nil:
		return p, ErrLeadershipLost
	}
	return p, nil
}

func (n *Node) Update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	n.flushMu.RLock()
	defer n.flushMu.RUnlock()
	term := n.ready.Load()
	if term == 0 {
		return ErrNotLeader
	}
	index, err := n.db.Propose(scope, term, fn, func(ops []bitcask.Op) (uint64, error) {
		p, err := n.propose(term, encodeEntry(kindOps, ops))
		return p.Index, err
	})
	if index == 0 || errors.Is(err, ErrNotLeader) || errors.Is(err, ErrLeadershipLost) {
		return err
	}
	if werr := n.wait(node.Proposal{Index: index, Term: term}); werr != nil {
		return werr
	}
	return err
}

func (n *Node) SetSystem(b []byte) error {
	term := n.ready.Load()
	if term == 0 {
		return ErrNotLeader
	}
	p, err := n.propose(term, append([]byte{kindSystem}, b...))
	if err != nil {
		return err
	}
	return n.wait(p)
}

func (n *Node) Flush() error {
	n.flushMu.Lock()
	defer n.flushMu.Unlock()
	term := n.ready.Load()
	if term == 0 {
		return ErrNotLeader
	}
	p, err := n.propose(term, encodeEntry(kindFlush, nil))
	if err != nil {
		return err
	}
	return n.wait(p)
}

func (n *Node) wait(p node.Proposal) error {
	if err := n.rn.Wait(context.Background(), p); err != nil {
		return ErrLeadershipLost
	}
	return nil
}

func (n *Node) ReadBarrier() error {
	if n.reads == ReadLocal {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*n.election)
	defer cancel()
	index, err := n.rn.ReadIndex(ctx)
	if err == nil {
		err = n.rn.WaitApplied(ctx, index)
	}
	if err != nil {
		return ErrUnconfirmed
	}
	return nil
}

func (n *Node) TransferLeadership(to string) error {
	if n.rn.Status().State != raft.StateLeader {
		return ErrNotLeader
	}
	targets := []raft.NodeID{raft.NodeID(to)}
	if to == "" {
		targets = nil
		for _, id := range n.rn.ConfState().Voters {
			if id != n.id {
				targets = append(targets, id)
			}
		}
	}
	err := ErrTransferFailed
	for _, id := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), 3*n.election)
		err = n.rn.TransferLeadership(ctx, id)
		cancel()
		switch {
		case err == nil:
			return nil
		case errors.Is(err, node.ErrNotLeader):
			return ErrNotLeader
		case errors.Is(err, raft.ErrTransferTarget):
			return fmt.Errorf("replica: %q is not a voter of the cluster", id)
		}
		err = ErrTransferFailed
	}
	return err
}

func (n *Node) Status() Status {
	st := n.rn.Status()
	cs := n.rn.ConfState()
	membership := "none"
	switch {
	case slices.Contains(cs.Voters, n.id):
		membership = "voter"
	case slices.Contains(cs.Learners, n.id):
		membership = "learner"
	}
	return Status{
		State:      st.State.String(),
		Term:       st.Term,
		Commit:     st.Commit,
		Applied:    st.Applied,
		LastIndex:  st.LastIndex,
		LeaderID:   string(st.Lead),
		LeaderAddr: cs.Addrs[st.Lead],
		Membership: membership,
		Voters:     len(cs.Voters),
		Learners:   len(cs.Learners),
	}
}

func (n *Node) Members() []Member {
	cs := n.rn.ConfState()
	var out []Member
	for _, id := range cs.Voters {
		out = append(out, Member{ID: string(id), Addr: cs.Addrs[id], Voter: true})
	}
	for _, id := range cs.Learners {
		out = append(out, Member{ID: string(id), Addr: cs.Addrs[id]})
	}
	return out
}

func (n *Node) AddLearner(id, addr string) error {
	return n.changeMembers(10*n.election, func(ctx context.Context) error {
		return n.rn.AddLearner(ctx, raft.NodeID(id), addr)
	})
}

func (n *Node) Promote(id string) error {
	return n.changeMembers(10*time.Minute, func(ctx context.Context) error {
		return n.rn.Promote(ctx, raft.NodeID(id))
	})
}

func (n *Node) Remove(id string) error {
	return n.changeMembers(10*n.election, func(ctx context.Context) error {
		return n.rn.Remove(ctx, raft.NodeID(id))
	})
}

func (n *Node) changeMembers(timeout time.Duration, change func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return membershipError(change(ctx))
}

func membershipError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, node.ErrNotLeader):
		return ErrNotLeader
	case errors.Is(err, raft.ErrConfChangePending):
		return ErrChangePending
	case errors.Is(err, raft.ErrConfChangeInvalid):
		return err
	}
	return ErrChangeUnknown
}

func (n *Node) Close() error {
	err := n.rn.Close()
	n.wg.Wait()
	return err
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
