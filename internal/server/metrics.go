package server

import (
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"time"
)

var latencyBounds = [...]time.Duration{
	10 * time.Microsecond, 25 * time.Microsecond, 50 * time.Microsecond,
	100 * time.Microsecond, 250 * time.Microsecond, 500 * time.Microsecond,
	time.Millisecond, 2500 * time.Microsecond, 5 * time.Millisecond,
	10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2500 * time.Millisecond,
}

var epoch = time.Now()

func (s *Server) clock() time.Duration {
	if !s.cfg.TrackLatency {
		return 0
	}
	return time.Since(epoch)
}

func (s *Server) observe(start time.Duration, n int) {
	if s.cfg.TrackLatency {
		s.latency.observe(time.Since(epoch)-start, n)
	}
}

type histogram struct {
	counts [len(latencyBounds) + 1]atomic.Uint64
	sum    atomic.Int64
}

func (h *histogram) observe(d time.Duration, n int) {
	i, _ := slices.BinarySearch(latencyBounds[:], d)
	h.counts[i].Add(uint64(n))
	h.sum.Add(int64(d) * int64(n))
}

func (h *histogram) write(w io.Writer, name string) {
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	var total uint64
	for i, b := range latencyBounds {
		total += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b.Seconds(), total)
	}
	total += h.counts[len(latencyBounds)].Load()
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", name, total, name, time.Duration(h.sum.Load()).Seconds(), name, total)
}

func (s *Server) WriteMetrics(w io.Writer) {
	metric := func(kind, name string, v any) {
		fmt.Fprintf(w, "# TYPE %s %s\n%s %v\n", name, kind, name, v)
	}
	fmt.Fprintf(w, "# TYPE casketdb_build_info gauge\ncasketdb_build_info{version=%q} 1\n", Version)
	metric("gauge", "casketdb_connected_clients", s.clientCount())
	metric("counter", "casketdb_connections_received_total", s.connections.Load())
	metric("counter", "casketdb_rejected_connections_total", s.rejected.Load())
	metric("counter", "casketdb_commands_processed_total", s.processed.Load())
	s.latency.write(w, "casketdb_command_duration_seconds")
	st := s.db.Stats()
	metric("gauge", "casketdb_keys", st.Keys)
	metric("gauge", "casketdb_keys_with_ttl", st.KeysWithTTL)
	metric("counter", "casketdb_expired_keys_total", st.ExpiredKeys)
	metric("gauge", "casketdb_bitcask_logs", st.Logs)
	metric("gauge", "casketdb_bitcask_data_files", st.DataFiles)
	metric("gauge", "casketdb_bitcask_total_bytes", st.TotalBytes)
	metric("gauge", "casketdb_bitcask_live_bytes", st.LiveBytes)
	metric("counter", "casketdb_bitcask_merges_total", st.Merges)
	metric("counter", "casketdb_bitcask_writes_total", st.Writes)
	metric("counter", "casketdb_bitcask_fsyncs_total", st.Fsyncs)
	metric("counter", "casketdb_bitcask_fsync_seconds_total", st.FsyncTime.Seconds())
	rep := s.cfg.Replica
	if rep == nil {
		return
	}
	rs := rep.Status()
	leader := 0
	if rs.State == "Leader" {
		leader = 1
	}
	metric("gauge", "casketdb_raft_leader", leader)
	metric("gauge", "casketdb_raft_term", rs.Term)
	metric("gauge", "casketdb_raft_commit_index", rs.Commit)
	metric("gauge", "casketdb_raft_applied_index", rs.Applied)
	metric("gauge", "casketdb_raft_first_index", rs.FirstIndex)
	metric("gauge", "casketdb_raft_last_index", rs.LastIndex)
	metric("gauge", "casketdb_raft_snapshot_index", rs.SnapshotIndex)
	metric("gauge", "casketdb_raft_voters", rs.Voters)
	metric("gauge", "casketdb_raft_learners", rs.Learners)
}
