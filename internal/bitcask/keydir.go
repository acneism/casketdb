package bitcask

import (
	"bytes"
	"sync"
)

const numShards = 1024

type entry struct {
	fileID    uint32
	valueSize uint32
	offset    int64
	expireAt  int64
}

func (e entry) expired(now int64) bool {
	return e.expireAt != 0 && e.expireAt <= now
}

func shardIndex[K string | []byte](key K) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % numShards)
}

type shard struct {
	mu    sync.RWMutex
	m     map[string]entry
	count int
	ttl   int
	live  int64
	churn uint64

	ovMu      sync.Mutex
	overlay   map[string]overlayEntry
	ovMembers map[memberRef]overlayEntry

	proposed        map[string]proposedOp
	proposedMembers map[memberRef]proposedOp

	tables map[string]*table

	_ [64]byte
}

type table struct {
	gen      uint64
	members  map[string]entry
	order    *skiplist
	nodes    map[string]*skipNode
	byMember bool
}

func (r memberRef) size(e entry) int64 {
	n := uint64(len(r.key))
	w := 1
	for ; n >= 0x80; n >>= 7 {
		w++
	}
	return recordSize(w+len(r.key)+8+len(r.member), int(e.valueSize))
}

func (s *shard) member(r memberRef) (entry, bool) {
	t := s.tables[r.key]
	if t == nil || t.gen != r.gen {
		return entry{}, false
	}
	e, ok := t.members[r.member]
	return e, ok
}

func (s *shard) ensureTable(key string, gen uint64, kind Kind) *table {
	if t := s.tables[key]; t != nil && t.gen == gen {
		return t
	}
	s.dropTable(key)
	if s.tables == nil {
		s.tables = make(map[string]*table)
	}
	t := &table{gen: gen, members: make(map[string]entry), byMember: kind&ByMember != 0}
	if kind&Ordered != 0 {
		t.order, t.nodes = newSkiplist(), make(map[string]*skipNode)
	}
	s.tables[key] = t
	return t
}

func (s *shard) setMember(r memberRef, e entry, value []byte) {
	t := s.ensureTable(r.key, r.gen, 0)
	if old, ok := t.members[r.member]; ok {
		s.live -= r.size(old)
	}
	t.members[r.member] = e
	s.live += r.size(e)
	if t.order != nil {
		if t.byMember {
			value = nil
		}
		if n := t.nodes[r.member]; n != nil {
			t.order.delete(n.value, r.member)
		}
		t.nodes[r.member] = t.order.insert(bytes.Clone(value), r.member)
	}
}

func (s *shard) removeMember(r memberRef) {
	e, ok := s.member(r)
	if !ok {
		return
	}
	t := s.tables[r.key]
	delete(t.members, r.member)
	s.live -= r.size(e)
	if n := t.nodes[r.member]; n != nil {
		t.order.delete(n.value, r.member)
		delete(t.nodes, r.member)
	}
}

func (s *shard) dropTable(key string) {
	t := s.tables[key]
	if t == nil {
		return
	}
	for m, e := range t.members {
		s.live -= memberRef{key, t.gen, m}.size(e)
	}
	delete(s.tables, key)
}

func (s *shard) memberAt(r memberRef, fileID uint32, offset int64) bool {
	e, ok := s.member(r)
	return ok && e.fileID == fileID && e.offset == offset
}

func (s *shard) relocateMember(r memberRef, oldFile uint32, oldOffset int64, newFile uint32, newOffset int64) {
	if s.memberAt(r, oldFile, oldOffset) {
		t := s.tables[r.key]
		e := t.members[r.member]
		e.fileID, e.offset = newFile, newOffset
		t.members[r.member] = e
	}
}

func (s *shard) addMemberOverlay(r memberRef, o overlayEntry) {
	s.ovMu.Lock()
	if s.ovMembers == nil {
		s.ovMembers = make(map[memberRef]overlayEntry)
	}
	s.ovMembers[r] = o
	s.ovMu.Unlock()
}

func (s *shard) getMemberOverlay(r memberRef, fileID uint32, offset int64) (overlayEntry, bool) {
	s.ovMu.Lock()
	o, ok := s.ovMembers[r]
	s.ovMu.Unlock()
	if !ok || o.fileID != fileID || o.offset != offset {
		return overlayEntry{}, false
	}
	return o, true
}

func (s *shard) dropMemberOverlay(r memberRef, fileID uint32, offset int64) {
	s.ovMu.Lock()
	if o, ok := s.ovMembers[r]; ok && o.fileID == fileID && o.offset == offset {
		delete(s.ovMembers, r)
	}
	s.ovMu.Unlock()
}

type keydir struct {
	shards [numShards]shard
}

func newKeydir() *keydir {
	kd := &keydir{}
	for i := range kd.shards {
		kd.shards[i].m = make(map[string]entry)
		kd.shards[i].overlay = make(map[string]overlayEntry)
	}
	return kd
}

func (kd *keydir) shard(key string) *shard {
	return &kd.shards[shardIndex(key)]
}

func (kd *keydir) reset() {
	for i := range kd.shards {
		kd.shards[i].reset()
	}
}

func (kd *keydir) totals() (count, ttl int, live int64) {
	for i := range kd.shards {
		s := &kd.shards[i]
		s.mu.RLock()
		count += s.count
		ttl += s.ttl
		live += s.live
		s.mu.RUnlock()
	}
	return count, ttl, live
}

func (s *shard) reset() {
	s.m = make(map[string]entry)
	s.tables = nil
	s.count, s.ttl, s.live = 0, 0, 0
	s.churn++
}

func (s *shard) set(key string, e entry) {
	if old, ok := s.m[key]; ok {
		s.forget(key, old)
	} else {
		s.churn++
	}
	s.m[key] = e
	s.count++
	s.live += recordSize(len(key), int(e.valueSize))
	if e.expireAt != 0 {
		s.ttl++
	}
}

func (s *shard) forget(key string, e entry) {
	s.count--
	s.live -= recordSize(len(key), int(e.valueSize))
	if e.expireAt != 0 {
		s.ttl--
	}
}

func (s *shard) remove(key string) bool {
	old, ok := s.m[key]
	if !ok {
		return false
	}
	delete(s.m, key)
	s.forget(key, old)
	s.dropTable(key)
	s.churn++
	return true
}

func (s *shard) at(key string, fileID uint32, offset int64) (entry, bool) {
	e, ok := s.m[key]
	return e, ok && e.fileID == fileID && e.offset == offset
}

func (s *shard) relocate(key string, oldFile uint32, oldOffset int64, newFile uint32, newOffset int64) {
	if e, ok := s.at(key, oldFile, oldOffset); ok {
		e.fileID, e.offset = newFile, newOffset
		s.m[key] = e
	}
}

func (s *shard) removeAt(key string, fileID uint32, offset int64) {
	if _, ok := s.at(key, fileID, offset); ok {
		s.remove(key)
	}
}

func (s *shard) addOverlay(key string, o overlayEntry) {
	s.ovMu.Lock()
	s.overlay[key] = o
	s.ovMu.Unlock()
}

func (s *shard) getOverlay(key string, fileID uint32, offset int64) (overlayEntry, bool) {
	s.ovMu.Lock()
	o, ok := s.overlay[key]
	s.ovMu.Unlock()
	if !ok || o.fileID != fileID || o.offset != offset {
		return overlayEntry{}, false
	}
	return o, true
}

func (s *shard) dropOverlay(key string, fileID uint32, offset int64) {
	s.ovMu.Lock()
	if o, ok := s.overlay[key]; ok && o.fileID == fileID && o.offset == offset {
		delete(s.overlay, key)
	}
	s.ovMu.Unlock()
}
