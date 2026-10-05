package bitcask

import (
	"cmp"
	"errors"
	"slices"
)

var ErrNotLocked = errors.New("bitcask: key outside of the transaction scope")

type Scope struct {
	keys      []string
	all       bool
	shardwise bool
}

func Keys(keys ...string) Scope {
	return Scope{keys: keys}
}

func All() Scope {
	return Scope{all: true}
}

func Shardwise() Scope {
	return Scope{shardwise: true}
}

type pendingOp struct {
	value    []byte
	expireAt int64
	kind     Kind
	deleted  bool
}

func (op pendingOp) gone(now int64) bool {
	return op.deleted || (op.expireAt != 0 && op.expireAt <= now)
}

type proposedOp struct {
	pendingOp
	term uint64
	id   uint64
}

type Version struct {
	exists bool
	fileID uint32
	offset int64
	churn  uint64
}

type Tx struct {
	db        *DB
	writable  bool
	now       int64
	all       bool
	shardwise bool
	shards    []int
	shardBuf  [4]int
	pending   map[string]pendingOp
	term      uint64
	depends   uint64
	order     []string
	err       error

	pendingMembers map[memberRef]pendingOp
	memberOrder    []memberRef
}

func (db *DB) begin(scope Scope, writable bool) *Tx {
	tx := &Tx{db: db, writable: writable, all: scope.all, shardwise: scope.shardwise && !writable}
	switch {
	case tx.all:
		for i := range db.kd.shards {
			tx.lock(i)
		}
	case tx.shardwise:
	default:
		tx.shards = tx.shardBuf[:0]
		for _, key := range scope.keys {
			tx.shards = append(tx.shards, shardIndex(key))
		}
		slices.Sort(tx.shards)
		tx.shards = slices.Compact(tx.shards)
		for _, i := range tx.shards {
			tx.lock(i)
		}
	}
	tx.now = db.nowMs()
	return tx
}

func (tx *Tx) lock(i int) {
	if tx.writable {
		tx.db.kd.shards[i].mu.Lock()
	} else {
		tx.db.kd.shards[i].mu.RLock()
	}
}

func (tx *Tx) unlock(i int) {
	if tx.writable {
		tx.db.kd.shards[i].mu.Unlock()
	} else {
		tx.db.kd.shards[i].mu.RUnlock()
	}
}

func (tx *Tx) release() {
	if tx.all {
		for i := range tx.db.kd.shards {
			tx.unlock(i)
		}
		return
	}
	for _, i := range tx.shards {
		tx.unlock(i)
	}
}

func (db *DB) View(scope Scope, fn func(tx *Tx) error) error {
	tx := db.begin(scope, false)
	defer tx.release()
	if db.closed.Load() {
		return ErrClosed
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.err
}

func (db *DB) Update(scope Scope, fn func(tx *Tx) error) error {
	waits, err := db.update(scope, fn)
	if err != nil || len(waits) == 0 {
		return err
	}
	return db.await(waits)
}

type Op struct {
	Key      string
	Value    []byte
	ExpireAt int64
	Kind     Kind
	Delete   bool
	Member   string
	IsMember bool
}

func (db *DB) Propose(scope Scope, term uint64, fn func(tx *Tx) error, publish func(ops []Op) (uint64, error)) (uint64, error) {
	tx := db.begin(scope, true)
	defer tx.release()
	tx.term = term
	if err := db.stateErr(); err != nil {
		return 0, err
	}
	if err := fn(tx); err != nil {
		return tx.depends, err
	}
	if tx.err != nil || len(tx.order) == 0 {
		return tx.depends, tx.err
	}
	ops, members := tx.ops()
	id, err := publish(ops)
	if err != nil {
		return tx.depends, err
	}
	for _, key := range tx.order {
		s := db.kd.shard(key)
		if s.proposed == nil {
			s.proposed = make(map[string]proposedOp)
		}
		s.proposed[key] = proposedOp{pendingOp: tx.pending[key], term: term, id: id}
	}
	for _, r := range members {
		s := db.kd.shard(r.key)
		if s.proposedMembers == nil {
			s.proposedMembers = make(map[memberRef]proposedOp)
		}
		s.proposedMembers[r] = proposedOp{pendingOp: tx.pendingMembers[r], term: term, id: id}
	}
	return id, nil
}

func (db *DB) DropProposed() {
	for i := range db.kd.shards {
		s := &db.kd.shards[i]
		s.mu.Lock()
		s.proposed, s.proposedMembers = nil, nil
		s.mu.Unlock()
	}
}

func (db *DB) update(scope Scope, fn func(tx *Tx) error) ([]waitPoint, error) {
	tx := db.begin(scope, true)
	defer tx.release()
	if err := db.stateErr(); err != nil {
		return nil, err
	}
	if err := fn(tx); err != nil {
		return nil, err
	}
	if tx.err != nil {
		return nil, tx.err
	}
	waits, err := tx.commit()
	if fn := db.onWrite.Load(); fn != nil && err == nil {
		for _, key := range tx.order {
			(*fn)(key)
		}
	}
	return waits, err
}

func (db *DB) WatchWrites(fn func(key string)) {
	db.onWrite.Store(&fn)
}

func (db *DB) Apply(ops []Op, upTo uint64) error {
	keys := make([]string, len(ops))
	for i, op := range ops {
		keys[i] = op.Key
	}
	_, err := db.update(Keys(keys...), func(tx *Tx) error {
		for _, op := range ops {
			switch {
			case op.IsMember && op.Delete:
				tx.DeleteMember(op.Key, op.Member)
			case op.IsMember:
				tx.PutMember(op.Key, op.Member, op.Value)
			case op.Delete:
				tx.Delete(op.Key)
			default:
				tx.PutKind(op.Key, op.Kind, op.Value, op.ExpireAt)
			}
		}
		if upTo == 0 {
			return nil
		}
		for _, op := range ops {
			s := db.kd.shard(op.Key)
			if !op.IsMember {
				if p, ok := s.proposed[op.Key]; ok && p.id <= upTo {
					delete(s.proposed, op.Key)
				}
				continue
			}
			gen, _, err := tx.gen(op.Key)
			if err != nil {
				return err
			}
			r := memberRef{op.Key, gen, op.Member}
			if p, ok := s.proposedMembers[r]; ok && p.id <= upTo {
				delete(s.proposedMembers, r)
			}
		}
		return nil
	})
	return err
}

func (db *DB) Dump(fn func(op Op) error) error {
	now := db.nowMs()
	for i := range db.kd.shards {
		s := &db.kd.shards[i]
		g := db.groupOfShard(i)
		var ops []Op
		s.mu.RLock()
		for key, e := range s.m {
			if e.expired(now) {
				continue
			}
			v, kind, err := g.readEntry(s, key, e)
			if err != nil {
				s.mu.RUnlock()
				return err
			}
			ops = append(ops, Op{Key: key, Value: v, ExpireAt: e.expireAt, Kind: kind})
			gen, ok := tableGen(kind, v)
			if t := s.tables[key]; !ok || t == nil || t.gen != gen {
				continue
			}
			for member, me := range s.tables[key].members {
				r := memberRef{key, gen, member}
				mv, err := g.readMember(s, r, me)
				if err != nil {
					s.mu.RUnlock()
					return err
				}
				ops = append(ops, Op{Key: key, Member: member, IsMember: true, Value: mv})
			}
		}
		s.mu.RUnlock()
		for _, op := range ops {
			if err := fn(op); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *Tx) ops() ([]Op, []memberRef) {
	ops := make([]Op, len(tx.order), len(tx.order)+len(tx.memberOrder))
	for i, key := range tx.order {
		p := tx.pending[key]
		ops[i] = Op{Key: key, Value: p.value, ExpireAt: p.expireAt, Kind: p.kind, Delete: p.deleted}
	}
	members := tx.liveMembers()
	for _, r := range members {
		p := tx.pendingMembers[r]
		ops = append(ops, Op{Key: r.key, Member: r.member, IsMember: true, Value: p.value, Delete: p.deleted})
	}
	return ops, members
}

func (tx *Tx) liveMembers() []memberRef {
	if len(tx.memberOrder) == 0 {
		return nil
	}
	gens := make(map[string]uint64)
	var live []memberRef
	for _, r := range tx.memberOrder {
		gen, ok := gens[r.key]
		if !ok {
			gen, _, _ = tx.gen(r.key)
			gens[r.key] = gen
		}
		if gen == r.gen {
			live = append(live, r)
		}
	}
	return live
}

func (tx *Tx) gen(key string) (uint64, bool, error) {
	v, kind, ok, err := tx.GetKind(key)
	if err != nil || !ok {
		return 0, false, err
	}
	gen, ok := tableGen(kind, v)
	return gen, ok, nil
}

func (tx *Tx) GetMember(key, member string) ([]byte, bool, error) {
	gen, ok, err := tx.gen(key)
	if err != nil || !ok {
		return nil, false, err
	}
	i, s := tx.shardFor(key)
	if s == nil {
		return nil, false, ErrNotLocked
	}
	return tx.readMember(i, s, memberRef{key, gen, member})
}

func (tx *Tx) HasMember(key, member string) (bool, error) {
	gen, ok, err := tx.gen(key)
	if err != nil || !ok {
		return false, err
	}
	_, s := tx.shardFor(key)
	if s == nil {
		return false, ErrNotLocked
	}
	_, _, found := tx.lookupMember(s, memberRef{key, gen, member})
	return found, nil
}

func (tx *Tx) lookupMember(s *shard, r memberRef) (pendingOp, entry, bool) {
	if op, ok := tx.pendingMembers[r]; ok {
		return op, entry{}, !op.deleted
	}
	if tx.term != 0 {
		if op, ok := s.proposedMembers[r]; ok && op.term == tx.term {
			tx.depends = max(tx.depends, op.id)
			return op.pendingOp, entry{}, !op.deleted
		}
	}
	e, ok := s.member(r)
	return pendingOp{}, e, ok
}

func (tx *Tx) readMember(i int, s *shard, r memberRef) ([]byte, bool, error) {
	op, e, ok := tx.lookupMember(s, r)
	switch {
	case !ok:
		return nil, false, nil
	case e == entry{}:
		return op.value, true, nil
	}
	v, err := tx.db.groupOfShard(i).readMember(s, r, e)
	return v, err == nil, err
}

func (tx *Tx) PutMember(key, member string, value []byte) {
	if !tx.writable {
		tx.err = ErrReadOnly
		return
	}
	if uint64(len(value)) > maxFieldSize {
		tx.err = ErrTooLarge
		return
	}
	gen, ok, err := tx.gen(key)
	switch {
	case err != nil:
		tx.err = err
	case !ok:
		tx.err = ErrNotTable
	default:
		tx.stageMember(memberRef{key, gen, member}, pendingOp{value: value})
	}
}

func (tx *Tx) DeleteMember(key, member string) bool {
	if !tx.writable {
		tx.err = ErrReadOnly
		return false
	}
	gen, ok, err := tx.gen(key)
	if err != nil || !ok {
		tx.err = cmp.Or(tx.err, err)
		return false
	}
	_, s := tx.shardFor(key)
	if s == nil {
		return false
	}
	r := memberRef{key, gen, member}
	if _, _, found := tx.lookupMember(s, r); !found {
		return false
	}
	tx.stageMember(r, pendingOp{deleted: true})
	return true
}

func (tx *Tx) Members(key string, values bool, fn func(member string, value []byte) bool) error {
	gen, ok, err := tx.gen(key)
	if err != nil || !ok {
		return err
	}
	i, s := tx.shardFor(key)
	if s == nil {
		return ErrNotLocked
	}
	extra := make(map[string]bool)
	for r := range tx.pendingMembers {
		if r.key == key && r.gen == gen {
			extra[r.member] = true
		}
	}
	if tx.term != 0 {
		for r, op := range s.proposedMembers {
			if r.key == key && r.gen == gen && op.term == tx.term {
				extra[r.member] = true
			}
		}
	}
	visit := func(member string) (bool, error) {
		r := memberRef{key, gen, member}
		if !values {
			_, _, found := tx.lookupMember(s, r)
			return !found || fn(member, nil), nil
		}
		v, found, err := tx.readMember(i, s, r)
		if err != nil || !found {
			return err == nil, err
		}
		return fn(member, v), nil
	}
	if t := s.tables[key]; t != nil && t.gen == gen {
		for member := range t.members {
			delete(extra, member)
			if more, err := visit(member); err != nil || !more {
				return err
			}
		}
	}
	for member := range extra {
		if more, err := visit(member); err != nil || !more {
			return err
		}
	}
	return nil
}

func (tx *Tx) orderedMembers(key string) (*orderedView, error) {
	v, kind, ok, err := tx.GetKind(key)
	if err != nil || !ok {
		return nil, err
	}
	gen, ok := tableGen(kind, v)
	if !ok {
		return nil, nil
	}
	byMember := kind&ByMember != 0
	_, s := tx.shardFor(key)
	if s == nil {
		return nil, ErrNotLocked
	}
	changed := make(map[string]pendingOp)
	if tx.term != 0 {
		for r, op := range s.proposedMembers {
			if r.key == key && r.gen == gen && op.term == tx.term {
				changed[r.member] = op.pendingOp
				tx.depends = max(tx.depends, op.id)
			}
		}
	}
	for r, op := range tx.pendingMembers {
		if r.key == key && r.gen == gen {
			changed[r.member] = op
		}
	}
	t := s.tables[key]
	switch {
	case t == nil || t.gen != gen:
		return newOrderedView(newSkiplist(), nil, changed, byMember), nil
	case t.order != nil:
		return newOrderedView(t.order, t.nodes, changed, byMember), nil
	}
	sl := newSkiplist()
	err = tx.Members(key, !byMember, func(member string, value []byte) bool {
		sl.insert(value, member)
		return true
	})
	return newOrderedView(sl, nil, nil, byMember), err
}

func (tx *Tx) MemberCount(key string, below func(value []byte, member string) bool) (int, error) {
	v, err := tx.orderedMembers(key)
	if err != nil || v == nil {
		return 0, err
	}
	return v.count(below), nil
}

func (tx *Tx) MemberRange(key string, from int, reverse bool, fn func(member string, value []byte) bool) error {
	v, err := tx.orderedMembers(key)
	if err != nil || v == nil {
		return err
	}
	v.walk(from, reverse, fn)
	return nil
}

func (tx *Tx) stageMember(r memberRef, op pendingOp) {
	if tx.pendingMembers == nil {
		tx.pendingMembers = make(map[memberRef]pendingOp)
	}
	if _, ok := tx.pendingMembers[r]; !ok {
		tx.memberOrder = append(tx.memberOrder, r)
	}
	tx.pendingMembers[r] = op
}

func (tx *Tx) Now() int64 {
	return tx.now
}

func (tx *Tx) shardFor(key string) (int, *shard) {
	i := shardIndex(key)
	if !tx.all {
		if _, ok := slices.BinarySearch(tx.shards, i); !ok {
			tx.err = ErrNotLocked
			return i, nil
		}
	}
	return i, &tx.db.kd.shards[i]
}

func (tx *Tx) Get(key string) ([]byte, bool, error) {
	v, kind, ok, err := tx.GetKind(key)
	if ok && kind != 0 {
		return nil, false, ErrWrongKind
	}
	return v, ok, err
}

func (tx *Tx) GetKind(key string) ([]byte, Kind, bool, error) {
	if op, ok := tx.pending[key]; ok {
		if op.deleted {
			return nil, 0, false, nil
		}
		return op.value, op.kind, true, nil
	}
	i, s := tx.shardFor(key)
	if s == nil {
		return nil, 0, false, ErrNotLocked
	}
	return tx.read(i, s, key)
}

func (tx *Tx) read(i int, s *shard, key string) ([]byte, Kind, bool, error) {
	if op, ok := tx.proposed(s, key); ok {
		if op.gone(tx.now) {
			return nil, 0, false, nil
		}
		return op.value, op.kind, true, nil
	}
	e, ok := s.m[key]
	if !ok || e.expired(tx.now) {
		return nil, 0, false, nil
	}
	v, kind, err := tx.db.groupOfShard(i).readEntry(s, key, e)
	if err != nil {
		return nil, 0, false, err
	}
	return v, kind, true, nil
}

func (tx *Tx) ExpireAt(key string) (int64, bool) {
	if op, ok := tx.pending[key]; ok {
		if op.deleted {
			return 0, false
		}
		return op.expireAt, true
	}
	_, s := tx.shardFor(key)
	if s == nil {
		return 0, false
	}
	return tx.stored(s, key)
}

func (tx *Tx) stored(s *shard, key string) (int64, bool) {
	if op, ok := tx.proposed(s, key); ok {
		if op.gone(tx.now) {
			return 0, false
		}
		return op.expireAt, true
	}
	e, ok := s.m[key]
	if !ok || e.expired(tx.now) {
		return 0, false
	}
	return e.expireAt, true
}

func (tx *Tx) proposed(s *shard, key string) (proposedOp, bool) {
	if tx.term == 0 {
		return proposedOp{}, false
	}
	op, ok := s.proposed[key]
	if !ok || op.term != tx.term {
		return proposedOp{}, false
	}
	tx.depends = max(tx.depends, op.id)
	return op, true
}

func (tx *Tx) Exists(key string) bool {
	_, ok := tx.ExpireAt(key)
	return ok
}

func (tx *Tx) Version(key string) Version {
	_, s := tx.shardFor(key)
	if s == nil {
		return Version{}
	}
	if op, ok := tx.proposed(s, key); ok {
		return Version{exists: !op.gone(tx.now), offset: int64(op.id)}
	}
	e, ok := s.m[key]
	if !ok || e.expired(tx.now) {
		return Version{churn: s.churn}
	}
	return Version{exists: true, fileID: e.fileID, offset: e.offset}
}

func (tx *Tx) global() bool {
	if tx.all || tx.shardwise {
		return true
	}
	tx.err = ErrNotLocked
	return false
}

func (tx *Tx) readShard(i int, fn func(s *shard)) {
	s := &tx.db.kd.shards[i]
	if tx.shardwise {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	fn(s)
}

func (tx *Tx) Len() int {
	if !tx.global() {
		return 0
	}
	n := 0
	for i := range tx.db.kd.shards {
		tx.readShard(i, func(s *shard) { n += s.count })
	}
	for _, key := range tx.order {
		_, stored := tx.db.kd.shard(key).m[key]
		deleted := tx.pending[key].deleted
		switch {
		case deleted && stored:
			n--
		case !deleted && !stored:
			n++
		}
	}
	return n
}

func (tx *Tx) ofKind(i int, s *shard, key string, kind func(Kind) bool) bool {
	if kind == nil {
		return true
	}
	if op, ok := tx.pending[key]; ok {
		return kind(op.kind)
	}
	_, k, ok, err := tx.read(i, s, key)
	return ok && err == nil && kind(k)
}

func (tx *Tx) Scan(cursor uint64, count int, match func(string) bool, kind func(Kind) bool) (uint64, []string) {
	if !tx.global() {
		return 0, nil
	}
	if count <= 0 {
		count = 10
	}
	var keys []string
	examined := 0
	i := cursor
	for ; i < numShards && examined < count; i++ {
		tx.readShard(int(i), func(s *shard) {
			for key := range s.m {
				examined++
				visible := false
				if op, ok := tx.pending[key]; ok {
					visible = !op.deleted
				} else {
					_, visible = tx.stored(s, key)
				}
				if visible && (match == nil || match(key)) && tx.ofKind(int(i), s, key, kind) {
					keys = append(keys, key)
				}
			}
			for _, key := range tx.order {
				if shardIndex(key) != int(i) || tx.pending[key].deleted {
					continue
				}
				if _, ok := s.m[key]; !ok && (match == nil || match(key)) && tx.ofKind(int(i), s, key, kind) {
					keys = append(keys, key)
				}
			}
		})
	}
	if i >= numShards {
		return 0, keys
	}
	return i, keys
}

func (tx *Tx) Put(key string, value []byte, expireAt int64) {
	tx.PutKind(key, 0, value, expireAt)
}

func (tx *Tx) PutKind(key string, kind Kind, value []byte, expireAt int64) {
	if !tx.writable {
		tx.err = ErrReadOnly
		return
	}
	if uint64(len(key)) > maxFieldSize || uint64(storedSize(kind, value)) > maxFieldSize {
		tx.err = ErrTooLarge
		return
	}
	if _, s := tx.shardFor(key); s == nil {
		return
	}
	if expireAt != 0 && expireAt <= tx.now {
		tx.Delete(key)
		return
	}
	tx.stage(key, pendingOp{value: value, expireAt: expireAt, kind: kind})
}

func (tx *Tx) Delete(key string) bool {
	if !tx.writable {
		tx.err = ErrReadOnly
		return false
	}
	if !tx.Exists(key) {
		return false
	}
	tx.stage(key, pendingOp{deleted: true})
	return true
}

func (tx *Tx) stage(key string, op pendingOp) {
	if tx.pending == nil {
		tx.pending = make(map[string]pendingOp)
	}
	if _, ok := tx.pending[key]; !ok {
		tx.order = append(tx.order, key)
	}
	tx.pending[key] = op
}

type groupBatch struct {
	g             *logGroup
	b             *pendingBatch
	keys          []string
	offsets       []int64
	members       []memberRef
	memberKeys    []string
	memberOffsets []int64
}

func (tx *Tx) commit() ([]waitPoint, error) {
	db := tx.db
	var parts []*groupBatch
	part := func(key string) *groupBatch {
		g := db.groupOfKey(key)
		for _, p := range parts {
			if p.g == g {
				return p
			}
		}
		gb := &groupBatch{g: g}
		parts = append(parts, gb)
		return gb
	}
	for _, key := range tx.order {
		if tx.pending[key].deleted {
			s := db.kd.shard(key)
			e, ok := s.m[key]
			if !ok {
				continue
			}
			if e.expired(tx.now) {
				s.remove(key)
				continue
			}
		}
		gb := part(key)
		gb.keys = append(gb.keys, key)
	}
	for _, r := range tx.liveMembers() {
		if _, stored := db.kd.shard(r.key).member(r); tx.pendingMembers[r].deleted && !stored {
			continue
		}
		gb := part(r.key)
		gb.members = append(gb.members, r)
	}
	if len(parts) == 0 {
		return nil, nil
	}
	cross := len(parts) > 1
	var txid uint64
	if cross {
		txid = db.nextTxID()
	}
	for _, gb := range parts {
		gb.encode(tx, cross, txid, uint32(len(parts)))
	}
	if cross {
		db.txGate.RLock()
		defer db.txGate.RUnlock()
	}
	for _, gb := range parts {
		if err := gb.g.reserve(gb.b); err != nil {
			return nil, err
		}
	}
	waits := make([]waitPoint, len(parts))
	for i, gb := range parts {
		gb.apply(tx)
		waits[i] = waitPoint{g: gb.g, seq: gb.b.seq}
	}
	if !cross {
		return waits, nil
	}
	if err := db.await(waits); err != nil {
		return nil, err
	}
	for i, gb := range parts {
		c := &pendingBatch{buf: appendControl(nil, flagTxCommit, txid)}
		if err := gb.g.reserve(c); err != nil {
			return nil, err
		}
		waits[i] = waitPoint{g: gb.g, seq: c.seq}
	}
	return nil, db.await(waits)
}

func (gb *groupBatch) encode(tx *Tx, cross bool, txid uint64, parts uint32) {
	var size int64
	if cross {
		size += recordSize(0, txHeaderSize)
	}
	for _, key := range gb.keys {
		op := tx.pending[key]
		size += recordSize(len(key), storedSize(op.kind, op.value))
	}
	gb.memberKeys = make([]string, len(gb.members))
	for j, r := range gb.members {
		gb.memberKeys[j] = r.recordKey()
		size += recordSize(len(gb.memberKeys[j]), len(tx.pendingMembers[r].value))
	}
	b := &pendingBatch{buf: make([]byte, 0, size)}
	if cross {
		b.buf = appendTxHeader(b.buf, txid, parts)
	}
	last := len(gb.keys) + len(gb.members) - 1
	gb.offsets = make([]int64, len(gb.keys))
	for i, key := range gb.keys {
		op := tx.pending[key]
		var flags byte
		if op.deleted {
			flags |= flagTombstone
		} else {
			b.refs = append(b.refs, overlayRef{key: key, offset: int64(len(b.buf))})
		}
		if i < last {
			flags |= flagMore
		}
		gb.offsets[i] = int64(len(b.buf))
		b.buf = appendRecord(b.buf, flags, op.expireAt, key, op.kind, op.value)
	}
	gb.memberOffsets = make([]int64, len(gb.members))
	for j, r := range gb.members {
		op := tx.pendingMembers[r]
		flags := flagMember
		if op.deleted {
			flags |= flagTombstone
		} else {
			b.refs = append(b.refs, overlayRef{key: r.key, offset: int64(len(b.buf)), member: &gb.members[j]})
		}
		if len(gb.keys)+j < last {
			flags |= flagMore
		}
		gb.memberOffsets[j] = int64(len(b.buf))
		b.buf = appendRecord(b.buf, flags, 0, gb.memberKeys[j], 0, op.value)
	}
	gb.b = b
}

func (gb *groupBatch) apply(tx *Tx) {
	b := gb.b
	written := b.df.written.Load() >= b.end()
	for i, key := range gb.keys {
		s := tx.db.kd.shard(key)
		op := tx.pending[key]
		if op.deleted {
			s.remove(key)
			continue
		}
		off := b.off + gb.offsets[i]
		stored := storedSize(op.kind, op.value)
		s.set(key, entry{fileID: b.df.id, offset: off, valueSize: uint32(stored), expireAt: op.expireAt})
		if gen, ok := tableGen(op.kind, op.value); ok {
			s.ensureTable(key, gen, op.kind)
		} else {
			s.dropTable(key)
		}
		if written {
			continue
		}
		end := gb.offsets[i] + headerSize + int64(len(key)+stored)
		start := end - int64(len(op.value))
		s.addOverlay(key, overlayEntry{fileID: b.df.id, kind: op.kind, offset: off, value: b.buf[start:end:end]})
		if b.df.written.Load() >= b.end() {
			s.dropOverlay(key, b.df.id, off)
		}
	}
	for j, r := range gb.members {
		s := tx.db.kd.shard(r.key)
		op := tx.pendingMembers[r]
		if op.deleted {
			s.removeMember(r)
			continue
		}
		off := b.off + gb.memberOffsets[j]
		s.setMember(r, entry{fileID: b.df.id, offset: off, valueSize: uint32(len(op.value))}, op.value)
		if written {
			continue
		}
		end := gb.memberOffsets[j] + headerSize + int64(len(gb.memberKeys[j])+len(op.value))
		start := end - int64(len(op.value))
		s.addMemberOverlay(r, overlayEntry{fileID: b.df.id, offset: off, value: b.buf[start:end:end]})
		if b.df.written.Load() >= b.end() {
			s.dropMemberOverlay(r, b.df.id, off)
		}
	}
}
