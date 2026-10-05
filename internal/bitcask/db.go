package bitcask

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCorrupt         = errors.New("bitcask: corrupted data")
	ErrLocked          = errors.New("bitcask: database is locked by another process")
	ErrClosed          = errors.New("bitcask: database is closed")
	ErrMergeInProgress = errors.New("bitcask: merge already in progress")
	ErrReadOnly        = errors.New("bitcask: write in read-only transaction")
	ErrTooLarge        = errors.New("bitcask: key or value too large")
	ErrWrongKind       = errors.New("bitcask: value of another kind")
	ErrNotTable        = errors.New("bitcask: key holds no member table")
)

type Stats struct {
	Keys        int
	KeysWithTTL int
	Logs        int
	DataFiles   int
	TotalBytes  int64
	LiveBytes   int64
	Merges      int64
	Writes      int64
	Fsyncs      int64
	FsyncTime   time.Duration
	ExpiredKeys int64
}

type DB struct {
	dir    string
	opts   Options
	lock   *fileLock
	kd     *keydir
	groups []*logGroup

	txGate sync.RWMutex
	txBase uint64
	txSeq  atomic.Uint64

	failed      atomic.Pointer[error]
	closed      atomic.Bool
	merges      atomic.Int64
	writes      atomic.Int64
	fsyncs      atomic.Int64
	fsyncTime   atomic.Int64
	expiredKeys atomic.Int64
	applied     atomic.Uint64
	syncPolicy  atomic.Int32

	expireCursor int

	sysMu    sync.Mutex
	system   []byte
	onSystem func([]byte)
	onWrite  atomic.Pointer[func(key string)]
	onFlush  atomic.Pointer[func()]

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func (db *DB) nowMs() int64 {
	return db.opts.Now().UnixMilli()
}

func Open(dir string, opts Options) (*DB, error) {
	if opts.MaxFileSize <= 0 {
		opts.MaxFileSize = DefaultOptions().MaxFileSize
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	lock, err := lockFile(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	db := &DB{dir: dir, opts: opts, lock: lock, kd: newKeydir(), stop: make(chan struct{})}
	db.SetSync(opts.Sync)
	if err := db.open(); err != nil {
		for _, g := range db.groups {
			_ = g.closeFiles()
		}
		_ = lock.unlock()
		return nil, err
	}
	db.startBackground()
	return db, nil
}

func (db *DB) open() error {
	dirs, err := db.layout()
	if err != nil {
		return err
	}
	for i, d := range dirs {
		if err := os.MkdirAll(d, dirMode); err != nil {
			return err
		}
		if err := recoverMerge(d); err != nil {
			return err
		}
		db.groups = append(db.groups, newLogGroup(db, i, d))
	}
	if err := recoverFlush(db.dir, dirs); err != nil {
		return err
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return err
	}
	db.txBase = binary.LittleEndian.Uint64(seed[:])
	if err := db.loadSystem(); err != nil {
		return err
	}
	return db.load()
}

func (db *DB) groupOfShard(i int) *logGroup {
	return db.groups[i%len(db.groups)]
}

func (db *DB) groupOfKey(key string) *logGroup {
	return db.groupOfShard(shardIndex(key))
}

func (db *DB) nextTxID() uint64 {
	return db.txBase + db.txSeq.Add(1)
}

func (db *DB) lockAll() {
	for i := range db.kd.shards {
		db.kd.shards[i].mu.Lock()
	}
}

func (db *DB) unlockAll() {
	for i := range db.kd.shards {
		db.kd.shards[i].mu.Unlock()
	}
}

func (db *DB) lockGroups() {
	for _, g := range db.groups {
		g.mergeMu.Lock()
	}
	db.lockAll()
	for _, g := range db.groups {
		g.logMu.Lock()
	}
}

func (db *DB) unlockGroups() {
	for _, g := range db.groups {
		g.logMu.Unlock()
	}
	db.unlockAll()
	for _, g := range db.groups {
		g.mergeMu.Unlock()
	}
}

func (db *DB) Close() error {
	db.stopOnce.Do(func() { close(db.stop) })
	db.wg.Wait()
	_ = db.reserveMarks()
	db.lockGroups()
	defer db.unlockGroups()
	if db.closed.Load() {
		return nil
	}
	db.closed.Store(true)
	var err error
	if db.failure() == nil {
		for _, g := range db.groups {
			if err = g.waitWritten(g.seq); err != nil {
				break
			}
			if err = g.active.f.Sync(); err != nil {
				err = db.fail(err)
				break
			}
		}
	}
	for _, g := range db.groups {
		if cerr := g.closeFiles(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if lerr := db.lock.unlock(); lerr != nil && err == nil {
		err = lerr
	}
	return err
}

func (db *DB) Options() Options {
	o := db.opts
	o.Sync = db.policy()
	return o
}

func (db *DB) SetSync(p SyncPolicy) {
	db.syncPolicy.Store(int32(p))
}

func (db *DB) policy() SyncPolicy {
	return SyncPolicy(db.syncPolicy.Load())
}

func (db *DB) Len() int {
	n, _, _ := db.kd.totals()
	return n
}

func (db *DB) Stats() Stats {
	keys, ttl, live := db.kd.totals()
	st := Stats{
		Keys:        keys,
		KeysWithTTL: ttl,
		Logs:        len(db.groups),
		LiveBytes:   live,
		Merges:      db.merges.Load(),
		Writes:      db.writes.Load(),
		Fsyncs:      db.fsyncs.Load(),
		FsyncTime:   time.Duration(db.fsyncTime.Load()),
		ExpiredKeys: db.expiredKeys.Load(),
	}
	for _, g := range db.groups {
		st.DataFiles += g.dataFiles()
		st.TotalBytes += g.totalBytes.Load()
	}
	return st
}

func (db *DB) Scan(cursor uint64, count int, match func(string) bool) (uint64, []string) {
	var next uint64
	var keys []string
	_ = db.View(Shardwise(), func(tx *Tx) error {
		next, keys = tx.Scan(cursor, count, match, nil)
		return nil
	})
	return next, keys
}

func (db *DB) Keys(match func(string) bool) []string {
	_, keys := db.Scan(0, math.MaxInt, match)
	return keys
}

func (db *DB) MarkApplied(index uint64) {
	db.applied.Store(index)
}

func (db *DB) reserveMarks() error {
	index := db.applied.Load()
	if index == 0 {
		return nil
	}
	for _, g := range db.groups {
		g.logMu.Lock()
		err := db.stateErr()
		if err == nil && index > g.mark {
			_, err = g.reserveMarkLocked(index)
		}
		g.logMu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) DurableIndex() uint64 {
	d := uint64(math.MaxUint64)
	for _, g := range db.groups {
		d = min(d, g.durableMark())
	}
	return d
}

func (db *DB) writeQueued() {
	if db.reserveMarks() != nil {
		return
	}
	for _, g := range db.groups {
		g.logMu.Lock()
		seq := g.seq
		g.logMu.Unlock()
		_ = g.waitWritten(seq)
	}
}

func (db *DB) Flush() error {
	db.lockGroups()
	defer db.unlockGroups()
	if err := db.stateErr(); err != nil {
		return err
	}
	bounds := make([]uint32, len(db.groups))
	old := make([][]uint32, len(db.groups))
	for i, g := range db.groups {
		old[i] = g.fileIDs()
		bounds[i] = g.active.id
		if err := g.rotateLocked(g.active.id + 1); err != nil {
			return err
		}
		if err := g.carryMarkLocked(); err != nil {
			return err
		}
	}
	if err := writeFlushMarker(db.dir, bounds); err != nil {
		return err
	}
	db.kd.reset()
	removed := true
	for i, g := range db.groups {
		g.filesMu.Lock()
		for _, id := range old[i] {
			g.dropFileLocked(id)
		}
		g.filesMu.Unlock()
		for _, id := range old[i] {
			if removeFiles(g.dir, id) != nil {
				removed = false
			}
		}
	}
	if removed {
		if err := os.Remove(filepath.Join(db.dir, flushName)); err == nil {
			_ = syncDir(db.dir)
		}
	}
	if fn := db.onFlush.Load(); fn != nil {
		(*fn)()
	}
	return nil
}

func (db *DB) WatchFlush(fn func()) {
	db.onFlush.Store(&fn)
}

type SnapshotFile struct {
	Path string
	Size int64
}

func (db *DB) LinkFiles(dir string) (int, []SnapshotFile, error) {
	if err := db.Sync(); err != nil {
		return 0, nil, err
	}
	db.txGate.Lock()
	defer db.txGate.Unlock()
	var files []SnapshotFile
	add := func(src string, size int64) error {
		rel, err := filepath.Rel(db.dir, src)
		if err != nil {
			return err
		}
		files = append(files, SnapshotFile{Path: filepath.ToSlash(rel), Size: size})
		return linkOrCopy(src, filepath.Join(dir, rel), size)
	}
	for _, g := range db.groups {
		g.filesMu.RLock()
		defer g.filesMu.RUnlock()
		for _, id := range g.sortedIDsLocked() {
			size := g.files[id].written.Load()
			if size == 0 {
				continue
			}
			if err := add(filepath.Join(g.dir, fileName(id, dataExt)), size); err != nil {
				return 0, nil, err
			}
			hint := filepath.Join(g.dir, fileName(id, hintExt))
			if st, err := os.Stat(hint); err == nil {
				if err := add(hint, st.Size()); err != nil {
					return 0, nil, err
				}
			}
		}
	}
	if err := db.linkSystem(add); err != nil {
		return 0, nil, err
	}
	return len(db.groups), files, nil
}

func linkOrCopy(src, dst string, size int64) error {
	if err := os.MkdirAll(filepath.Dir(dst), dirMode); err != nil {
		return err
	}
	if os.Link(src, dst) == nil {
		return nil
	}
	return CopyPrefix(src, dst, size)
}

func CopyPrefix(src, dst string, size int64) error {
	if err := os.MkdirAll(filepath.Dir(dst), dirMode); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	_, err = io.CopyN(out, in, size)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}

func (db *DB) Merge() error {
	busy := false
	for _, g := range db.groups {
		err := g.merge()
		if errors.Is(err, ErrMergeInProgress) {
			busy = true
			continue
		}
		if err != nil {
			return err
		}
	}
	if busy {
		return ErrMergeInProgress
	}
	return nil
}

func (db *DB) startBackground() {
	ticks := 0
	db.every(time.Second, func() {
		ticks++
		if db.policy() == SyncNo && ticks%noSyncInterval != 0 {
			db.writeQueued()
			return
		}
		_ = db.Sync()
	})
	if db.opts.ExpireInterval > 0 {
		db.every(db.opts.ExpireInterval, db.expireCycle)
	}
	if db.opts.MergeInterval > 0 {
		db.every(db.opts.MergeInterval, db.maybeMerge)
	}
}

func (db *DB) every(interval time.Duration, fn func()) {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-db.stop:
				return
			case <-t.C:
				fn()
			}
		}
	}()
}

func (db *DB) maybeMerge() {
	live := make([]int64, len(db.groups))
	for i := range db.kd.shards {
		s := &db.kd.shards[i]
		s.mu.RLock()
		live[i%len(db.groups)] += s.live
		s.mu.RUnlock()
	}
	minBytes := db.opts.MergeMinBytes / int64(len(db.groups))
	for i, g := range db.groups {
		total := g.totalBytes.Load()
		if total == 0 || total < minBytes || total <= live[i] {
			continue
		}
		if float64(total-live[i])/float64(total) < db.opts.MergeRatio {
			continue
		}
		_ = g.merge()
	}
}
