package bitcask

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func multiOptions(logs int) Options {
	o := testOptions()
	o.Logs = logs
	return o
}

func keyInLog(logs, log int, prefix string) string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		if shardIndex(key)%logs == log {
			return key
		}
	}
}

func lastFileOfLog(t *testing.T, dir string, log int) string {
	t.Helper()
	ids, err := listIDs(groupDir(dir, log), dataExt)
	if err != nil || len(ids) == 0 {
		t.Fatalf("log %d has no data files: %v", log, err)
	}
	return filepath.Join(groupDir(dir, log), fileName(ids[len(ids)-1], dataExt))
}

func putPair(t *testing.T, db *DB, a, av, b, bv string) {
	t.Helper()
	err := db.Update(Keys(a, b), func(tx *Tx) error {
		tx.Put(a, []byte(av), 0)
		tx.Put(b, []byte(bv), 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCrossLogBatchCommits(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 0, "a"), keyInLog(4, 1, "b")
	db := mustOpen(t, dir, o)
	putPair(t, db, a, "new", b, "new")
	expect(t, db, a, "new")
	expect(t, db, b, "new")
	mustClose(t, db)

	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	expect(t, db, a, "new")
	expect(t, db, b, "new")
	if st := db.Stats(); st.Logs != 4 || st.Keys != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

func cutTail(t *testing.T, path string, n int64) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, max(st.Size()-n, 0)); err != nil {
		t.Fatal(err)
	}
}

var commitSize = recordSize(0, 8)

func openCleanup(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	db := mustOpen(t, dir, o)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCrossLogBatchRollsBackWhenPartIsMissing(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 0, "a"), keyInLog(4, 1, "b")
	db := mustOpen(t, dir, o)
	put(t, db, a, "old", 0)
	put(t, db, b, "old", 0)
	putPair(t, db, a, "new", b, "new")
	mustClose(t, db)

	cutTail(t, lastFileOfLog(t, dir, 0), commitSize)
	cutTail(t, lastFileOfLog(t, dir, 1), commitSize+2)

	db = mustOpen(t, dir, o)
	expect(t, db, a, "old")
	expect(t, db, b, "old")
	if n := db.Len(); n != 2 {
		t.Fatalf("len = %d, want 2", n)
	}
	put(t, db, a, "newer", 0)
	mustClose(t, db)

	db = openCleanup(t, dir, o)
	expect(t, db, a, "newer")
	expect(t, db, b, "old")
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	expect(t, db, a, "newer")
	expect(t, db, b, "old")
}

func TestCrossLogRollbackRemovesCreatedKeys(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 2, "x"), keyInLog(4, 3, "y")
	db := mustOpen(t, dir, o)
	putPair(t, db, a, "1", b, "1")
	mustClose(t, db)

	cutTail(t, lastFileOfLog(t, dir, 2), commitSize)
	if err := os.Truncate(lastFileOfLog(t, dir, 3), 5); err != nil {
		t.Fatal(err)
	}

	db = openCleanup(t, dir, o)
	expectMissing(t, db, a)
	expectMissing(t, db, b)
	if n := db.Len(); n != 0 {
		t.Fatalf("len = %d, want 0", n)
	}
}

func TestCrossLogCommitInOneLogIsEnoughAndRepaired(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 0, "a"), keyInLog(4, 1, "b")
	db := mustOpen(t, dir, o)
	putPair(t, db, a, "new", b, "new")
	mustClose(t, db)

	cutTail(t, lastFileOfLog(t, dir, 1), commitSize)

	db = mustOpen(t, dir, o)
	expect(t, db, a, "new")
	expect(t, db, b, "new")
	if err := db.groups[0].merge(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, db)

	db = openCleanup(t, dir, o)
	expect(t, db, a, "new")
	expect(t, db, b, "new")
}

func TestLogCountIsPersisted(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, multiOptions(4))
	put(t, db, "k", "v", 0)
	mustClose(t, db)

	db = mustOpen(t, dir, multiOptions(0))
	if st := db.Stats(); st.Logs != 4 {
		t.Fatalf("logs = %d, want 4", st.Logs)
	}
	expect(t, db, "k", "v")
	mustClose(t, db)

	if _, err := Open(dir, multiOptions(2)); !errors.Is(err, ErrLayout) {
		t.Fatalf("open with a different log count: err = %v, want ErrLayout", err)
	}
}

func TestFilesArePrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows controls access with ACLs, not permission bits")
	}
	dir := filepath.Join(t.TempDir(), "db")
	db := mustOpen(t, dir, multiOptions(2))
	put(t, db, "before-flush", "v", 0)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		put(t, db, fmt.Sprintf("k%d", i), "v", 0)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, db)

	hints := 0
	files := 0
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files++
		}
		if filepath.Ext(path) == hintExt {
			hints++
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s has mode %v, want no access for group and others", path, info.Mode().Perm())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 || hints == 0 {
		t.Fatalf("files = %d, hint files = %d, want both above zero", files, hints)
	}
}

func TestOldMetaOpensAndIsUpgraded(t *testing.T) {
	for _, magic := range []string{"bitkv-meta 1", "casketdb-meta 1"} {
		dir := t.TempDir()
		db := mustOpen(t, dir, multiOptions(2))
		put(t, db, "k", "v", 0)
		mustClose(t, db)
		if err := os.WriteFile(filepath.Join(dir, metaName), []byte(magic+"\nlogs 2\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		db = mustOpen(t, dir, multiOptions(0))
		if st := db.Stats(); st.Logs != 2 {
			t.Fatalf("%s: logs = %d, want 2", magic, st.Logs)
		}
		expect(t, db, "k", "v")
		mustClose(t, db)
		if logs, got, err := readMeta(dir); logs != 2 || got != metaMagic || err != nil {
			t.Fatalf("%s: META after open = %d, %q, %v", magic, logs, got, err)
		}
	}
}

func TestLegacySingleLogLayoutOpens(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, multiOptions(1))
	put(t, db, "a", "1", 0)
	put(t, db, "b", "2", 0)
	mustClose(t, db)
	entries, err := os.ReadDir(logDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Rename(filepath.Join(logDir(dir), e.Name()), filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(logDir(dir)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, metaName)); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, multiOptions(4)); !errors.Is(err, ErrLayout) {
		t.Fatalf("open legacy layout with 4 logs: err = %v, want ErrLayout", err)
	}
	db = mustOpen(t, dir, multiOptions(0))
	defer mustClose(t, db)
	if st := db.Stats(); st.Logs != 1 {
		t.Fatalf("logs = %d, want 1", st.Logs)
	}
	expect(t, db, "a", "1")
	expect(t, db, "b", "2")
	put(t, db, "c", "3", 0)
	expect(t, db, "c", "3")
	if fileExists(filepath.Join(dir, metaName)) || fileExists(logDir(dir)) {
		t.Fatal("legacy database was converted to the multi-log layout")
	}
}

func TestLogsWriteInParallel(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	db := mustOpen(t, dir, o)
	for i := range 400 {
		put(t, db, fmt.Sprintf("k%d", i), "v", 0)
	}
	used := 0
	for i := range 4 {
		if ids, _ := listIDs(groupDir(dir, i), dataExt); len(ids) > 0 {
			if fi, err := os.Stat(filepath.Join(groupDir(dir, i), fileName(ids[len(ids)-1], dataExt))); err == nil && fi.Size() > 0 {
				used++
			}
		}
	}
	if used != 4 {
		t.Fatalf("only %d of 4 logs received writes", used)
	}
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	if n := db.Len(); n != 400 {
		t.Fatalf("len after reopen = %d, want 400", n)
	}
}

func markedWrites(t *testing.T, db *DB, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		ops := make([]Op, 8)
		for j := range ops {
			ops[j] = Op{Key: fmt.Sprintf("k%d-%d", i, j), Value: []byte(fmt.Sprintf("v%d", i))}
		}
		if err := db.Apply(ops, 0); err != nil {
			t.Fatal(err)
		}
		db.MarkApplied(uint64(i))
	}
}

func diskImage(t *testing.T, db *DB, keep map[int]int64) string {
	t.Helper()
	dir := t.TempDir()
	copyFiles(t, db.dir, dir, func(string) bool { return true })
	for i, g := range db.groups {
		dst := filepath.Join(dir, filepath.Base(g.dir))
		if err := os.MkdirAll(dst, 0o755); err != nil {
			t.Fatal(err)
		}
		copyFiles(t, g.dir, dst, func(string) bool { return true })
		if size, ok := keep[i]; ok {
			if err := os.Truncate(filepath.Join(dst, fileName(g.active.id, dataExt)), size); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dir
}

func expectMarked(t *testing.T, db *DB, upTo int) {
	t.Helper()
	for i := 1; i <= upTo; i++ {
		for j := range 8 {
			expect(t, db, fmt.Sprintf("k%d-%d", i, j), fmt.Sprintf("v%d", i))
		}
	}
}

func TestDurableIndexNeverOverestimates(t *testing.T) {
	o := multiOptions(4)
	o.Sync = SyncEverySec
	o.MaxFileSize = 1 << 20
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	markedWrites(t, db, 1, 50)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := db.DurableIndex(); got != 50 {
		t.Fatalf("durable index after sync = %d, want 50", got)
	}
	synced := map[int]int64{}
	for i, g := range db.groups {
		synced[i] = g.active.written.Load()
	}
	markedWrites(t, db, 51, 100)
	if got := db.DurableIndex(); got < 50 || got > 100 {
		t.Fatalf("durable index before the next sync = %d", got)
	}
	db.writeQueued()
	for _, c := range []struct {
		name string
		keep map[int]int64
		want uint64
	}{
		{"kill -9", nil, 100},
		{"power loss", synced, 50},
		{"one log lost its unsynced tail", map[int]int64{0: synced[0]}, 50},
	} {
		t.Run(c.name, func(t *testing.T) {
			img := mustOpen(t, diskImage(t, db, c.keep), o)
			defer mustClose(t, img)
			if got := img.DurableIndex(); got != c.want {
				t.Fatalf("recovered durable index %d, want %d", got, c.want)
			}
			expectMarked(t, img, int(c.want))
		})
	}
}

func TestDurableIndexSurvivesMergeAndFlush(t *testing.T) {
	o := multiOptions(2)
	o.Sync = SyncEverySec
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	markedWrites(t, db, 1, 60)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	merged := mustOpen(t, diskImage(t, db, nil), o)
	if got := merged.DurableIndex(); got != 60 {
		t.Fatalf("durable index after merge = %d, want 60", got)
	}
	expectMarked(t, merged, 60)
	mustClose(t, merged)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	flushed := mustOpen(t, diskImage(t, db, nil), o)
	defer mustClose(t, flushed)
	if got := flushed.DurableIndex(); got != 60 || flushed.Len() != 0 {
		t.Fatalf("after flush: durable index %d, %d keys", got, flushed.Len())
	}
}

func TestDurableIndexWithoutFsyncWaitsForSync(t *testing.T) {
	o := multiOptions(4)
	o.MaxFileSize = 1 << 20
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	markedWrites(t, db, 1, 30)
	db.writeQueued()
	if got := db.DurableIndex(); got != 0 {
		t.Fatalf("durable index before an fsync = %d, want 0", got)
	}
	img := mustOpen(t, diskImage(t, db, nil), o)
	defer mustClose(t, img)
	if got := img.DurableIndex(); got != 30 {
		t.Fatalf("durable index recovered from files that survived = %d, want 30", got)
	}
	expectMarked(t, img, 30)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := db.DurableIndex(); got != 30 {
		t.Fatalf("durable index after an fsync = %d, want 30", got)
	}
}
