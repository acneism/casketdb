package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/clock"
)

func testOptions() Options {
	o := DefaultOptions()
	o.Sync = SyncNo
	o.MaxFileSize = 4 << 10
	o.MergeInterval = 0
	o.ExpireInterval = 0
	o.Logs = 1
	return o
}

func logDir(dir string) string {
	return groupDir(dir, 0)
}

func mustOpen(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	db, err := Open(dir, o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func mustClose(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func write(db *DB, key string, value []byte, expireAt int64) error {
	return db.Update(Keys(key), func(tx *Tx) error {
		tx.Put(key, value, expireAt)
		return nil
	})
}

func put(t *testing.T, db *DB, key, value string, expireAt int64) {
	t.Helper()
	if err := write(db, key, []byte(value), expireAt); err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}

func increment(tx *Tx, key string) error {
	v, _, err := tx.Get(key)
	if err != nil {
		return err
	}
	n := 0
	if len(v) > 0 {
		if _, err := fmt.Sscan(string(v), &n); err != nil {
			return err
		}
	}
	tx.Put(key, []byte(fmt.Sprint(n+1)), 0)
	return nil
}

func inParallel(t *testing.T, workers int, fn func(worker int) error) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			if err := fn(w); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func checkAcrossRestart(t *testing.T, db *DB, dir string, o Options, check func(*DB)) {
	t.Helper()
	check(db)
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	check(db)
}

func del(t *testing.T, db *DB, key string) bool {
	t.Helper()
	var deleted bool
	err := db.Update(Keys(key), func(tx *Tx) error {
		deleted = tx.Delete(key)
		return nil
	})
	if err != nil {
		t.Fatalf("delete %q: %v", key, err)
	}
	return deleted
}

func get(t *testing.T, db *DB, key string) (string, bool) {
	t.Helper()
	var value []byte
	var found bool
	err := db.View(Keys(key), func(tx *Tx) error {
		var err error
		value, found, err = tx.Get(key)
		return err
	})
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return string(value), found
}

func expect(t *testing.T, db *DB, key, want string) {
	t.Helper()
	if got, ok := get(t, db, key); !ok || got != want {
		t.Fatalf("get %q = %q, %v; want %q", key, got, ok, want)
	}
}

func expectMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	if got, ok := get(t, db, key); ok {
		t.Fatalf("get %q = %q; want missing", key, got)
	}
}

func dataIDs(t *testing.T, dir string) []uint32 {
	t.Helper()
	ids, err := listIDs(logDir(dir), dataExt)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func lastDataFile(t *testing.T, dir string) string {
	t.Helper()
	ids := dataIDs(t, dir)
	if len(ids) == 0 {
		t.Fatal("no data files")
	}
	return filepath.Join(logDir(dir), fileName(ids[len(ids)-1], dataExt))
}

func copyFiles(t *testing.T, src, dst string, keep func(name string) bool) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == lockName || !keep(e.Name()) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPutGetDelete(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	put(t, db, "a", "1", 0)
	expect(t, db, "a", "1")
	put(t, db, "a", "2", 0)
	expect(t, db, "a", "2")
	if !del(t, db, "a") {
		t.Fatal("delete of existing key returned false")
	}
	expectMissing(t, db, "a")
	if del(t, db, "a") {
		t.Fatal("delete of missing key returned true")
	}
	if n := db.Len(); n != 0 {
		t.Fatalf("len = %d, want 0", n)
	}
}

func TestReopen(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for i := range 500 {
		put(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 0)
	}
	for i := 0; i < 500; i += 3 {
		del(t, db, fmt.Sprintf("k%d", i))
	}
	if db.Stats().DataFiles < 2 {
		t.Fatal("expected file rotation")
	}
	mustClose(t, db)

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	for i := range 500 {
		key := fmt.Sprintf("k%d", i)
		if i%3 == 0 {
			expectMissing(t, db, key)
		} else {
			expect(t, db, key, fmt.Sprintf("v%d", i))
		}
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	put(t, db, "a", "1", 0)
	put(t, db, "b", "2", 0)
	mustClose(t, db)

	f, err := os.OpenFile(lastDataFile(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{0xAB}, 64)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db = mustOpen(t, dir, testOptions())
	expect(t, db, "a", "1")
	expect(t, db, "b", "2")
	put(t, db, "c", "3", 0)
	mustClose(t, db)

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	expect(t, db, "c", "3")
}

func TestPartialBatchIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.MaxFileSize = 1 << 20
	db := mustOpen(t, dir, o)
	put(t, db, "before", "ok", 0)
	err := db.Update(Keys("x", "y", "z"), func(tx *Tx) error {
		tx.Put("x", []byte("1"), 0)
		tx.Put("y", []byte("2"), 0)
		tx.Put("z", []byte("3"), 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustClose(t, db)

	path := lastDataFile(t, dir)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, st.Size()-2); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	expect(t, db, "before", "ok")
	for _, key := range []string{"x", "y", "z"} {
		expectMissing(t, db, key)
	}
	st, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := recordSize(len("before"), len("ok")); st.Size() != want {
		t.Fatalf("file size after recovery = %d, want %d", st.Size(), want)
	}
}

func manualOptions() (Options, *clock.Manual) {
	clk := clock.NewManual(time.Now())
	o := testOptions()
	o.Now = clk.Now
	return o, clk
}

func at(clk *clock.Manual, d time.Duration) int64 {
	return clk.Now().Add(d).UnixMilli()
}

func TestExpiredValueDoesNotResurrect(t *testing.T) {
	dir := t.TempDir()
	o, clk := manualOptions()
	db := mustOpen(t, dir, o)
	put(t, db, "k", "old", 0)
	put(t, db, "k", "new", at(clk, 50*time.Millisecond))
	expect(t, db, "k", "new")
	clk.Advance(49 * time.Millisecond)
	expect(t, db, "k", "new")
	clk.Advance(time.Millisecond)
	expectMissing(t, db, "k")
	mustClose(t, db)

	db = mustOpen(t, dir, o)
	expectMissing(t, db, "k")
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	expectMissing(t, db, "k")
	mustClose(t, db)

	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	expectMissing(t, db, "k")
}

func TestMergeDropsExpiredKeys(t *testing.T) {
	o, clk := manualOptions()
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	put(t, db, "short", "v", at(clk, 30*time.Millisecond))
	put(t, db, "long", "v", 0)
	clk.Advance(time.Second)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if n := db.Len(); n != 1 {
		t.Fatalf("len after merge = %d, want 1", n)
	}
	expect(t, db, "long", "v")
}

func TestActiveExpirySweepsSparseTTLKeys(t *testing.T) {
	for _, long := range []int{50, 5000} {
		t.Run(fmt.Sprintf("long=%d", long), func(t *testing.T) {
			o, clk := manualOptions()
			o.MaxFileSize = 64 << 20
			db := mustOpen(t, t.TempDir(), o)
			defer mustClose(t, db)
			const plain, short = 20000, 50
			err := db.Update(All(), func(tx *Tx) error {
				for i := range plain {
					tx.Put(fmt.Sprintf("plain:%d", i), []byte("v"), 0)
				}
				for i := range short {
					tx.Put(fmt.Sprintf("short:%d", i), []byte("v"), at(clk, time.Second))
				}
				for i := range long {
					tx.Put(fmt.Sprintf("long:%d", i), []byte("v"), at(clk, time.Hour))
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			clk.Advance(2 * time.Second)
			want := plain + long
			cycles := 0
			for db.Len() > want {
				if cycles++; cycles > numShards {
					t.Fatalf("after %d cycles %d keys remain, want %d", numShards, db.Len(), want)
				}
				db.expireCycle()
			}
			st := db.Stats()
			t.Logf("expired %d keys in %d cycles", st.ExpiredKeys, cycles)
			if st.ExpiredKeys != short || st.KeysWithTTL != long {
				t.Fatalf("stats = %+v, want %d expired and %d keys with TTL left", st, short, long)
			}
			for i := range short {
				expectMissing(t, db, fmt.Sprintf("short:%d", i))
			}
			expect(t, db, "long:0", "v")
			if maxCycles := numShards / expireShardsPerCycle; cycles > maxCycles {
				t.Fatalf("sweep took %d cycles, want at most %d", cycles, maxCycles)
			}
		})
	}
}

func TestActiveExpiryIgnoresRenewedKeys(t *testing.T) {
	o, clk := manualOptions()
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	put(t, db, "k", "old", at(clk, time.Second))
	clk.Advance(2 * time.Second)
	put(t, db, "k", "renewed", at(clk, time.Hour))
	for range numShards {
		db.expireCycle()
	}
	expect(t, db, "k", "renewed")
	if st := db.Stats(); st.ExpiredKeys != 0 {
		t.Fatalf("expired %d keys, want 0", st.ExpiredKeys)
	}
}

func TestMergeCompacts(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for round := range 10 {
		for i := range 100 {
			put(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-%d", i, round), 0)
		}
	}
	for i := 0; i < 100; i += 2 {
		del(t, db, fmt.Sprintf("k%d", i))
	}
	before := db.Stats()
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	after := db.Stats()
	if after.TotalBytes >= before.TotalBytes/2 {
		t.Fatalf("merge did not compact: %d -> %d bytes", before.TotalBytes, after.TotalBytes)
	}
	if after.LiveBytes != before.LiveBytes || after.Keys != before.Keys {
		t.Fatalf("live data changed: %+v -> %+v", before, after)
	}
	check := func(db *DB) {
		t.Helper()
		for i := range 100 {
			key := fmt.Sprintf("k%d", i)
			if i%2 == 0 {
				expectMissing(t, db, key)
			} else {
				expect(t, db, key, fmt.Sprintf("v%d-9", i))
			}
		}
	}
	check(db)
	if hints, _ := listIDs(logDir(dir), hintExt); len(hints) == 0 {
		t.Fatal("no hint files after merge")
	}
	if _, err := os.Stat(filepath.Join(logDir(dir), mergeDirName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("merge directory left behind: %v", err)
	}
	mustClose(t, db)

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	check(db)
	put(t, db, "k1", "fresh", 0)
	expect(t, db, "k1", "fresh")
}

func TestMergeWithConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	var mu sync.Mutex
	model := map[string]string{}
	done := make(chan error, 1)
	go func() {
		for i := range 3000 {
			key := fmt.Sprintf("k%d", i%64)
			value := fmt.Sprintf("v%d", i)
			if err := write(db, key, []byte(value), 0); err != nil {
				done <- err
				return
			}
			mu.Lock()
			model[key] = value
			mu.Unlock()
		}
		done <- nil
	}()
	merges := 0
	for running := true; running; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			running = false
		default:
		}
		if err := db.Merge(); err != nil {
			t.Fatal(err)
		}
		merges++
	}
	if merges < 2 {
		t.Fatalf("only %d merges ran", merges)
	}
	verify := func(db *DB) {
		t.Helper()
		for k, v := range model {
			expect(t, db, k, v)
		}
		if db.Len() != len(model) {
			t.Fatalf("len = %d, want %d", db.Len(), len(model))
		}
	}
	checkAcrossRestart(t, db, dir, testOptions(), verify)
}

func TestMergeCompletesAfterCrash(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for i := range 300 {
		put(t, db, fmt.Sprintf("k%d", i%50), fmt.Sprintf("v%d", i), 0)
	}
	for i := 0; i < 50; i += 5 {
		del(t, db, fmt.Sprintf("k%d", i))
	}
	mustClose(t, db)
	ids := dataIDs(t, dir)
	boundary := ids[len(ids)-1]

	scratch := t.TempDir()
	copyFiles(t, dir, scratch, func(string) bool { return true })
	if err := os.MkdirAll(logDir(scratch), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFiles(t, logDir(dir), logDir(scratch), func(string) bool { return true })
	sdb := mustOpen(t, scratch, testOptions())
	if err := sdb.Merge(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, sdb)

	mergeDir := filepath.Join(logDir(dir), mergeDirName)
	if err := os.MkdirAll(mergeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	copyFiles(t, logDir(scratch), mergeDir, func(name string) bool {
		base := strings.TrimSuffix(strings.TrimSuffix(name, dataExt), hintExt)
		return fileExists(filepath.Join(logDir(scratch), base+hintExt))
	})
	if err := writeMarker(mergeDir, boundary); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	for i := range 50 {
		key := fmt.Sprintf("k%d", i)
		if i%5 == 0 {
			expectMissing(t, db, key)
		} else {
			expect(t, db, key, fmt.Sprintf("v%d", 250+i))
		}
	}
	for _, id := range dataIDs(t, dir) {
		if id <= boundary {
			t.Fatalf("old data file %d was not removed", id)
		}
	}
	if _, err := os.Stat(mergeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("merge directory left behind: %v", err)
	}
}

func TestIncompleteMergeIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	put(t, db, "a", "1", 0)
	mustClose(t, db)
	mergeDir := filepath.Join(logDir(dir), mergeDirName)
	if err := os.MkdirAll(mergeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mergeDir, fileName(99, dataExt)), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	expect(t, db, "a", "1")
	if _, err := os.Stat(mergeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete merge directory left behind: %v", err)
	}
	if fileExists(filepath.Join(logDir(dir), fileName(99, dataExt))) {
		t.Fatal("file from incomplete merge was moved into the database")
	}
}

func TestFlushPersists(t *testing.T) {
	for _, logs := range []int{1, 4} {
		t.Run(fmt.Sprintf("logs=%d", logs), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions()
			o.Logs = logs
			db := mustOpen(t, dir, o)
			for i := range 20 {
				put(t, db, fmt.Sprintf("old%d", i), "1", 0)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			expectMissing(t, db, "old0")
			put(t, db, "c", "3", 0)
			mustClose(t, db)

			db = mustOpen(t, dir, o)
			defer mustClose(t, db)
			for i := range 20 {
				expectMissing(t, db, fmt.Sprintf("old%d", i))
			}
			expect(t, db, "c", "3")
			if n := db.Len(); n != 1 {
				t.Fatalf("len = %d, want 1", n)
			}
			if fileExists(filepath.Join(dir, flushName)) {
				t.Fatal("flush marker left behind")
			}
		})
	}
}

func TestFlushCompletesAfterCrash(t *testing.T) {
	dir := t.TempDir()
	saved := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	put(t, db, "a", "1", 0)
	put(t, db, "b", "2", 0)
	mustClose(t, db)
	ids := dataIDs(t, dir)
	bound := ids[len(ids)-1]
	copyFiles(t, logDir(dir), saved, func(string) bool { return true })

	db = mustOpen(t, dir, testOptions())
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	put(t, db, "c", "3", 0)
	mustClose(t, db)
	copyFiles(t, saved, logDir(dir), func(string) bool { return true })
	if err := writeFlushMarker(dir, []uint32{bound}); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	expectMissing(t, db, "a")
	expectMissing(t, db, "b")
	expect(t, db, "c", "3")
	for _, id := range dataIDs(t, dir) {
		if id <= bound {
			t.Fatalf("file %d from before FLUSH was not removed", id)
		}
	}
	if fileExists(filepath.Join(dir, flushName)) {
		t.Fatal("flush marker left behind")
	}
}

func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	if _, err := Open(dir, testOptions()); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: err = %v, want ErrLocked", err)
	}
}

func TestCorruptionInOlderFileFails(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for i := range 300 {
		put(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 0)
	}
	mustClose(t, db)
	ids := dataIDs(t, dir)
	if len(ids) < 2 {
		t.Fatal("expected several data files")
	}
	path := filepath.Join(logDir(dir), fileName(ids[0], dataExt))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open corrupted db: err = %v, want ErrCorrupt", err)
	}
}

func TestReadDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.MaxFileSize = 1 << 20
	db := mustOpen(t, dir, o)
	defer mustClose(t, db)
	put(t, db, "k", "value", 0)
	g := db.groups[0]
	g.logMu.Lock()
	df := g.active
	err := g.rotateLocked(df.id + 1)
	g.logMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := df.f.WriteAt([]byte("X"), headerSize+1); err != nil {
		t.Fatal(err)
	}
	err = db.View(Keys("k"), func(tx *Tx) error {
		_, _, err := tx.Get("k")
		return err
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read of corrupted record: err = %v, want ErrCorrupt", err)
	}
}

func TestScanVisitsEveryKeyOnce(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	for i := range 1000 {
		put(t, db, fmt.Sprintf("key:%d", i), "v", 0)
	}
	seen := map[string]int{}
	var cursor uint64
	for {
		next, keys := db.Scan(cursor, 50, nil)
		for _, k := range keys {
			seen[k]++
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != 1000 {
		t.Fatalf("scan saw %d keys, want 1000", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("key %q returned %d times", k, n)
		}
	}
	matched := db.Keys(func(k string) bool { return strings.HasSuffix(k, "7") })
	if len(matched) != 100 {
		t.Fatalf("keys ending in 7: %d, want 100", len(matched))
	}
}

func TestSyncAlwaysConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.Sync = SyncAlways
	db := mustOpen(t, dir, o)
	inParallel(t, 8, func(g int) error {
		for i := range 50 {
			if err := write(db, fmt.Sprintf("g%d-%d", g, i), []byte("v"), 0); err != nil {
				return err
			}
		}
		return nil
	})
	checkAcrossRestart(t, db, dir, o, func(db *DB) {
		if n := db.Len(); n != 400 {
			t.Fatalf("len = %d, want 400", n)
		}
	})
}

func TestConcurrentReadModifyWrite(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncNo, SyncAlways} {
		t.Run(policy.String(), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions()
			o.Sync = policy
			db := mustOpen(t, dir, o)
			inParallel(t, 8, func(g int) error {
				for i := range 150 {
					key := fmt.Sprintf("counter%d", (g+i)%4)
					err := db.Update(Keys(key), func(tx *Tx) error {
						return increment(tx, key)
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			checkAcrossRestart(t, db, dir, o, func(db *DB) {
				for k := range 4 {
					expect(t, db, fmt.Sprintf("counter%d", k), "300")
				}
			})
		})
	}
}

func TestMultiShardTransactionsUnderContention(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	keys := make([]string, 16)
	for i := range keys {
		keys[i] = fmt.Sprint("c", i)
	}
	all := append(slices.Clone(keys), "total")
	value := func(tx *Tx, key string) int {
		v, _, _ := tx.Get(key)
		n := 0
		fmt.Sscan(string(v), &n)
		return n
	}
	inParallel(t, 16, func(g int) error {
		rng := rand.New(rand.NewPCG(uint64(g), 1))
		for range 300 {
			a, b := keys[rng.IntN(len(keys))], keys[rng.IntN(len(keys))]
			if err := db.Update(Keys(a, b, "total"), func(tx *Tx) error {
				for _, key := range []string{a, b, "total", "total"} {
					if err := increment(tx, key); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			if err := db.View(Keys(all...), func(tx *Tx) error {
				sum := 0
				for _, key := range keys {
					sum += value(tx, key)
				}
				if total := value(tx, "total"); sum != total {
					return fmt.Errorf("a reader saw counters summing to %d and a total of %d", sum, total)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	expect(t, db, "total", "9600")
}

func TestGroupCommitSharesFsyncs(t *testing.T) {
	o := testOptions()
	o.Sync = SyncAlways
	o.MaxFileSize = 64 << 20
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	const writers, perWriter = 16, 30
	inParallel(t, writers, func(g int) error {
		for i := range perWriter {
			if err := write(db, fmt.Sprintf("g%d-%d", g, i), []byte("v"), 0); err != nil {
				return err
			}
		}
		return nil
	})
	st := db.Stats()
	t.Logf("%d commits, %d writes, %d fsyncs", writers*perWriter, st.Writes, st.Fsyncs)
	if st.Fsyncs >= writers*perWriter || st.Writes >= writers*perWriter {
		t.Fatalf("no batching: %d writes and %d fsyncs for %d commits", st.Writes, st.Fsyncs, writers*perWriter)
	}
}

func TestReadsDuringWritesAreConsistent(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	const versions = 2000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range versions {
			value := fmt.Sprintf("v%05d", i)
			if err := db.Update(Keys("k", "mirror"), func(tx *Tx) error {
				tx.Put("k", []byte(value), 0)
				tx.Put("mirror", []byte(value), 0)
				return nil
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := ""
			for {
				select {
				case <-done:
					return
				default:
				}
				var k, m []byte
				err := db.View(Keys("k", "mirror"), func(tx *Tx) error {
					var err error
					if k, _, err = tx.Get("k"); err != nil {
						return err
					}
					m, _, err = tx.Get("mirror")
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
				if string(k) != string(m) {
					t.Errorf("torn batch: k=%q mirror=%q", k, m)
					return
				}
				if string(k) < last {
					t.Errorf("value went backwards: %q after %q", k, last)
					return
				}
				last = string(k)
			}
		}()
	}
	wg.Wait()
	expect(t, db, "k", fmt.Sprintf("v%05d", versions-1))
}

func keysInDifferentShards() (string, string) {
	a := "a"
	for i := 0; ; i++ {
		b := fmt.Sprintf("b%d", i)
		if shardIndex(a) != shardIndex(b) {
			return a, b
		}
	}
}

func TestTxRejectsKeysOutsideScope(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	a, b := keysInDifferentShards()
	err := db.Update(Keys(a), func(tx *Tx) error {
		tx.Put(b, []byte("v"), 0)
		return nil
	})
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("put outside scope: err = %v, want ErrNotLocked", err)
	}
	expectMissing(t, db, b)
	err = db.View(Keys(a), func(tx *Tx) error {
		_, _, err := tx.Get(b)
		return err
	})
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("get outside scope: err = %v, want ErrNotLocked", err)
	}
	err = db.View(Keys(a), func(tx *Tx) error {
		tx.Len()
		return nil
	})
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("len outside scope: err = %v, want ErrNotLocked", err)
	}
}

func TestParallelWritersAndGlobalReaders(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.Logs = 4
	db := mustOpen(t, dir, o)
	const writers, perWriter = 8, 300
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			db.Keys(func(k string) bool { return strings.HasPrefix(k, "shared:") })
			db.Len()
			db.Stats()
			if err := db.Merge(); err != nil && !errors.Is(err, ErrMergeInProgress) {
				t.Error(err)
				return
			}
		}
	})
	inParallel(t, writers, func(g int) error {
		for i := range perWriter {
			a, b := fmt.Sprintf("w%d:%d", g, i), fmt.Sprintf("shared:%d", i%16)
			err := db.Update(Keys(a, b), func(tx *Tx) error {
				tx.Put(a, []byte("v"), 0)
				return increment(tx, b)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	close(stop)
	readers.Wait()
	checkAcrossRestart(t, db, dir, o, func(db *DB) {
		if n := db.Len(); n != writers*perWriter+16 {
			t.Fatalf("len = %d, want %d", n, writers*perWriter+16)
		}
		total := 0
		for i := range 16 {
			v, _ := get(t, db, fmt.Sprintf("shared:%d", i))
			n := 0
			if _, err := fmt.Sscan(v, &n); err != nil {
				t.Fatalf("shared:%d = %q: %v", i, v, err)
			}
			total += n
		}
		if total != writers*perWriter {
			t.Fatalf("shared counters sum to %d, want %d", total, writers*perWriter)
		}
	})
}

func TestLinkFilesMakesARestorableCopy(t *testing.T) {
	o := testOptions()
	o.Logs = 2
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	value := strings.Repeat("v", 40)
	for i := range 200 {
		put(t, db, fmt.Sprintf("k%d", i), value, 0)
	}
	snap := t.TempDir()
	logs, files, err := db.LinkFiles(snap)
	if err != nil {
		t.Fatal(err)
	}
	put(t, db, "after", "x", 0)
	dir := t.TempDir()
	for _, f := range files {
		p := filepath.FromSlash(f.Path)
		if err := CopyPrefix(filepath.Join(snap, p), filepath.Join(dir, p), f.Size); err != nil {
			t.Fatal(err)
		}
	}
	o.Logs = logs
	restored := mustOpen(t, dir, o)
	defer mustClose(t, restored)
	n := 0
	err = restored.Dump(func(op Op) error {
		n++
		if string(op.Value) != value {
			return fmt.Errorf("%s = %q", op.Key, op.Value)
		}
		return nil
	})
	if err != nil || n != 200 {
		t.Fatalf("dumped %d keys, err %v; want 200", n, err)
	}
	expectMissing(t, restored, "after")
}

func TestKinds(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	db := mustOpen(t, dir, o)
	if err := db.Update(Keys("h", "s"), func(tx *Tx) error {
		tx.PutKind("h", 3, []byte("fields"), 0)
		tx.Put("s", []byte("text"), 0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	check := func(db *DB) {
		t.Helper()
		if err := db.View(Keys("h", "s"), func(tx *Tx) error {
			if _, _, err := tx.Get("h"); !errors.Is(err, ErrWrongKind) {
				t.Errorf("Get of a typed value = %v, want ErrWrongKind", err)
			}
			if v, kind, ok, err := tx.GetKind("h"); string(v) != "fields" || kind != 3 || !ok || err != nil {
				t.Errorf("GetKind = %q, %d, %v, %v", v, kind, ok, err)
			}
			if v, kind, ok, err := tx.GetKind("s"); string(v) != "text" || kind != 0 || !ok || err != nil {
				t.Errorf("GetKind of a string = %q, %d, %v, %v", v, kind, ok, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.View(Shardwise(), func(tx *Tx) error {
			if _, keys := tx.Scan(0, math.MaxInt, nil, func(k Kind) bool { return k == 3 }); !slices.Equal(keys, []string{"h"}) {
				t.Errorf("Scan of kind 3 = %v", keys)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		var ops []Op
		if err := db.Dump(func(op Op) error { ops = append(ops, op); return nil }); err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(ops, func(op Op) bool { return op.Key == "h" && op.Kind == 3 && string(op.Value) == "fields" }) {
			t.Errorf("Dump = %+v", ops)
		}
	}
	check(db)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	checkAcrossRestart(t, db, dir, o, check)
}

func tableValue(gen uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, gen)
}

func members(t *testing.T, db *DB, key string) map[string]string {
	t.Helper()
	got := map[string]string{}
	if err := db.View(Keys(key), func(tx *Tx) error {
		return tx.Members(key, true, func(m string, v []byte) bool {
			got[m] = string(v)
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestMembers(t *testing.T) {
	o, clk := manualOptions()
	dir := t.TempDir()
	db := mustOpen(t, dir, o)
	update := func(fn func(tx *Tx)) {
		t.Helper()
		if err := db.Update(Keys("h", "old", "ttl", "s"), func(tx *Tx) error { fn(tx); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	update(func(tx *Tx) {
		tx.PutKind("h", Table|4, tableValue(7), 0)
		tx.PutMember("h", "a", []byte("1"))
		tx.PutMember("h", "b", []byte("2"))
		tx.PutKind("old", Table|4, tableValue(1), 0)
		tx.PutMember("old", "x", []byte("gone"))
		tx.PutKind("ttl", Table|4, tableValue(3), at(clk, time.Second))
		tx.PutMember("ttl", "y", []byte("expires"))
		tx.PutKind("s", Table|4, tableValue(9), 0)
		tx.PutMember("s", "q", []byte("dropped"))
	})
	update(func(tx *Tx) {
		tx.PutMember("h", "a", []byte("10"))
		if !tx.DeleteMember("h", "b") || tx.DeleteMember("h", "nope") {
			t.Error("DeleteMember reported the wrong result")
		}
		tx.PutMember("h", "c", []byte("3"))
		tx.PutKind("old", Table|4, tableValue(2), 0)
		tx.PutMember("old", "z", []byte("new"))
		tx.Put("s", []byte("string"), 0)
	})
	if err := db.Update(Keys("s"), func(tx *Tx) error {
		tx.PutMember("s", "q", []byte("refused"))
		return nil
	}); !errors.Is(err, ErrNotTable) {
		t.Fatalf("PutMember on a string = %v, want ErrNotTable", err)
	}
	clk.Advance(2 * time.Second)
	want := map[string]map[string]string{"h": {"a": "10", "c": "3"}, "old": {"z": "new"}, "ttl": {}, "s": {}}
	check := func(db *DB) {
		t.Helper()
		for key, w := range want {
			if got := members(t, db, key); !maps.Equal(got, w) {
				t.Errorf("members of %s = %v, want %v", key, got, w)
			}
		}
		var dumped []Op
		if err := db.Dump(func(op Op) error { dumped = append(dumped, op); return nil }); err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(dumped, func(op Op) bool { return op.IsMember && op.Key == "h" && op.Member == "c" && string(op.Value) == "3" }) {
			t.Errorf("Dump = %+v", dumped)
		}
	}
	check(db)
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	check(db)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	checkAcrossRestart(t, db, dir, o, check)
}

func inOrder(t *testing.T, tx *Tx, key string, reverse bool) []string {
	t.Helper()
	n, err := tx.MemberCount(key, func([]byte, string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	from := 0
	if reverse {
		from = n - 1
	}
	var got []string
	if err := tx.MemberRange(key, from, reverse, func(m string, v []byte) bool {
		got = append(got, m+"="+string(v))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestOrderedMembers(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	db := mustOpen(t, dir, o)
	update := func(fn func(tx *Tx)) {
		t.Helper()
		if err := db.Update(Keys("z"), func(tx *Tx) error { fn(tx); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	update(func(tx *Tx) {
		tx.PutKind("z", Table|Ordered|3, tableValue(4), 0)
		for _, m := range []string{"c2", "a2", "b1", "d3"} {
			tx.PutMember("z", m[:1], []byte(m[1:]))
		}
		if got := inOrder(t, tx, "z", false); !slices.Equal(got, []string{"b=1", "a=2", "c=2", "d=3"}) {
			t.Errorf("order inside the creating transaction = %v", got)
		}
	})
	update(func(tx *Tx) {
		tx.PutKind("z", Table|Ordered|3, tableValue(4), 0)
		tx.PutMember("z", "d", []byte("0"))
		tx.DeleteMember("z", "c")
	})
	check := func(db *DB) {
		t.Helper()
		if err := db.View(Keys("z"), func(tx *Tx) error {
			if got := inOrder(t, tx, "z", false); !slices.Equal(got, []string{"d=0", "b=1", "a=2"}) {
				t.Errorf("forward order = %v", got)
			}
			if got := inOrder(t, tx, "z", true); !slices.Equal(got, []string{"a=2", "b=1", "d=0"}) {
				t.Errorf("reverse order = %v", got)
			}
			if n, err := tx.MemberCount("z", func(v []byte, _ string) bool { return string(v) < "2" }); n != 2 || err != nil {
				t.Errorf("members below 2 = %d, %v", n, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check(db)
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	check(db)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	checkAcrossRestart(t, db, dir, o, check)
}

func TestOrderedByMember(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	db := mustOpen(t, dir, o)
	kind := Table | Ordered | ByMember | 6
	if err := db.Update(Keys("s"), func(tx *Tx) error {
		tx.PutKind("s", kind, tableValue(9), 0)
		for _, m := range []string{"c", "a", "d", "b"} {
			tx.PutMember("s", m, []byte("value of "+m))
		}
		if got := inOrder(t, tx, "s", false); !slices.Equal(got, []string{"a=", "b=", "c=", "d="}) {
			t.Errorf("order inside the creating transaction = %v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(Keys("s"), func(tx *Tx) error {
		tx.PutKind("s", kind, tableValue(9), 0)
		tx.DeleteMember("s", "b")
		tx.PutMember("s", "aa", []byte("z"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	check := func(db *DB) {
		t.Helper()
		if err := db.View(Keys("s"), func(tx *Tx) error {
			if got := inOrder(t, tx, "s", true); !slices.Equal(got, []string{"d=", "c=", "aa=", "a="}) {
				t.Errorf("reverse order = %v", got)
			}
			if v, ok, err := tx.GetMember("s", "c"); string(v) != "value of c" || !ok || err != nil {
				t.Errorf("GetMember c = %q, %v, %v", v, ok, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check(db)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	checkAcrossRestart(t, db, dir, o, check)
}

func TestProposedMembers(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	var published []Op
	publish := func(ops []Op) (uint64, error) {
		published = ops
		return 1, nil
	}
	if _, err := db.Propose(Keys("p"), 1, func(tx *Tx) error {
		tx.PutKind("p", Table|4, tableValue(5), 0)
		tx.PutMember("p", "m", []byte("v"))
		return nil
	}, publish); err != nil {
		t.Fatal(err)
	}
	if len(published) != 2 || !published[1].IsMember || published[1].Member != "m" {
		t.Fatalf("published %+v", published)
	}
	depends, err := db.Propose(Keys("p"), 1, func(tx *Tx) error {
		if v, ok, err := tx.GetMember("p", "m"); string(v) != "v" || !ok || err != nil {
			t.Errorf("proposed member = %q, %v, %v", v, ok, err)
		}
		return nil
	}, publish)
	if err != nil || depends != 1 {
		t.Fatalf("depends %d, %v", depends, err)
	}
	depends, err = db.Propose(Keys("p"), 1, func(tx *Tx) error {
		if got := inOrder(t, tx, "p", false); !slices.Equal(got, []string{"m=v"}) {
			t.Errorf("proposed members in order = %v", got)
		}
		return nil
	}, publish)
	if err != nil || depends != 1 {
		t.Fatalf("depends of an ordered read %d, %v", depends, err)
	}
	if err := db.Apply(published, 1); err != nil {
		t.Fatal(err)
	}
	if got := members(t, db, "p"); !maps.Equal(got, map[string]string{"m": "v"}) {
		t.Fatalf("members after Apply = %v", got)
	}
}

func TestRandomMember(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	if err := db.Update(Keys("r", "s"), func(tx *Tx) error {
		tx.PutKind("r", Table|4, tableValue(5), 0)
		for i := range 200 {
			tx.PutMember("r", fmt.Sprint(i), []byte(fmt.Sprint("v", i)))
		}
		tx.Put("s", []byte("string"), 0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	picked := map[string]int{}
	for range 4000 {
		if err := db.View(Keys("r"), func(tx *Tx) error {
			m, v, ok, err := tx.RandomMember("r", true, nil)
			if err != nil || !ok || string(v) != "v"+m {
				t.Fatalf("RandomMember = %q, %q, %v, %v", m, v, ok, err)
			}
			picked[m]++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(picked) != 200 || slices.Max(slices.Collect(maps.Values(picked))) >= 70 {
		t.Fatalf("4000 picks of 200 members: %d distinct, counts %v", len(picked), picked)
	}
	if err := db.Update(Keys("r", "s", "missing"), func(tx *Tx) error {
		for i := range 200 {
			if i != 7 {
				tx.DeleteMember("r", fmt.Sprint(i))
			}
		}
		tx.PutMember("r", "new", []byte("n"))
		for range 20 {
			if m, _, ok, err := tx.RandomMember("r", false, nil); m != "7" || !ok || err != nil {
				t.Fatalf("RandomMember with pending deletes = %q, %v, %v", m, ok, err)
			}
		}
		if m, v, ok, err := tx.RandomMember("r", true, func(m string) bool { return m == "7" }); m != "new" || string(v) != "n" || !ok || err != nil {
			t.Fatalf("RandomMember skipping 7 = %q, %q, %v, %v", m, v, ok, err)
		}
		if _, _, ok, err := tx.RandomMember("r", false, func(string) bool { return true }); ok || err != nil {
			t.Fatalf("RandomMember skipping all = %v, %v", ok, err)
		}
		for _, key := range []string{"s", "missing"} {
			if _, _, ok, err := tx.RandomMember(key, false, nil); ok || err != nil {
				t.Fatalf("RandomMember(%q) = %v, %v", key, ok, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSystemState(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	var seen []string
	db.WatchSystem(func(b []byte) { seen = append(seen, string(b)) })
	if err := db.SetSystem([]byte(`{"users":{}}`)); err != nil {
		t.Fatal(err)
	}
	snap := t.TempDir()
	_, files, err := db.LinkFiles(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(files, func(f SnapshotFile) bool { return f.Path == "SYSTEM" && f.Size == 12 }) {
		t.Fatalf("snapshot files %v lack SYSTEM", files)
	}
	mustClose(t, db)
	db = mustOpen(t, dir, testOptions())
	if got := string(db.System()); got != `{"users":{}}` {
		t.Fatalf("system state after reopen = %q", got)
	}
	if err := db.SetSystem(nil); err != nil {
		t.Fatal(err)
	}
	mustClose(t, db)
	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	if db.System() != nil || !slices.Equal(seen, []string{"", `{"users":{}}`}) {
		t.Fatalf("system state %q after clearing, hook saw %q", db.System(), seen)
	}
}

func TestParseSyncPolicy(t *testing.T) {
	for _, p := range []SyncPolicy{SyncAlways, SyncEverySec, SyncNo} {
		if got, err := ParseSyncPolicy(strings.ToUpper(p.String())); err != nil || got != p {
			t.Fatalf("ParseSyncPolicy(%q) = %v, %v", strings.ToUpper(p.String()), got, err)
		}
	}
	if _, err := ParseSyncPolicy("never"); err == nil {
		t.Fatal("ParseSyncPolicy accepted never")
	}
}

func TestSetSyncTakesEffect(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	db.SetSync(SyncAlways)
	before := db.Stats().Fsyncs
	put(t, db, "k", "v", 0)
	if db.Stats().Fsyncs == before {
		t.Fatal("a write after SetSync(SyncAlways) returned without an fsync")
	}
	if db.Options().Sync != SyncAlways {
		t.Fatalf("Options().Sync = %v, want always", db.Options().Sync)
	}
}

func TestReadOnlyTxRejectsWrites(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	err := db.View(Keys("k"), func(tx *Tx) error {
		tx.Put("k", []byte("v"), 0)
		return nil
	})
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("err = %v, want ErrReadOnly", err)
	}
	expectMissing(t, db, "k")
}

func TestModel(t *testing.T) {
	for _, logs := range []int{1, 4} {
		t.Run(fmt.Sprintf("logs=%d", logs), func(t *testing.T) {
			o := testOptions()
			o.Logs = logs
			runModel(t, o)
		})
	}
}

func runModel(t *testing.T, o Options) {
	dir := t.TempDir()
	db := mustOpen(t, dir, o)
	defer func() { mustClose(t, db) }()
	rng := rand.New(rand.NewPCG(1, 2))
	model := map[string]string{}
	for i := range 20000 {
		key := fmt.Sprintf("key%d", rng.IntN(200))
		switch r := rng.IntN(10); {
		case r < 5:
			value := fmt.Sprintf("value-%d-%s", i, strings.Repeat("x", rng.IntN(40)))
			put(t, db, key, value, 0)
			model[key] = value
		case r < 7:
			_, want := model[key]
			if got := del(t, db, key); got != want {
				t.Fatalf("op %d: delete %q = %v, want %v", i, key, got, want)
			}
			delete(model, key)
		case r < 9:
			got, ok := get(t, db, key)
			want, wantOK := model[key]
			if ok != wantOK || got != want {
				t.Fatalf("op %d: get %q = %q, %v; want %q, %v", i, key, got, ok, want, wantOK)
			}
		default:
			other := fmt.Sprintf("key%d", rng.IntN(200))
			err := db.Update(Keys(key, other), func(tx *Tx) error {
				tx.Put(key, []byte("batch-"+key), 0)
				tx.Delete(other)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			model[key] = "batch-" + key
			delete(model, other)
		}
		if i%2000 == 1999 {
			if err := db.Merge(); err != nil {
				t.Fatal(err)
			}
		}
		if i%5000 == 4999 {
			mustClose(t, db)
			db = mustOpen(t, dir, o)
		}
	}
	for k, v := range model {
		expect(t, db, k, v)
	}
	if n := db.Len(); n != len(model) {
		t.Fatalf("len = %d, want %d", n, len(model))
	}
}

func TestReadsServedFromMemory(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	value := strings.Repeat("v", 100)
	for i := range 300 {
		put(t, db, fmt.Sprintf("k%d", i), value, 0)
	}
	check := func(db *DB) {
		t.Helper()
		g := db.groups[0]
		g.logMu.Lock()
		active := g.active
		g.logMu.Unlock()
		g.filesMu.RLock()
		sealed := 0
		for _, df := range g.files {
			if df == active {
				if p := df.mem.Load(); p == nil || int64(len(*p)) != df.written.Load() {
					t.Fatalf("active file %d is not mirrored in memory", df.id)
				}
				continue
			}
			sealed++
			if int64(len(df.mm)) != df.size {
				t.Fatalf("sealed file %d: mapped %d of %d bytes", df.id, len(df.mm), df.size)
			}
		}
		g.filesMu.RUnlock()
		if sealed == 0 {
			t.Fatal("no sealed files to check")
		}
		for i := range 300 {
			if got, ok := get(t, db, fmt.Sprintf("k%d", i)); !ok || got != value {
				t.Fatalf("k%d = %q, %v", i, got, ok)
			}
		}
	}
	check(db)
	for i := range 150 {
		put(t, db, fmt.Sprintf("k%d", i), value, 0)
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	checkAcrossRestart(t, db, dir, testOptions(), check)
}

func TestProposedWritesVisibility(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	var published [][]Op
	propose := func(term uint64, fn func(tx *Tx) error) uint64 {
		t.Helper()
		index, err := db.Propose(Keys("k"), term, fn, func(ops []Op) (uint64, error) {
			published = append(published, ops)
			return uint64(len(published)), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return index
	}
	incr := func(tx *Tx) error {
		v, _, err := tx.Get("k")
		tx.Put("k", append(v, 'x'), 0)
		return err
	}
	seen := func(term uint64) string {
		var v []byte
		propose(term, func(tx *Tx) error {
			v, _, _ = tx.Get("k")
			return nil
		})
		return string(v)
	}
	propose(7, incr)
	propose(7, incr)
	if got := seen(7); got != "xx" {
		t.Fatalf("same term sees %q, want xx", got)
	}
	if got := seen(8); got != "" {
		t.Fatalf("other term sees %q, want nothing", got)
	}
	if got, ok := get(t, db, "k"); ok {
		t.Fatalf("reader sees uncommitted %q", got)
	}
	if err := db.Apply(published[0], 1); err != nil {
		t.Fatal(err)
	}
	if got, _ := get(t, db, "k"); got != "x" {
		t.Fatalf("after first apply reader sees %q, want x", got)
	}
	if got := seen(7); got != "xx" {
		t.Fatalf("after first apply same term sees %q, want xx", got)
	}
	if err := db.Apply(published[1], 2); err != nil {
		t.Fatal(err)
	}
	propose(7, incr)
	db.DropProposed()
	if got := seen(7); got != "xx" {
		t.Fatalf("after drop same term sees %q, want committed xx", got)
	}
}

func TestProposeReportsWhatTheOutcomeDependsOn(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	next := uint64(10)
	propose := func(term uint64, fn func(tx *Tx) error) (uint64, error) {
		return db.Propose(Keys("k"), term, fn, func([]Op) (uint64, error) {
			next++
			return next, nil
		})
	}
	read := func(tx *Tx) error {
		tx.Exists("k")
		return nil
	}
	if index, err := propose(7, read); err != nil || index != 0 {
		t.Fatalf("a read of committed state depends on %d, %v; want 0", index, err)
	}
	if index, _ := propose(7, func(tx *Tx) error { tx.Put("k", []byte("v"), 0); return nil }); index != 11 {
		t.Fatalf("a published write depends on %d, want its own proposal 11", index)
	}
	if index, err := propose(7, read); err != nil || index != 11 {
		t.Fatalf("a no-op write after a proposal depends on %d, %v; want 11", index, err)
	}
	failed := errors.New("wrong type")
	if index, err := propose(7, func(tx *Tx) error { tx.Exists("k"); return failed }); err != failed || index != 11 {
		t.Fatalf("a failed write that read a proposal depends on %d, %v; want 11", index, err)
	}
	if index, _ := propose(8, read); index != 0 {
		t.Fatalf("a writer of another term depends on %d, want 0", index)
	}
	if err := db.Apply([]Op{{Key: "k", Value: []byte("v")}}, 11); err != nil {
		t.Fatal(err)
	}
	if index, _ := propose(7, read); index != 0 {
		t.Fatalf("after the proposal is applied a no-op write depends on %d, want 0", index)
	}
}

func FuzzScanner(f *testing.F) {
	valid := appendRecord(nil, 0, 0, "key", 0, []byte("value"))
	valid = appendRecord(valid, flagTombstone, 0, "key", 0, nil)
	valid = appendRecord(valid, 0, 0, "hash", 1, []byte("fields"))
	valid = appendTxHeader(valid, 7, 2)
	valid = appendControl(valid, flagMark, 42)
	f.Add(valid)
	f.Add(valid[:len(valid)-3])
	f.Fuzz(func(t *testing.T, data []byte) {
		s := newScanner(bytes.NewReader(data), int64(len(data)))
		for {
			r, err := s.next()
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, errTorn) {
					t.Fatalf("scan failed with %v", err)
				}
				return
			}
			if got := appendRecord(nil, r.flags, r.expireAt, string(r.key), 0, r.value); !bytes.Equal(got, data[r.offset:s.offset]) {
				t.Fatalf("record at %d encodes to %x, the file has %x", r.offset, got, data[r.offset:s.offset])
			}
		}
	})
}
