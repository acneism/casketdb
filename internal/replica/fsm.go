package replica

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

const (
	kindOps      byte = 1
	kindFlush    byte = 2
	kindSystem   byte = 3
	kindTypedOps byte = 4
	kindPublish  byte = 5
	flagDelete   byte = 1
	flagMember   byte = 2

	restoreBatch = 1024
	restoreDir   = "restore"
	snapshotInfo = "casketdb-snapshot"
)

var (
	errEntry    = errors.New("replica: malformed log entry")
	errSnapshot = errors.New("replica: malformed snapshot")
)

type bitcaskFSM struct {
	db      *bitcask.DB
	dir     string
	applied atomic.Uint64
	first   atomic.Uint64
	publish atomic.Pointer[func(channel, message []byte, shard bool) int]
	waiters sync.Map
}

func newBitcaskFSM(db *bitcask.DB, dir string) (*bitcaskFSM, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f := &bitcaskFSM{db: db, dir: dir}
	f.applied.Store(db.DurableIndex())
	return f, nil
}

func (f *bitcaskFSM) Apply(ents []raft.Entry) error {
	var ops []bitcask.Op
	var upTo uint64
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		err := f.db.Apply(ops, upTo)
		ops, upTo = ops[:0], 0
		return err
	}
	for _, e := range ents {
		if e.Type != raft.EntryNormal {
			continue
		}
		if len(e.Data) == 0 {
			return errEntry
		}
		switch e.Data[0] {
		case kindOps, kindTypedOps:
			decoded, err := decodeOps(bytes.NewReader(e.Data[1:]), e.Data[0] == kindTypedOps)
			if err != nil {
				return err
			}
			ops = append(ops, decoded...)
			upTo = e.Index
		case kindFlush:
			if err := flush(); err != nil {
				return err
			}
			if err := f.db.Flush(); err != nil {
				return err
			}
		case kindSystem:
			if err := flush(); err != nil {
				return err
			}
			if err := f.db.SetSystem(e.Data[1:]); err != nil {
				return err
			}
		case kindPublish:
			if err := flush(); err != nil {
				return err
			}
			if err := f.deliver(e.Data[1:]); err != nil {
				return err
			}
		default:
			return errEntry
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if n := len(ents); n > 0 {
		f.first.CompareAndSwap(0, ents[0].Index)
		f.db.MarkApplied(ents[n-1].Index)
		f.applied.Store(ents[n-1].Index)
	}
	return nil
}

func encodePublish(nonce uint64, channel, message []byte, shard bool) []byte {
	b := []byte{kindPublish, 0}
	if shard {
		b[1] = 1
	}
	b = binary.LittleEndian.AppendUint64(b, nonce)
	b = binary.AppendUvarint(b, uint64(len(channel)))
	return append(append(b, channel...), message...)
}

func (f *bitcaskFSM) deliver(b []byte) error {
	if len(b) < 9 {
		return errEntry
	}
	shard, nonce := b[0] == 1, binary.LittleEndian.Uint64(b[1:])
	r := bytes.NewReader(b[9:])
	channel, err := readField(r)
	if err != nil {
		return errEntry
	}
	message := b[len(b)-r.Len():]
	n := 0
	if fn := f.publish.Load(); fn != nil {
		n = (*fn)(channel, message, shard)
	}
	if w, ok := f.waiters.LoadAndDelete(nonce); ok {
		w.(chan int) <- n
	}
	return nil
}

func (f *bitcaskFSM) DurableIndex() uint64 { return f.db.DurableIndex() }

func (f *bitcaskFSM) Snapshot(dir string) (raft.SnapshotMeta, error) {
	index := f.applied.Load()
	logs, files, err := f.db.LinkFiles(dir)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	info := appendFileList(nil, logs, len(files))
	for _, sf := range files {
		info = appendFileEntry(info, sf)
	}
	if err := writeSync(filepath.Join(dir, snapshotInfo), info); err != nil {
		return raft.SnapshotMeta{}, err
	}
	return raft.SnapshotMeta{Index: index}, nil
}

func (f *bitcaskFSM) Restore(src node.SnapshotSource) error {
	info, err := os.ReadFile(filepath.Join(src.Dir, snapshotInfo))
	if err != nil {
		return err
	}
	logs, files, err := readFileList(bytes.NewReader(info))
	if err != nil {
		return err
	}
	tmp := filepath.Join(f.dir, restoreDir)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for _, sf := range files {
		if err := bitcask.CopyPrefix(filepath.Join(src.Dir, filepath.FromSlash(sf.Path)), filepath.Join(tmp, filepath.FromSlash(sf.Path)), sf.Size); err != nil {
			return err
		}
	}
	opts := bitcask.DefaultOptions()
	opts.Logs = logs
	opts.Sync = bitcask.SyncNo
	opts.MergeInterval = 0
	opts.ExpireInterval = 0
	opts.Now = f.db.Options().Now
	from, err := bitcask.Open(tmp, opts)
	if err != nil {
		return err
	}
	defer from.Close()
	if err := f.db.Flush(); err != nil {
		return err
	}
	err = from.CopyTo(f.db, restoreBatch)
	if err == nil {
		f.db.MarkApplied(src.Meta.Index)
		err = f.db.Sync()
	}
	if err != nil {
		return err
	}
	f.applied.Store(src.Meta.Index)
	return nil
}

func encodeEntry(kind byte, ops []bitcask.Op) []byte {
	if kind == kindOps && slices.ContainsFunc(ops, func(op bitcask.Op) bool { return op.Kind != 0 || op.IsMember }) {
		kind = kindTypedOps
	}
	size := 1
	for _, op := range ops {
		size += 2 + 4*binary.MaxVarintLen64 + len(op.Key) + len(op.Member) + len(op.Value)
	}
	b := append(make([]byte, 0, size), kind)
	for _, op := range ops {
		b = appendOp(b, op, kind == kindTypedOps)
	}
	return b
}

func decodeOps(body *bytes.Reader, typed bool) ([]bitcask.Op, error) {
	var ops []bitcask.Op
	for body.Len() > 0 {
		op, err := readOp(body, typed)
		if err != nil {
			return nil, errEntry
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func appendOp(b []byte, op bitcask.Op, typed bool) []byte {
	var flags byte
	if op.Delete {
		flags = flagDelete
	}
	if op.IsMember {
		flags |= flagMember
	}
	b = append(b, flags)
	if typed {
		b = append(b, byte(op.Kind))
	}
	b = binary.AppendVarint(b, op.ExpireAt)
	b = binary.AppendUvarint(b, uint64(len(op.Key)))
	b = append(b, op.Key...)
	if op.IsMember {
		b = binary.AppendUvarint(b, uint64(len(op.Member)))
		b = append(b, op.Member...)
	}
	b = binary.AppendUvarint(b, uint64(len(op.Value)))
	return append(b, op.Value...)
}

func readOp(r *bytes.Reader, typed bool) (bitcask.Op, error) {
	flags, err := r.ReadByte()
	if err != nil {
		return bitcask.Op{}, err
	}
	op := bitcask.Op{Delete: flags&flagDelete != 0, IsMember: typed && flags&flagMember != 0}
	if typed {
		kind, err := r.ReadByte()
		if err != nil {
			return op, unexpected(err)
		}
		op.Kind = bitcask.Kind(kind)
	}
	if op.ExpireAt, err = binary.ReadVarint(r); err != nil {
		return op, unexpected(err)
	}
	key, err := readField(r)
	if err != nil {
		return op, err
	}
	op.Key = string(key)
	if op.IsMember {
		member, err := readField(r)
		if err != nil {
			return op, err
		}
		op.Member = string(member)
	}
	op.Value, err = readField(r)
	return op, err
}

func readField(r *bytes.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, unexpected(err)
	}
	if n > uint64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	return b, unexpected(err)
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func appendFileList(b []byte, logs, count int) []byte {
	b = binary.AppendUvarint(b, uint64(logs))
	return binary.AppendUvarint(b, uint64(count))
}

func appendFileEntry(b []byte, sf bitcask.SnapshotFile) []byte {
	b = binary.AppendUvarint(b, uint64(len(sf.Path)))
	b = append(b, sf.Path...)
	return binary.AppendUvarint(b, uint64(sf.Size))
}

func readFileList(r *bytes.Reader) (int, []bitcask.SnapshotFile, error) {
	logs, err := binary.ReadUvarint(r)
	if err != nil || logs == 0 || logs > 1<<16 {
		return 0, nil, errSnapshot
	}
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, errSnapshot
	}
	var files []bitcask.SnapshotFile
	for range count {
		path, err := readField(r)
		if err != nil {
			return 0, nil, errSnapshot
		}
		size, err := binary.ReadUvarint(r)
		if err != nil || int64(size) < 0 || !filepath.IsLocal(filepath.FromSlash(string(path))) {
			return 0, nil, errSnapshot
		}
		files = append(files, bitcask.SnapshotFile{Path: string(path), Size: int64(size)})
	}
	return int(logs), files, nil
}

func writeSync(path string, b []byte) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = file.Write(b)
	if err == nil {
		err = file.Sync()
	}
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	return err
}
