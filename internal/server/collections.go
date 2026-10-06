package server

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"

	"github.com/acneism/casketdb/internal/bitcask"
)

const (
	maxRandomCount  = 1 << 24
	maxListpackLen  = 128
	maxListpackItem = 64
)

type listpack []byte

func (l listpack) next() (field, value []byte, rest listpack, ok bool) {
	n, k := binary.Uvarint(l)
	if k <= 0 || n > uint64(len(l)-k) {
		return nil, nil, nil, false
	}
	field, l = l[k:k+int(n)], l[k+int(n):]
	n, k = binary.Uvarint(l)
	if k <= 0 || n > uint64(len(l)-k) {
		return nil, nil, nil, false
	}
	return field, l[k : k+int(n)], l[k+int(n):], true
}

func (l listpack) each(fn func(field, value []byte)) {
	for rest := l; ; {
		field, value, next, ok := rest.next()
		if !ok {
			return
		}
		fn(field, value)
		rest = next
	}
}

func (l listpack) len() int {
	n := 0
	l.each(func(_, _ []byte) { n++ })
	return n
}

func (l listpack) find(field []byte) (start, end int, value []byte, ok bool) {
	for rest := l; ; {
		f, v, next, more := rest.next()
		if !more {
			return len(l), len(l), nil, false
		}
		if bytes.Equal(f, field) {
			return len(l) - len(rest), len(l) - len(next), v, true
		}
		rest = next
	}
}

func (l listpack) put(field, value []byte) (listpack, bool) {
	start, end, _, found := l.find(field)
	out := make(listpack, 0, len(l)-(end-start)+2*binary.MaxVarintLen64+len(field)+len(value))
	out = append(out, l[:start]...)
	out = binary.AppendUvarint(out, uint64(len(field)))
	out = append(out, field...)
	out = binary.AppendUvarint(out, uint64(len(value)))
	out = append(out, value...)
	return append(out, l[end:]...), !found
}

func (l listpack) del(field []byte) (listpack, bool) {
	start, end, _, found := l.find(field)
	if !found {
		return l, false
	}
	return append(append(make(listpack, 0, len(l)-(end-start)), l[:start]...), l[end:]...), true
}

type collection struct {
	tx    *bitcask.Tx
	key   string
	typ   bitcask.Kind
	blob  listpack
	table bool
	gen   uint64
	count int
	dirty bool
}

func openCollection(tx *bitcask.Tx, key []byte, typ bitcask.Kind) (*collection, reply, error) {
	value, kind, found, err := tx.GetKind(string(key))
	if err != nil {
		return nil, nil, err
	}
	c := &collection{tx: tx, key: string(key), typ: typ}
	switch {
	case !found:
	case kind == typ:
		c.blob = value
	case kind == c.tableKind() && len(value) >= 8:
		n, _ := binary.Uvarint(value[8:])
		c.table, c.gen, c.count = true, binary.LittleEndian.Uint64(value), int(n)
	default:
		return nil, errorReply(errWrongType), nil
	}
	return c, nil, nil
}

func (c *collection) tableKind() bitcask.Kind {
	if c.typ == typeZSet {
		return c.typ | bitcask.Table | bitcask.Ordered
	}
	return c.typ | bitcask.Table
}

func (c *collection) len() int {
	if c.table {
		return c.count
	}
	return c.blob.len()
}

func (c *collection) get(field []byte) ([]byte, bool, error) {
	if c.table {
		return c.tx.GetMember(c.key, string(field))
	}
	_, _, value, ok := c.blob.find(field)
	return value, ok, nil
}

func (c *collection) has(field []byte) (bool, error) {
	if c.table {
		return c.tx.HasMember(c.key, string(field))
	}
	_, _, _, ok := c.blob.find(field)
	return ok, nil
}

func (c *collection) set(field, value []byte) (bool, error) {
	c.dirty = true
	if !c.table {
		var added bool
		c.blob, added = c.blob.put(field, value)
		if len(field) > maxListpackItem || len(value) > maxListpackItem || (added && c.blob.len() > maxListpackLen) {
			c.convert()
		}
		return added, nil
	}
	_, found, err := c.tx.GetMember(c.key, string(field))
	if err != nil {
		return false, err
	}
	c.tx.PutMember(c.key, string(field), value)
	if !found {
		c.count++
	}
	return !found, nil
}

func (c *collection) del(field []byte) bool {
	var deleted bool
	if c.table {
		if deleted = c.tx.DeleteMember(c.key, string(field)); deleted {
			c.count--
		}
	} else {
		c.blob, deleted = c.blob.del(field)
	}
	c.dirty = c.dirty || deleted
	return deleted
}

func (c *collection) each(values bool, fn func(field, value []byte)) error {
	if c.table {
		return c.tx.Members(c.key, values, func(field string, value []byte) bool {
			fn([]byte(field), value)
			return true
		})
	}
	c.blob.each(fn)
	return nil
}

func (c *collection) random(count int64, values bool) ([][]byte, [][]byte, error) {
	var fields, vals [][]byte
	if !c.table || (count > 0 && count*2 >= int64(c.len())) {
		var all, allValues [][]byte
		if err := c.each(values, func(field, value []byte) {
			all, allValues = append(all, field), append(allValues, value)
		}); err != nil {
			return nil, nil, err
		}
		for _, i := range randomPicks(len(all), count) {
			fields, vals = append(fields, all[i]), append(vals, allValues[i])
		}
		return fields, vals, nil
	}
	picked := make(map[string]bool)
	skip := func(member string) bool { return count > 0 && picked[member] }
	for range max(count, -count) {
		member, value, ok, err := c.tx.RandomMember(c.key, values, skip)
		if err != nil || !ok {
			return fields, vals, err
		}
		picked[member] = true
		fields, vals = append(fields, []byte(member)), append(vals, value)
	}
	return fields, vals, nil
}

func (c *collection) meta() []byte {
	return binary.AppendUvarint(binary.LittleEndian.AppendUint64(nil, c.gen), uint64(c.count))
}

func newGen() uint64 {
	for {
		if gen := rand.Uint64(); gen != 0 {
			return gen
		}
	}
}

func (c *collection) convert() {
	c.gen = newGen()
	expireAt, _ := c.tx.ExpireAt(c.key)
	c.table = true
	c.tx.PutKind(c.key, c.tableKind(), c.meta(), expireAt)
	c.blob.each(func(field, value []byte) {
		c.tx.PutMember(c.key, string(field), value)
		c.count++
	})
	c.blob = nil
}

func (c *collection) store() {
	if !c.dirty {
		return
	}
	expireAt, _ := c.tx.ExpireAt(c.key)
	switch {
	case c.len() == 0:
		c.tx.Delete(c.key)
	case c.table:
		c.tx.PutKind(c.key, c.tableKind(), c.meta(), expireAt)
	default:
		c.tx.PutKind(c.key, c.typ, c.blob, expireAt)
	}
}

type mpop struct {
	keys  [][]byte
	first bool
	count int64
}

func parseMPop(args [][]byte, numIndex int, first, last string) (mpop, reply) {
	numkeys, ok := parseInt(args[numIndex])
	if !ok || numkeys < 1 {
		return mpop{}, errorReply("ERR numkeys should be greater than 0")
	}
	if numkeys > int64(len(args)-numIndex-2) {
		return mpop{}, errorReply(errSyntax)
	}
	where := numIndex + 1 + int(numkeys)
	p := mpop{keys: args[numIndex+1 : where], count: 1}
	switch upper(args[where]) {
	case first:
		p.first = true
	case last:
	default:
		return mpop{}, errorReply(errSyntax)
	}
	for j, counted := where+1, false; j < len(args); j, counted = j+2, true {
		if counted || upper(args[j]) != "COUNT" || j+1 == len(args) {
			return mpop{}, errorReply(errSyntax)
		}
		if p.count, ok = parseInt(args[j+1]); !ok || p.count < 1 {
			return mpop{}, errorReply("ERR count should be greater than 0")
		}
	}
	return p, nil
}

func randomPicks(n int, count int64) []int {
	switch {
	case n == 0:
		return nil
	case count < 0:
		picks := make([]int, -count)
		for i := range picks {
			picks[i] = rand.IntN(n)
		}
		return picks
	}
	return rand.Perm(n)[:min(int(count), n)]
}
