package server

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
)

const (
	streamKind  = typeStream | bitcask.Table | bitcask.Ordered | bitcask.ByMember
	errStreamID = "ERR Invalid stream ID specified as stream command argument"
	errNoKey    = "ERR no such key"
)

const (
	trimMaxLen = iota + 1
	trimMinID
)

var streamCommands = map[string]command{
	"xadd":      {arity: -5, kind: kindWrite, keys: oneKey, acl: catStream | catFast, tx: cmdXAdd},
	"xlen":      {arity: 2, kind: kindRead, keys: oneKey, acl: catStream | catFast, tx: cmdXLen},
	"xrange":    {arity: -4, kind: kindRead, keys: oneKey, acl: catStream, tx: xrange(false)},
	"xrevrange": {arity: -4, kind: kindRead, keys: oneKey, acl: catStream, tx: xrange(true)},
	"xdel":      {arity: -3, kind: kindWrite, keys: oneKey, acl: catStream | catFast, tx: cmdXDel},
	"xtrim":     {arity: -4, kind: kindWrite, keys: oneKey, acl: catStream, tx: cmdXTrim},
	"xsetid":    {arity: -3, kind: kindWrite, keys: oneKey, acl: catStream | catFast, tx: cmdXSetID},
	"xread":     {arity: -4, kind: kindRead, keys: keySpec{streams: true}, acl: catStream | catBlocking, tx: cmdXRead, blocks: true},
	"xinfo":     {arity: -2, kind: kindRead, keys: keySpec{first: 2, last: 2, step: 1}, acl: catStream, tx: cmdXInfo},
}

type streamID struct{ ms, seq uint64 }

var maxStreamID = streamID{math.MaxUint64, math.MaxUint64}

func (a streamID) less(b streamID) bool {
	return a.ms < b.ms || a.ms == b.ms && a.seq < b.seq
}

func (a streamID) String() string {
	return strconv.FormatUint(a.ms, 10) + "-" + strconv.FormatUint(a.seq, 10)
}

func (a streamID) reply() reply { return bulkReply(a.String()) }

func (a streamID) next() (streamID, bool) {
	switch {
	case a.seq < math.MaxUint64:
		return streamID{a.ms, a.seq + 1}, true
	case a.ms < math.MaxUint64:
		return streamID{a.ms + 1, 0}, true
	}
	return streamID{}, false
}

func (a streamID) prev() (streamID, bool) {
	switch {
	case a.seq > 0:
		return streamID{a.ms, a.seq - 1}, true
	case a.ms > 0:
		return streamID{a.ms - 1, math.MaxUint64}, true
	}
	return maxStreamID, false
}

func parseStreamID(arg []byte, missingSeq uint64, strict, autoSeq bool) (id streamID, seqGiven, ok bool) {
	s := string(arg)
	switch {
	case len(s) > 127:
		return streamID{}, false, false
	case s == "-":
		return streamID{}, true, !strict
	case s == "+":
		return maxStreamID, true, !strict
	}
	msPart, seqPart, dash := strings.Cut(s, "-")
	ms, err := strconv.ParseUint(msPart, 10, 64)
	switch {
	case err != nil:
		return streamID{}, false, false
	case !dash:
		return streamID{ms, missingSeq}, true, true
	case autoSeq && seqPart == "*":
		return streamID{ms, 0}, false, true
	}
	seq, err := strconv.ParseUint(seqPart, 10, 64)
	return streamID{ms, seq}, true, err == nil
}

func parseIntervalID(arg []byte, missingSeq uint64) (streamID, bool, bool) {
	if len(arg) > 1 && arg[0] == '(' {
		id, _, ok := parseStreamID(arg[1:], missingSeq, true, false)
		return id, true, ok
	}
	id, _, ok := parseStreamID(arg, missingSeq, false, false)
	return id, false, ok
}

func idBytes(id streamID) string {
	return string(binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, id.ms), id.seq))
}

func entryMember(id streamID) string {
	return "e" + idBytes(id)
}

func memberID(member string) streamID {
	b := []byte(member[len(member)-16:])
	return streamID{binary.BigEndian.Uint64(b), binary.BigEndian.Uint64(b[8:])}
}

func encodeFields(fields [][]byte) []byte {
	out := binary.AppendUvarint(nil, uint64(len(fields)/2))
	for _, f := range fields {
		out = binary.AppendUvarint(out, uint64(len(f)))
		out = append(out, f...)
	}
	return out
}

func fieldsReply(v []byte) arrayReply {
	n, k := binary.Uvarint(v)
	if k <= 0 {
		return arrayReply{}
	}
	v = v[k:]
	out := make(arrayReply, 0, 2*n)
	for len(v) > 0 {
		size, k := binary.Uvarint(v)
		if k <= 0 || size > uint64(len(v)-k) {
			break
		}
		out = append(out, bulkReply(v[k:k+int(size)]))
		v = v[k+int(size):]
	}
	return out
}

type stream struct {
	tx         *bitcask.Tx
	key        string
	exists     bool
	gen        uint64
	length     uint64
	last       streamID
	maxDeleted streamID
	added      uint64
	groups     []*streamGroup
	first      *streamID
}

func openStream(tx *bitcask.Tx, key []byte) (*stream, reply, error) {
	value, kind, found, err := tx.GetKind(string(key))
	if err != nil {
		return nil, nil, err
	}
	s := &stream{tx: tx, key: string(key)}
	if !found {
		return s, nil, nil
	}
	if kind != streamKind || len(value) < 8 {
		return nil, errorReply(errWrongType), nil
	}
	s.exists, s.gen = true, binary.LittleEndian.Uint64(value)
	rest := value[8:]
	read := func() uint64 {
		v, k := binary.Uvarint(rest)
		if k > 0 {
			rest = rest[k:]
		}
		return v
	}
	s.length = read()
	s.last = streamID{read(), read()}
	s.maxDeleted = streamID{read(), read()}
	s.added = read()
	for n := read(); n > 0; n-- {
		size := read()
		if size > uint64(len(rest)) {
			break
		}
		g := &streamGroup{name: string(rest[:size])}
		rest = rest[size:]
		g.last = streamID{read(), read()}
		g.entriesRead = int64(read()) - 1
		g.pending, g.consumers = read(), read()
		s.groups = append(s.groups, g)
	}
	return s, nil, nil
}

func (s *stream) store() {
	if !s.exists {
		s.exists, s.gen = true, newGen()
	}
	v := binary.LittleEndian.AppendUint64(nil, s.gen)
	for _, n := range []uint64{s.length, s.last.ms, s.last.seq, s.maxDeleted.ms, s.maxDeleted.seq, s.added, uint64(len(s.groups))} {
		v = binary.AppendUvarint(v, n)
	}
	for _, g := range s.groups {
		v = binary.AppendUvarint(v, uint64(len(g.name)))
		v = append(v, g.name...)
		for _, n := range []uint64{g.last.ms, g.last.seq, uint64(g.entriesRead + 1), g.pending, g.consumers} {
			v = binary.AppendUvarint(v, n)
		}
	}
	expireAt, _ := s.tx.ExpireAt(s.key)
	s.tx.PutKind(s.key, streamKind, v, expireAt)
}

func (s *stream) scan(from string, fn func(member string) bool) error {
	start, err := s.tx.MemberCount(s.key, func(_ []byte, m string) bool { return m < from })
	if err != nil {
		return err
	}
	return s.tx.MemberRange(s.key, start, false, func(m string, _ []byte) bool { return fn(m) })
}

func (s *stream) idRange(prefix string, from, to streamID, count int64, reverse bool) ([]streamID, error) {
	if !s.exists || to.less(from) {
		return nil, nil
	}
	lo, hi := prefix+idBytes(from), prefix+idBytes(to)
	var out []streamID
	collect := func(m string) bool {
		if m < lo || m > hi {
			return false
		}
		out = append(out, memberID(m))
		return count <= 0 || int64(len(out)) < count
	}
	if !reverse {
		return out, s.scan(lo, collect)
	}
	start, err := s.tx.MemberCount(s.key, func(_ []byte, m string) bool { return m <= hi })
	if err != nil {
		return nil, err
	}
	err = s.tx.MemberRange(s.key, start-1, true, func(m string, _ []byte) bool { return collect(m) })
	return out, err
}

func (s *stream) ids(from, to streamID, count int64, reverse bool) ([]streamID, error) {
	return s.idRange("e", from, to, count, reverse)
}

func (s *stream) entry(id streamID) (reply, error) {
	v, found, err := s.tx.GetMember(s.key, entryMember(id))
	if err != nil || !found {
		return nilReply, err
	}
	return arrayReply{id.reply(), fieldsReply(v)}, nil
}

func (s *stream) entries(ids []streamID) (arrayReply, error) {
	out := make(arrayReply, len(ids))
	for i, id := range ids {
		var err error
		if out[i], err = s.entry(id); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *stream) edge(last bool) (streamID, bool, error) {
	ids, err := s.ids(streamID{}, maxStreamID, 1, last)
	if err != nil || len(ids) == 0 {
		return streamID{}, false, err
	}
	return ids[0], true, nil
}

type trimSpec struct {
	strategy int
	approx   bool
	maxLen   int64
	minID    streamID
	limit    int64
}

func (s *stream) trim(t trimSpec) (int64, error) {
	count, to := int64(0), maxStreamID
	switch t.strategy {
	case trimMaxLen:
		if s.length <= uint64(t.maxLen) {
			return 0, nil
		}
		count = int64(s.length - uint64(t.maxLen))
	case trimMinID:
		var ok bool
		if to, ok = t.minID.prev(); !ok {
			return 0, nil
		}
	}
	if t.limit > 0 && (count == 0 || t.limit < count) {
		count = t.limit
	}
	ids, err := s.ids(streamID{}, to, count, false)
	for _, id := range ids {
		s.tx.DeleteMember(s.key, entryMember(id))
	}
	s.length -= uint64(len(ids))
	s.first = nil
	return int64(len(ids)), err
}

func (s *stream) firstID() (streamID, error) {
	if s.first == nil {
		id, _, err := s.edge(false)
		if err != nil {
			return streamID{}, err
		}
		s.first = &id
	}
	return *s.first, nil
}

type addArgs struct {
	trimSpec
	noMkStream bool
	id         streamID
	idGiven    bool
	seqGiven   bool
}

func parseAddOrTrim(args [][]byte, xadd bool) (addArgs, int, reply) {
	var a addArgs
	limitGiven := false
	i := 2
options:
	for ; i < len(args); i++ {
		more := len(args) - 1 - i
		switch opt := upper(args[i]); {
		case xadd && opt == "*":
			break options
		case (opt == "MAXLEN" || opt == "MINID") && more > 0:
			if a.strategy != 0 {
				return a, 0, errorReply("ERR syntax error, MAXLEN and MINID options at the same time are not compatible")
			}
			a.approx = false
			if more >= 2 && (string(args[i+1]) == "~" || string(args[i+1]) == "=") {
				a.approx = string(args[i+1]) == "~"
				i++
			}
			i++
			if opt == "MAXLEN" {
				n, ok := parseInt(args[i])
				switch {
				case !ok:
					return a, 0, errorReply(errNotInteger)
				case n < 0:
					return a, 0, errorReply("ERR The MAXLEN argument must be >= 0.")
				}
				a.strategy, a.maxLen = trimMaxLen, n
			} else {
				id, _, ok := parseStreamID(args[i], 0, true, false)
				if !ok {
					return a, 0, errorReply(errStreamID)
				}
				a.strategy, a.minID = trimMinID, id
			}
		case opt == "LIMIT" && more > 0:
			n, ok := parseInt(args[i+1])
			switch {
			case !ok:
				return a, 0, errorReply(errNotInteger)
			case n < 0:
				return a, 0, errorReply("ERR The LIMIT argument must be >= 0.")
			}
			a.limit, limitGiven = n, true
			i++
		case xadd && opt == "NOMKSTREAM":
			a.noMkStream = true
		case xadd:
			id, seqGiven, ok := parseStreamID(args[i], 0, true, true)
			if !ok {
				return a, 0, errorReply(errStreamID)
			}
			a.id, a.idGiven, a.seqGiven = id, true, seqGiven
			break options
		default:
			return a, 0, errorReply(errSyntax)
		}
	}
	switch {
	case limitGiven && a.strategy == 0:
		return a, 0, errorReply("ERR syntax error, LIMIT cannot be used without specifying a trimming strategy")
	case !xadd && a.strategy == 0:
		return a, 0, errorReply("ERR syntax error, XTRIM must be called with a trimming strategy")
	case limitGiven && !a.approx:
		return a, 0, errorReply("ERR syntax error, LIMIT cannot be used without the special ~ option")
	case !limitGiven && a.approx:
		a.limit = 10000
	case !limitGiven:
		a.limit = 0
	}
	return a, i, nil
}

func (s *stream) nextID(a addArgs, now int64) (streamID, bool) {
	var id streamID
	switch {
	case a.idGiven && a.seqGiven:
		id = a.id
	case a.idGiven && a.id.ms == s.last.ms:
		if s.last.seq == math.MaxUint64 {
			return streamID{}, false
		}
		id = streamID{s.last.ms, s.last.seq + 1}
	case a.idGiven:
		id = a.id
	case uint64(max(now, 0)) > s.last.ms:
		id = streamID{uint64(now), 0}
	default:
		var ok bool
		if id, ok = s.last.next(); !ok {
			return streamID{}, false
		}
	}
	return id, s.last.less(id)
}

func cmdXAdd(tx *bitcask.Tx, args [][]byte) (reply, error) {
	a, i, bad := parseAddOrTrim(args, true)
	if bad != nil {
		return bad, nil
	}
	fields := args[min(i+1, len(args)):]
	if len(fields) < 2 || len(fields)%2 == 1 {
		return errorReply("ERR wrong number of arguments for 'xadd' command"), nil
	}
	if a.idGiven && a.seqGiven && a.id == (streamID{}) {
		return errorReply("ERR The ID specified in XADD must be greater than 0-0"), nil
	}
	s, bad, err := openStream(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if !s.exists {
		if a.noMkStream {
			return nilReply, nil
		}
		s.store()
	}
	if s.last == maxStreamID {
		return errorReply("ERR The stream has exhausted the last possible ID, unable to add more items"), nil
	}
	id, ok := s.nextID(a, tx.Now())
	if !ok {
		return errorReply("ERR The ID specified in XADD is equal or smaller than the target stream top item"), nil
	}
	tx.PutMember(s.key, entryMember(id), encodeFields(fields))
	s.length++
	s.last = id
	s.added++
	if a.strategy != 0 {
		if _, err := s.trim(a.trimSpec); err != nil {
			return nil, err
		}
	}
	s.store()
	return id.reply(), nil
}

func cmdXLen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openStream(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(int64(s.length)), nil
}

func xrange(reverse bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		startArg, endArg := args[2], args[3]
		if reverse {
			startArg, endArg = args[3], args[2]
		}
		start, end, bad := parseRangeIDs(startArg, endArg)
		if bad != nil {
			return bad, nil
		}
		count := int64(-1)
		for j := 4; j < len(args); j += 2 {
			if upper(args[j]) != "COUNT" || j+1 >= len(args) {
				return errorReply(errSyntax), nil
			}
			n, ok := parseInt(args[j+1])
			if !ok {
				return errorReply(errNotInteger), nil
			}
			count = max(n, 0)
		}
		s, bad, err := openStream(tx, args[1])
		switch {
		case bad != nil || err != nil:
			return bad, err
		case !s.exists:
			return arrayReply{}, nil
		case count == 0:
			return nullArrayReply{}, nil
		}
		ids, err := s.ids(start, end, count, reverse)
		if err != nil {
			return nil, err
		}
		return s.entries(ids)
	}
}

func cmdXDel(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openStream(tx, args[1])
	if bad != nil || err != nil || !s.exists {
		return cmpReply(bad, intReply(0)), err
	}
	ids := make([]streamID, len(args)-2)
	for i, arg := range args[2:] {
		var ok bool
		if ids[i], _, ok = parseStreamID(arg, 0, true, false); !ok {
			return errorReply(errStreamID), nil
		}
	}
	deleted := 0
	for _, id := range ids {
		if tx.DeleteMember(s.key, entryMember(id)) {
			deleted++
			s.length--
			s.first = nil
			if s.maxDeleted.less(id) {
				s.maxDeleted = id
			}
		}
	}
	if deleted > 0 {
		s.store()
	}
	return intReply(deleted), nil
}

func cmdXTrim(tx *bitcask.Tx, args [][]byte) (reply, error) {
	a, _, bad := parseAddOrTrim(args, false)
	if bad != nil {
		return bad, nil
	}
	s, bad, err := openStream(tx, args[1])
	if bad != nil || err != nil || !s.exists {
		return cmpReply(bad, intReply(0)), err
	}
	n, err := s.trim(a.trimSpec)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		s.store()
	}
	return intReply(n), nil
}

func cmdXSetID(tx *bitcask.Tx, args [][]byte) (reply, error) {
	id, _, ok := parseStreamID(args[2], 0, true, false)
	if !ok {
		return errorReply(errStreamID), nil
	}
	added, maxDeleted := int64(-1), streamID{}
	for i := 3; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return errorReply(errSyntax), nil
		}
		switch upper(args[i]) {
		case "ENTRIESADDED":
			if added, ok = parseInt(args[i+1]); !ok {
				return errorReply(errNotInteger), nil
			}
			if added < 0 {
				return errorReply("ERR entries_added must be positive"), nil
			}
		case "MAXDELETEDID":
			if maxDeleted, _, ok = parseStreamID(args[i+1], 0, true, false); !ok {
				return errorReply(errStreamID), nil
			}
			if id.less(maxDeleted) {
				return errorReply("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id"), nil
			}
		default:
			return errorReply(errSyntax), nil
		}
	}
	s, bad, err := openStream(tx, args[1])
	switch {
	case bad != nil || err != nil:
		return bad, err
	case !s.exists:
		return errorReply(errNoKey), nil
	case id.less(s.maxDeleted):
		return errorReply("ERR The ID specified in XSETID is smaller than current max_deleted_entry_id"), nil
	}
	if s.length > 0 {
		top, _, err := s.edge(true)
		switch {
		case err != nil:
			return nil, err
		case id.less(top):
			return errorReply("ERR The ID specified in XSETID is smaller than the target stream top item"), nil
		case added != -1 && s.length > uint64(added):
			return errorReply("ERR The entries_added specified in XSETID is smaller than the target stream length"), nil
		}
	}
	s.last = id
	if added != -1 {
		s.added = uint64(added)
	}
	if maxDeleted != (streamID{}) {
		s.maxDeleted = maxDeleted
	}
	s.store()
	return statusReply("OK"), nil
}

type readArgs struct {
	count           int64
	timeout         time.Duration
	group, consumer []byte
	noack           bool
	at              int
	keys, ids       [][]byte
}

func parseRead(args [][]byte, grouped bool) (readArgs, reply) {
	r := readArgs{timeout: -1}
	name, symbol := "xread", "$"
	if grouped {
		name, symbol = "xreadgroup", ">"
	}
	for i := 1; i < len(args) && r.at == 0; i++ {
		more := len(args) - i - 1
		switch opt := upper(args[i]); {
		case opt == "BLOCK" && more > 0:
			i++
			ms, ok := parseInt(args[i])
			switch {
			case !ok:
				return r, errorReply("ERR timeout is not an integer or out of range")
			case ms < 0:
				return r, errorReply("ERR timeout is negative")
			}
			r.timeout = time.Duration(min(ms, math.MaxInt64/int64(time.Millisecond))) * time.Millisecond
		case opt == "COUNT" && more > 0:
			i++
			n, ok := parseInt(args[i])
			if !ok {
				return r, errorReply(errNotInteger)
			}
			r.count = max(n, 0)
		case opt == "STREAMS" && more > 0:
			if more%2 != 0 {
				return r, errorReply("ERR Unbalanced '" + name + "' list of streams: for each stream key an ID or '" + symbol + "' must be specified.")
			}
			r.at = i + 1
		case opt == "GROUP" && more >= 2:
			if !grouped {
				return r, errorReply("ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.")
			}
			r.group, r.consumer = args[i+1], args[i+2]
			i += 2
		case opt == "NOACK":
			if !grouped {
				return r, errorReply("ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.")
			}
			r.noack = true
		default:
			return r, errorReply(errSyntax)
		}
	}
	switch {
	case r.at == 0:
		return r, errorReply(errSyntax)
	case grouped && r.group == nil:
		return r, errorReply("ERR Missing GROUP option for XREADGROUP")
	}
	n := (len(args) - r.at) / 2
	r.keys, r.ids = args[r.at:r.at+n], args[r.at+n:]
	return r, nil
}

func cmdXRead(tx *bitcask.Tx, args [][]byte) (reply, error) {
	r, bad := parseRead(args, false)
	if bad != nil {
		return bad, nil
	}
	streams := make([]*stream, len(r.keys))
	after := make([]streamID, len(r.keys))
	var retry [][]byte
	for i, key := range r.keys {
		s, bad, err := openStream(tx, key)
		if bad != nil || err != nil {
			return bad, err
		}
		streams[i] = s
		switch string(r.ids[i]) {
		case "$":
			after[i] = s.last
			if retry == nil {
				retry = append([][]byte(nil), args...)
			}
			retry[r.at+len(r.keys)+i] = []byte(s.last.String())
		case ">":
			return errorReply("ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option."), nil
		default:
			var ok bool
			if after[i], _, ok = parseStreamID(r.ids[i], 0, true, false); !ok {
				return errorReply(errStreamID), nil
			}
		}
	}
	var out arrayReply
	for i, s := range streams {
		from, ok := after[i].next()
		if !ok || s.length == 0 {
			continue
		}
		ids, err := s.ids(from, maxStreamID, r.count, false)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			continue
		}
		entries, err := s.entries(ids)
		if err != nil {
			return nil, err
		}
		out = append(out, arrayReply{bulkReply(r.keys[i]), entries})
	}
	switch {
	case len(out) > 0:
		return out, nil
	case r.timeout >= 0:
		return blockReply{timeout: r.timeout, keys: r.keys, empty: nullArrayReply{}, retry: retry}, nil
	}
	return nullArrayReply{}, nil
}

func cmdXInfo(tx *bitcask.Tx, args [][]byte) (reply, error) {
	switch sub := upper(args[1]); {
	case sub == "HELP" && len(args) == 2:
		return stringsReply{
			"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
			"CONSUMERS <key> <groupname>",
			"    Show consumers of <groupname>.",
			"GROUPS <key>",
			"    Show the stream consumer groups.",
			"STREAM <key> [FULL [COUNT <count>]",
			"    Show information about the stream.",
			"HELP",
			"    Print this help.",
		}, nil
	case sub == "STREAM" && len(args) >= 3:
		return xinfoStream(tx, args)
	case sub == "GROUPS" && len(args) == 3:
		return xinfoGroups(tx, args[2])
	case sub == "CONSUMERS" && len(args) == 4:
		return xinfoConsumers(tx, args[2], args[3])
	}
	return unknownSubcommand(args), nil
}

func xinfoStream(tx *bitcask.Tx, args [][]byte) (reply, error) {
	full, count := false, int64(10)
	switch {
	case len(args) == 3:
	case upper(args[3]) != "FULL":
		return unknownSubcommand(args), nil
	case len(args) == 6 && upper(args[4]) == "COUNT":
		n, ok := parseInt(args[5])
		if !ok {
			return errorReply(errNotInteger), nil
		}
		full, count = true, n
		if count < 0 {
			count = 10
		}
	case len(args) > 4:
		return unknownSubcommand(args), nil
	default:
		full = true
	}
	s, bad, err := openStream(tx, args[2])
	switch {
	case bad != nil || err != nil:
		return bad, err
	case !s.exists:
		return errorReply(errNoKey), nil
	}
	first, _, err := s.edge(false)
	if err != nil {
		return nil, err
	}
	out := arrayReply{
		bulkReply("length"), intReply(int64(s.length)),
		bulkReply("radix-tree-keys"), intReply(0),
		bulkReply("radix-tree-nodes"), intReply(0),
		bulkReply("last-generated-id"), s.last.reply(),
		bulkReply("max-deleted-entry-id"), s.maxDeleted.reply(),
		bulkReply("entries-added"), intReply(int64(s.added)),
		bulkReply("recorded-first-entry-id"), first.reply(),
	}
	if full {
		ids, err := s.ids(streamID{}, maxStreamID, count, false)
		if err != nil {
			return nil, err
		}
		entries, err := s.entries(ids)
		if err != nil {
			return nil, err
		}
		groups, err := s.groupsFull(count)
		if err != nil {
			return nil, err
		}
		return append(out, bulkReply("entries"), entries, bulkReply("groups"), groups), nil
	}
	firstEntry, err := s.edgeEntry(false)
	if err != nil {
		return nil, err
	}
	lastEntry, err := s.edgeEntry(true)
	if err != nil {
		return nil, err
	}
	return append(out, bulkReply("groups"), intReply(int64(len(s.groups))), bulkReply("first-entry"), firstEntry, bulkReply("last-entry"), lastEntry), nil
}

func (s *stream) edgeEntry(last bool) (reply, error) {
	id, ok, err := s.edge(last)
	if err != nil || !ok {
		return nilReply, err
	}
	return s.entry(id)
}
