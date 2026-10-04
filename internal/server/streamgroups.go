package server

import (
	"encoding/binary"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/acneism/casketdb/internal/bitcask"
)

const errBusyGroup = "BUSYGROUP Consumer Group name already exists"

var streamGroupCommands = map[string]command{
	"xgroup":     {arity: -2, kind: kindWrite, keys: keySpec{first: 2, last: 2, step: 1}, acl: catStream, tx: cmdXGroup},
	"xreadgroup": {arity: -7, kind: kindWrite, keys: keySpec{streams: true}, acl: catStream | catBlocking, tx: cmdXReadGroup, blocks: true},
	"xack":       {arity: -4, kind: kindWrite, keys: oneKey, acl: catStream | catFast, tx: cmdXAck},
	"xpending":   {arity: -3, kind: kindRead, keys: oneKey, acl: catStream, tx: cmdXPending},
	"xclaim":     {arity: -6, kind: kindWrite, keys: oneKey, acl: catStream | catFast, tx: cmdXClaim},
	"xautoclaim": {arity: -6, kind: kindWrite, keys: oneKey, acl: catStream | catFast, tx: cmdXAutoClaim},
}

type streamGroup struct {
	name        string
	last        streamID
	entriesRead int64
	pending     uint64
	consumers   uint64
}

type streamConsumer struct {
	name    string
	seen    int64
	active  int64
	pending uint64
}

type nack struct {
	time     int64
	count    uint64
	consumer string
}

func groupKey(tag byte, group string) string {
	return string(binary.AppendUvarint([]byte{tag}, uint64(len(group)))) + group
}

func consumerMember(group, consumer string) string {
	return groupKey('c', group) + consumer
}

func nackMember(group string, id streamID) string {
	return groupKey('p', group) + idBytes(id)
}

func ownedPrefix(group, consumer string) string {
	return groupKey('q', group) + string(binary.AppendUvarint(nil, uint64(len(consumer)))) + consumer
}

func (s *stream) group(name []byte) *streamGroup {
	i, found := slices.BinarySearchFunc(s.groups, string(name), func(g *streamGroup, n string) int { return strings.Compare(g.name, n) })
	if !found {
		return nil
	}
	return s.groups[i]
}

func (s *stream) addGroup(g *streamGroup) {
	i, _ := slices.BinarySearchFunc(s.groups, g.name, func(g *streamGroup, n string) int { return strings.Compare(g.name, n) })
	s.groups = slices.Insert(s.groups, i, g)
}

func (s *stream) deletePrefix(prefix string) error {
	var doomed []string
	err := s.scan(prefix, func(m string) bool {
		if !strings.HasPrefix(m, prefix) {
			return false
		}
		doomed = append(doomed, m)
		return true
	})
	for _, m := range doomed {
		s.tx.DeleteMember(s.key, m)
	}
	return err
}

func (s *stream) hasTombstones(from streamID) (bool, error) {
	if s.length == 0 || s.maxDeleted == (streamID{}) {
		return false, nil
	}
	first, err := s.firstID()
	if err != nil || s.maxDeleted.less(first) {
		return false, err
	}
	return !s.maxDeleted.less(from), nil
}

func (s *stream) estimateRead(id streamID) (int64, error) {
	switch {
	case s.added == 0:
		return 0, nil
	case s.length == 0 && !s.last.less(id), id == s.last:
		return int64(s.added), nil
	case s.last.less(id):
		return -1, nil
	}
	first, err := s.firstID()
	if err != nil {
		return -1, err
	}
	if s.maxDeleted == (streamID{}) || s.maxDeleted.less(first) {
		switch {
		case id.less(first):
			return int64(s.added - s.length), nil
		case id == first:
			return int64(s.added - s.length + 1), nil
		}
	}
	return -1, nil
}

func (s *stream) lag(g *streamGroup) (reply, error) {
	if s.added == 0 {
		return intReply(0), nil
	}
	tombstones, err := s.hasTombstones(g.last)
	if err != nil {
		return nil, err
	}
	if g.entriesRead != -1 && !tombstones {
		return intReply(int64(s.added) - g.entriesRead), nil
	}
	read, err := s.estimateRead(g.last)
	if err != nil || read == -1 {
		return nilReply, err
	}
	return intReply(int64(s.added) - read), nil
}

type groupSession struct {
	s         *stream
	g         *streamGroup
	now       int64
	consumers map[string]*streamConsumer
}

func (s *stream) session(g *streamGroup) *groupSession {
	return &groupSession{s: s, g: g, now: s.tx.Now(), consumers: map[string]*streamConsumer{}}
}

func decodeConsumer(name string, v []byte) *streamConsumer {
	c := &streamConsumer{name: name}
	read := func() uint64 {
		n, k := binary.Uvarint(v)
		if k > 0 {
			v = v[k:]
		}
		return n
	}
	c.seen, c.active, c.pending = int64(read()), int64(read())-1, read()
	return c
}

func (gs *groupSession) consumer(name string, create bool) (*streamConsumer, error) {
	if c, ok := gs.consumers[name]; ok {
		return c, nil
	}
	v, found, err := gs.s.tx.GetMember(gs.s.key, consumerMember(gs.g.name, name))
	var c *streamConsumer
	switch {
	case err != nil:
		return nil, err
	case found:
		c = decodeConsumer(name, v)
	case !create:
		return nil, nil
	default:
		c = &streamConsumer{name: name, seen: gs.now, active: -1}
		gs.g.consumers++
	}
	gs.consumers[name] = c
	return c, nil
}

func (gs *groupSession) save() {
	for _, c := range gs.consumers {
		v := binary.AppendUvarint(nil, uint64(max(c.seen, 0)))
		v = binary.AppendUvarint(v, uint64(c.active+1))
		gs.s.tx.PutMember(gs.s.key, consumerMember(gs.g.name, c.name), binary.AppendUvarint(v, c.pending))
	}
	gs.s.store()
}

func (gs *groupSession) nack(id streamID) (*nack, bool, error) {
	v, found, err := gs.s.tx.GetMember(gs.s.key, nackMember(gs.g.name, id))
	if err != nil || !found {
		return &nack{}, false, err
	}
	n := &nack{}
	t, k := binary.Uvarint(v)
	if k > 0 {
		v = v[k:]
	}
	count, k := binary.Uvarint(v)
	if k > 0 {
		v = v[k:]
	}
	n.time, n.count, n.consumer = int64(t), count, string(v)
	return n, true, nil
}

func (gs *groupSession) putNack(id streamID, n *nack) {
	v := binary.AppendUvarint(binary.AppendUvarint(nil, uint64(max(n.time, 0))), n.count)
	gs.s.tx.PutMember(gs.s.key, nackMember(gs.g.name, id), append(v, n.consumer...))
}

func (gs *groupSession) give(id streamID, n *nack, found bool, c *streamConsumer) error {
	if found && n.consumer == c.name {
		return nil
	}
	if found {
		old, err := gs.consumer(n.consumer, false)
		if err != nil {
			return err
		}
		if old != nil {
			old.pending--
		}
		gs.s.tx.DeleteMember(gs.s.key, ownedPrefix(gs.g.name, n.consumer)+idBytes(id))
	} else {
		gs.g.pending++
	}
	n.consumer = c.name
	c.pending++
	gs.s.tx.PutMember(gs.s.key, ownedPrefix(gs.g.name, c.name)+idBytes(id), nil)
	return nil
}

func (gs *groupSession) drop(id streamID, n *nack) error {
	gs.s.tx.DeleteMember(gs.s.key, nackMember(gs.g.name, id))
	gs.s.tx.DeleteMember(gs.s.key, ownedPrefix(gs.g.name, n.consumer)+idBytes(id))
	gs.g.pending--
	c, err := gs.consumer(n.consumer, false)
	if c != nil {
		c.pending--
	}
	return err
}

func (gs *groupSession) advance(id streamID) error {
	if !gs.g.last.less(id) {
		return nil
	}
	tombstones, err := gs.s.hasTombstones(id)
	if err != nil {
		return err
	}
	switch {
	case gs.g.entriesRead != -1 && !tombstones:
		gs.g.entriesRead++
	case gs.s.added > 0:
		if gs.g.entriesRead, err = gs.s.estimateRead(id); err != nil {
			return err
		}
	}
	gs.g.last = id
	return nil
}

func (gs *groupSession) deliver(c *streamConsumer, count int64, noack bool) (arrayReply, error) {
	from, ok := gs.g.last.next()
	if !ok {
		return nil, nil
	}
	ids, err := gs.s.ids(from, maxStreamID, count, false)
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	entries, err := gs.s.entries(ids)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := gs.advance(id); err != nil {
			return nil, err
		}
		if noack {
			continue
		}
		n, found, err := gs.nack(id)
		if err != nil {
			return nil, err
		}
		if err := gs.give(id, n, found, c); err != nil {
			return nil, err
		}
		n.time, n.count = gs.now, 1
		gs.putNack(id, n)
	}
	c.active = gs.now
	return entries, nil
}

func (gs *groupSession) history(c *streamConsumer, after streamID, count int64) (arrayReply, error) {
	out := arrayReply{}
	from, ok := after.next()
	if !ok {
		return out, nil
	}
	ids, err := gs.s.idRange(ownedPrefix(gs.g.name, c.name), from, maxStreamID, count, false)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		e, err := gs.s.entry(id)
		if err != nil {
			return nil, err
		}
		if _, deleted := e.(nullReply); deleted {
			out = append(out, arrayReply{id.reply(), nullArrayReply{}})
			continue
		}
		n, found, err := gs.nack(id)
		if err != nil {
			return nil, err
		}
		if found {
			n.time, n.count = gs.now, n.count+1
			gs.putNack(id, n)
		}
		out = append(out, e)
	}
	return out, nil
}

func noGroup(key, group []byte, suffix string) errorReply {
	return errorReply("NOGROUP No such key '" + string(key) + "' or consumer group '" + string(group) + "'" + suffix)
}

func cmdXGroup(tx *bitcask.Tx, args [][]byte) (reply, error) {
	sub := upper(args[1])
	switch {
	case sub == "HELP" && len(args) == 2:
		return stringsReply{
			"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
			"CREATE <key> <groupname> <id|$> [option]",
			"    Create a new consumer group. Options are:",
			"    * MKSTREAM",
			"      Create the empty stream if it does not exist.",
			"    * ENTRIESREAD entries_read",
			"      Set the group's entries_read counter (internal use).",
			"CREATECONSUMER <key> <groupname> <consumer>",
			"    Create a new consumer in the specified group.",
			"DELCONSUMER <key> <groupname> <consumer>",
			"    Remove the specified consumer.",
			"DESTROY <key> <groupname>",
			"    Remove the specified group.",
			"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
			"    Set the current group ID and entries_read counter.",
			"HELP",
			"    Print this help.",
		}, nil
	case sub == "CREATE" && len(args) >= 5 && len(args) <= 8,
		sub == "SETID" && (len(args) == 5 || len(args) == 7),
		sub == "DESTROY" && len(args) == 4,
		(sub == "CREATECONSUMER" || sub == "DELCONSUMER") && len(args) == 5:
	default:
		return unknownSubcommand(args), nil
	}
	mkstream, entriesRead := false, int64(-1)
	for i := 5; (sub == "CREATE" || sub == "SETID") && i < len(args); i++ {
		switch opt := upper(args[i]); {
		case opt == "MKSTREAM" && sub == "CREATE":
			mkstream = true
		case opt == "ENTRIESREAD" && i+1 < len(args):
			var ok bool
			if entriesRead, ok = parseInt(args[i+1]); !ok {
				return errorReply(errNotInteger), nil
			}
			if entriesRead < -1 {
				return errorReply("ERR value for ENTRIESREAD must be positive or -1"), nil
			}
			i++
		default:
			return unknownSubcommand(args), nil
		}
	}
	s, bad, err := openStream(tx, args[2])
	if bad != nil || err != nil {
		return bad, err
	}
	g := s.group(args[3])
	if !mkstream {
		switch {
		case !s.exists:
			return errorReply("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."), nil
		case g == nil && sub != "CREATE" && sub != "DESTROY":
			return errorReply("NOGROUP No such consumer group '" + string(args[3]) + "' for key name '" + string(args[2]) + "'"), nil
		}
	}
	switch sub {
	case "CREATE", "SETID":
		id := s.last
		if string(args[4]) != "$" {
			var ok bool
			if id, _, ok = parseStreamID(args[4], 0, sub == "CREATE", false); !ok {
				return errorReply(errStreamID), nil
			}
		}
		if sub == "SETID" {
			g.last, g.entriesRead = id, entriesRead
			break
		}
		if g != nil {
			return errorReply(errBusyGroup), nil
		}
		s.addGroup(&streamGroup{name: string(args[3]), last: id, entriesRead: entriesRead})
	case "DESTROY":
		if g == nil {
			return intReply(0), nil
		}
		for _, tag := range []byte{'c', 'p', 'q'} {
			if err := s.deletePrefix(groupKey(tag, g.name)); err != nil {
				return nil, err
			}
		}
		s.groups = slices.DeleteFunc(s.groups, func(x *streamGroup) bool { return x == g })
		s.store()
		return intReply(1), nil
	case "CREATECONSUMER":
		gs := s.session(g)
		before := g.consumers
		if _, err := gs.consumer(string(args[4]), true); err != nil {
			return nil, err
		}
		if g.consumers == before {
			return intReply(0), nil
		}
		gs.save()
		return intReply(1), nil
	case "DELCONSUMER":
		gs := s.session(g)
		c, err := gs.consumer(string(args[4]), false)
		if err != nil || c == nil {
			return intReply(0), err
		}
		owned := ownedPrefix(g.name, c.name)
		ids, err := s.idRange(owned, streamID{}, maxStreamID, 0, false)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			tx.DeleteMember(s.key, nackMember(g.name, id))
			tx.DeleteMember(s.key, owned+idBytes(id))
		}
		g.pending -= uint64(len(ids))
		g.consumers--
		tx.DeleteMember(s.key, consumerMember(g.name, c.name))
		s.store()
		return intReply(int64(c.pending)), nil
	}
	s.store()
	return statusReply("OK"), nil
}

func cmdXReadGroup(tx *bitcask.Tx, args [][]byte) (reply, error) {
	r, bad := parseRead(args, true)
	if bad != nil {
		return bad, nil
	}
	sessions := map[string]*groupSession{}
	type target struct {
		gs      *groupSession
		history bool
		after   streamID
	}
	targets := make([]target, len(r.keys))
	for i, key := range r.keys {
		gs := sessions[string(key)]
		if gs == nil {
			s, bad, err := openStream(tx, key)
			if bad != nil || err != nil {
				return bad, err
			}
			g := s.group(r.group)
			if !s.exists || g == nil {
				return noGroup(key, r.group, " in XREADGROUP with GROUP option"), nil
			}
			gs = s.session(g)
			sessions[string(key)] = gs
		}
		targets[i].gs = gs
		switch string(r.ids[i]) {
		case "$":
			return errorReply("ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set."), nil
		case ">":
		default:
			id, _, ok := parseStreamID(r.ids[i], 0, true, false)
			if !ok {
				return errorReply(errStreamID), nil
			}
			targets[i].history, targets[i].after = true, id
		}
	}
	var out arrayReply
	for i, t := range targets {
		c, err := t.gs.consumer(string(r.consumer), true)
		if err != nil {
			return nil, err
		}
		c.seen = t.gs.now
		var entries arrayReply
		if t.history {
			entries, err = t.gs.history(c, t.after, r.count)
		} else {
			entries, err = t.gs.deliver(c, r.count, r.noack)
		}
		if err != nil {
			return nil, err
		}
		if t.history || len(entries) > 0 {
			out = append(out, arrayReply{bulkReply(r.keys[i]), entries})
		}
	}
	for _, gs := range sessions {
		gs.save()
	}
	switch {
	case len(out) > 0:
		return out, nil
	case r.timeout >= 0:
		return blockReply{timeout: r.timeout, keys: r.keys, empty: nullArrayReply{}}, nil
	}
	return nullArrayReply{}, nil
}

func openGroup(tx *bitcask.Tx, key, group []byte, suffix string) (*groupSession, reply, error) {
	s, bad, err := openStream(tx, key)
	if bad != nil || err != nil {
		return nil, bad, err
	}
	g := s.group(group)
	if !s.exists || g == nil {
		return nil, noGroup(key, group, suffix), nil
	}
	return s.session(g), nil, nil
}

func cmdXAck(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openStream(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	g := s.group(args[2])
	if !s.exists || g == nil {
		return intReply(0), nil
	}
	ids := make([]streamID, len(args)-3)
	for i, arg := range args[3:] {
		var ok bool
		if ids[i], _, ok = parseStreamID(arg, 0, true, false); !ok {
			return errorReply(errStreamID), nil
		}
	}
	gs := s.session(g)
	acked := 0
	for _, id := range ids {
		n, found, err := gs.nack(id)
		if err != nil {
			return nil, err
		}
		if found {
			if err := gs.drop(id, n); err != nil {
				return nil, err
			}
			acked++
		}
	}
	if acked > 0 {
		gs.save()
	}
	return intReply(acked), nil
}

func parseRangeIDs(startArg, endArg []byte) (streamID, streamID, reply) {
	start, exclusive, ok := parseIntervalID(startArg, 0)
	if !ok {
		return start, start, errorReply(errStreamID)
	}
	if exclusive {
		if start, ok = start.next(); !ok {
			return start, start, errorReply("ERR invalid start ID for the interval")
		}
	}
	end, exclusive, ok := parseIntervalID(endArg, math.MaxUint64)
	if !ok {
		return start, end, errorReply(errStreamID)
	}
	if exclusive {
		if end, ok = end.prev(); !ok {
			return start, end, errorReply("ERR invalid end ID for the interval")
		}
	}
	return start, end, nil
}

func cmdXPending(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if len(args) != 3 && (len(args) < 6 || len(args) > 9) {
		return errorReply(errSyntax), nil
	}
	var minIdle, count int64
	var start, end streamID
	var consumer []byte
	if len(args) >= 6 {
		at := 3
		if upper(args[3]) == "IDLE" {
			var ok bool
			if minIdle, ok = parseInt(args[4]); !ok {
				return errorReply(errNotInteger), nil
			}
			if len(args) < 8 {
				return errorReply(errSyntax), nil
			}
			at = 5
		}
		n, ok := parseInt(args[at+2])
		if !ok {
			return errorReply(errNotInteger), nil
		}
		count = max(n, 0)
		var bad reply
		if start, end, bad = parseRangeIDs(args[at], args[at+1]); bad != nil {
			return bad, nil
		}
		if at+3 < len(args) {
			consumer = args[at+3]
		}
	}
	gs, bad, err := openGroup(tx, args[1], args[2], "")
	if bad != nil || err != nil {
		return bad, err
	}
	s, g := gs.s, gs.g
	if len(args) == 3 {
		if g.pending == 0 {
			return arrayReply{intReply(0), nilReply, nilReply, nullArrayReply{}}, nil
		}
		prefix := groupKey('p', g.name)
		first, err := s.idRange(prefix, streamID{}, maxStreamID, 1, false)
		if err != nil {
			return nil, err
		}
		last, err := s.idRange(prefix, streamID{}, maxStreamID, 1, true)
		if err != nil || len(first) == 0 || len(last) == 0 {
			return nil, err
		}
		owners := arrayReply{}
		consumers, err := gs.all()
		for _, c := range consumers {
			if c.pending > 0 {
				owners = append(owners, arrayReply{bulkReply(c.name), bulkReply(strconv.FormatUint(c.pending, 10))})
			}
		}
		return arrayReply{intReply(int64(g.pending)), first[0].reply(), last[0].reply(), owners}, err
	}
	prefix := groupKey('p', g.name)
	if consumer != nil {
		c, err := gs.consumer(string(consumer), false)
		if err != nil || c == nil {
			return arrayReply{}, err
		}
		prefix = ownedPrefix(g.name, c.name)
	}
	ids, err := s.idRange(prefix, start, end, 0, false)
	if err != nil {
		return nil, err
	}
	out := arrayReply{}
	for _, id := range ids {
		if int64(len(out)) == count {
			break
		}
		n, found, err := gs.nack(id)
		if err != nil {
			return nil, err
		}
		idle := gs.now - n.time
		if !found || minIdle > 0 && idle < minIdle {
			continue
		}
		out = append(out, arrayReply{id.reply(), bulkReply(n.consumer), intReply(max(idle, 0)), intReply(int64(n.count))})
	}
	return out, nil
}

func (gs *groupSession) all() ([]*streamConsumer, error) {
	prefix := groupKey('c', gs.g.name)
	var names []string
	err := gs.s.scan(prefix, func(m string) bool {
		if !strings.HasPrefix(m, prefix) {
			return false
		}
		names = append(names, m[len(prefix):])
		return true
	})
	out := make([]*streamConsumer, 0, len(names))
	for _, name := range names {
		c, err := gs.consumer(name, false)
		if err != nil {
			return nil, err
		}
		if c != nil {
			out = append(out, c)
		}
	}
	return out, err
}

func (gs *groupSession) claimReply(id streamID, justID bool) (reply, error) {
	if justID {
		return id.reply(), nil
	}
	return gs.s.entry(id)
}

func cmdXClaim(tx *bitcask.Tx, args [][]byte) (reply, error) {
	gs, bad, err := openGroup(tx, args[1], args[2], "")
	if bad != nil || err != nil {
		return bad, err
	}
	minIdle, ok := parseInt(args[4])
	if !ok {
		return errorReply("ERR Invalid min-idle-time argument for XCLAIM"), nil
	}
	minIdle = max(minIdle, 0)
	j := 5
	var ids []streamID
	for ; j < len(args); j++ {
		id, _, ok := parseStreamID(args[j], 0, true, false)
		if !ok {
			break
		}
		ids = append(ids, id)
	}
	now := gs.now
	force, justID, retry, delivery := false, false, int64(-1), int64(-1)
	var lastID streamID
	for ; j < len(args); j++ {
		more := len(args) - 1 - j
		switch opt := upper(args[j]); {
		case opt == "FORCE":
			force = true
		case opt == "JUSTID":
			justID = true
		case (opt == "IDLE" || opt == "TIME" || opt == "RETRYCOUNT") && more > 0:
			j++
			n, ok := parseInt(args[j])
			if !ok {
				return errorReply("ERR Invalid " + opt + " option argument for XCLAIM"), nil
			}
			switch opt {
			case "IDLE":
				delivery = now - n
			case "TIME":
				delivery = n
			default:
				retry = n
			}
		case opt == "LASTID" && more > 0:
			j++
			if lastID, _, ok = parseStreamID(args[j], 0, true, false); !ok {
				return errorReply(errStreamID), nil
			}
		default:
			return errorReply("ERR Unrecognized XCLAIM option '" + string(args[j]) + "'"), nil
		}
	}
	if gs.g.last.less(lastID) {
		gs.g.last = lastID
	}
	if delivery < 0 || delivery > now {
		delivery = now
	}
	c, err := gs.consumer(string(args[3]), true)
	if err != nil {
		return nil, err
	}
	c.seen = now
	out := arrayReply{}
	for _, id := range ids {
		n, found, err := gs.nack(id)
		if err != nil {
			return nil, err
		}
		exists, err := tx.HasMember(gs.s.key, entryMember(id))
		switch {
		case err != nil:
			return nil, err
		case !exists:
			if found {
				if err := gs.drop(id, n); err != nil {
					return nil, err
				}
			}
			continue
		case !found && !force:
			continue
		case !found:
			n.count = 1
		case minIdle > 0 && now-n.time < minIdle:
			continue
		}
		if err := gs.give(id, n, found, c); err != nil {
			return nil, err
		}
		n.time = delivery
		switch {
		case retry >= 0:
			n.count = uint64(retry)
		case !justID:
			n.count++
		}
		gs.putNack(id, n)
		r, err := gs.claimReply(id, justID)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
		c.active = now
	}
	gs.save()
	return out, nil
}

func cmdXAutoClaim(tx *bitcask.Tx, args [][]byte) (reply, error) {
	minIdle, ok := parseInt(args[4])
	if !ok {
		return errorReply("ERR Invalid min-idle-time argument for XAUTOCLAIM"), nil
	}
	minIdle = max(minIdle, 0)
	start, exclusive, ok := parseIntervalID(args[5], 0)
	if !ok {
		return errorReply(errStreamID), nil
	}
	if exclusive {
		if start, ok = start.next(); !ok {
			return errorReply("ERR invalid start ID for the interval"), nil
		}
	}
	count, justID := int64(100), false
	for j := 6; j < len(args); j++ {
		switch opt := upper(args[j]); {
		case opt == "COUNT" && j+1 < len(args):
			n, ok := parseInt(args[j+1])
			if !ok || n < 1 || n > math.MaxInt64/16 {
				return errorReply("ERR COUNT must be > 0"), nil
			}
			count = n
			j++
		case opt == "JUSTID":
			justID = true
		default:
			return errorReply(errSyntax), nil
		}
	}
	gs, bad, err := openGroup(tx, args[1], args[2], "")
	if bad != nil || err != nil {
		return bad, err
	}
	c, err := gs.consumer(string(args[3]), true)
	if err != nil {
		return nil, err
	}
	c.seen = gs.now
	attempts := count * 10
	ids, err := gs.s.idRange(groupKey('p', gs.g.name), start, maxStreamID, attempts+1, false)
	if err != nil {
		return nil, err
	}
	claimed, deleted := arrayReply{}, arrayReply{}
	done := 0
	for done < len(ids) && int64(done) < attempts && count > 0 {
		id := ids[done]
		done++
		n, _, err := gs.nack(id)
		if err != nil {
			return nil, err
		}
		exists, err := tx.HasMember(gs.s.key, entryMember(id))
		switch {
		case err != nil:
			return nil, err
		case !exists:
			if err := gs.drop(id, n); err != nil {
				return nil, err
			}
			deleted = append(deleted, id.reply())
			count--
			continue
		case minIdle > 0 && gs.now-n.time < minIdle:
			continue
		}
		if err := gs.give(id, n, true, c); err != nil {
			return nil, err
		}
		n.time = gs.now
		if !justID {
			n.count++
		}
		gs.putNack(id, n)
		r, err := gs.claimReply(id, justID)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, r)
		count--
		c.active = gs.now
	}
	next := streamID{}
	if done < len(ids) {
		next = ids[done]
	}
	gs.save()
	return arrayReply{next.reply(), claimed, deleted}, nil
}

func xinfoGroups(tx *bitcask.Tx, key []byte) (reply, error) {
	s, bad, err := openStream(tx, key)
	switch {
	case bad != nil || err != nil:
		return bad, err
	case !s.exists:
		return errorReply(errNoKey), nil
	}
	out := arrayReply{}
	for _, g := range s.groups {
		lag, err := s.lag(g)
		if err != nil {
			return nil, err
		}
		out = append(out, arrayReply{
			bulkReply("name"), bulkReply(g.name),
			bulkReply("consumers"), intReply(int64(g.consumers)),
			bulkReply("pending"), intReply(int64(g.pending)),
			bulkReply("last-delivered-id"), g.last.reply(),
			bulkReply("entries-read"), entriesRead(g),
			bulkReply("lag"), lag,
		})
	}
	return out, nil
}

func entriesRead(g *streamGroup) reply {
	if g.entriesRead == -1 {
		return nilReply
	}
	return intReply(g.entriesRead)
}

func xinfoConsumers(tx *bitcask.Tx, key, group []byte) (reply, error) {
	s, bad, err := openStream(tx, key)
	switch {
	case bad != nil || err != nil:
		return bad, err
	case !s.exists:
		return errorReply(errNoKey), nil
	}
	g := s.group(group)
	if g == nil {
		return errorReply("NOGROUP No such consumer group '" + string(group) + "' for key name '" + string(key) + "'"), nil
	}
	gs := s.session(g)
	consumers, err := gs.all()
	out := arrayReply{}
	for _, c := range consumers {
		inactive := int64(-1)
		if c.active != -1 {
			inactive = gs.now - c.active
		}
		out = append(out, arrayReply{
			bulkReply("name"), bulkReply(c.name),
			bulkReply("pending"), intReply(int64(c.pending)),
			bulkReply("idle"), intReply(max(gs.now-c.seen, 0)),
			bulkReply("inactive"), intReply(inactive),
		})
	}
	return out, err
}

func (s *stream) groupsFull(count int64) (arrayReply, error) {
	out := arrayReply{}
	for _, g := range s.groups {
		gs := s.session(g)
		lag, err := s.lag(g)
		if err != nil {
			return nil, err
		}
		ids, err := s.idRange(groupKey('p', g.name), streamID{}, maxStreamID, count, false)
		if err != nil {
			return nil, err
		}
		pending := arrayReply{}
		for _, id := range ids {
			n, _, err := gs.nack(id)
			if err != nil {
				return nil, err
			}
			pending = append(pending, arrayReply{id.reply(), bulkReply(n.consumer), intReply(n.time), intReply(int64(n.count))})
		}
		consumers, err := gs.all()
		if err != nil {
			return nil, err
		}
		cs := arrayReply{}
		for _, c := range consumers {
			ids, err := s.idRange(ownedPrefix(g.name, c.name), streamID{}, maxStreamID, count, false)
			if err != nil {
				return nil, err
			}
			owned := arrayReply{}
			for _, id := range ids {
				n, _, err := gs.nack(id)
				if err != nil {
					return nil, err
				}
				owned = append(owned, arrayReply{id.reply(), intReply(n.time), intReply(int64(n.count))})
			}
			cs = append(cs, arrayReply{
				bulkReply("name"), bulkReply(c.name),
				bulkReply("seen-time"), intReply(c.seen),
				bulkReply("active-time"), intReply(c.active),
				bulkReply("pel-count"), intReply(int64(c.pending)),
				bulkReply("pending"), owned,
			})
		}
		out = append(out, arrayReply{
			bulkReply("name"), bulkReply(g.name),
			bulkReply("last-delivered-id"), g.last.reply(),
			bulkReply("entries-read"), entriesRead(g),
			bulkReply("lag"), lag,
			bulkReply("pel-count"), intReply(int64(g.pending)),
			bulkReply("pending"), pending,
			bulkReply("consumers"), cs,
		})
	}
	return out, nil
}
