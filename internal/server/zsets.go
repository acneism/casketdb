package server

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/acneism/casketdb/internal/bitcask"
)

var zsetCommands = map[string]command{
	"zadd":             {arity: -4, kind: kindWrite, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZAdd},
	"zincrby":          {arity: 4, kind: kindWrite, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZIncrBy},
	"zrem":             {arity: -3, kind: kindWrite, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZRem},
	"zscore":           {arity: 3, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZScore},
	"zmscore":          {arity: -3, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZMScore},
	"zcard":            {arity: 2, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZCard},
	"zcount":           {arity: 4, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZCount},
	"zlexcount":        {arity: 4, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZLexCount},
	"zrank":            {arity: -3, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZRank},
	"zrevrank":         {arity: -3, kind: kindRead, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZRevRank},
	"zrange":           {arity: -4, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRange},
	"zrangestore":      {arity: -5, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catSortedSet, tx: cmdZRangeStore},
	"zrevrange":        {arity: -4, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRevRange},
	"zrangebyscore":    {arity: -4, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRangeByScore},
	"zrevrangebyscore": {arity: -4, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRevRangeByScore},
	"zrangebylex":      {arity: -4, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRangeByLex},
	"zrevrangebylex":   {arity: -4, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRevRangeByLex},
	"zremrangebyrank":  {arity: 4, kind: kindWrite, keys: oneKey, acl: catSortedSet, tx: cmdZRemRangeByRank},
	"zremrangebyscore": {arity: 4, kind: kindWrite, keys: oneKey, acl: catSortedSet, tx: cmdZRemRangeByScore},
	"zremrangebylex":   {arity: 4, kind: kindWrite, keys: oneKey, acl: catSortedSet, tx: cmdZRemRangeByLex},
	"zpopmin":          {arity: -2, kind: kindWrite, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZPopMin},
	"zpopmax":          {arity: -2, kind: kindWrite, keys: oneKey, acl: catSortedSet | catFast, tx: cmdZPopMax},
	"zmpop":            {arity: -4, kind: kindWrite, keys: keySpec{numkeys: 1}, acl: catSortedSet, tx: cmdZMPop},
	"zrandmember":      {arity: -2, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZRandMember},
	"zscan":            {arity: -3, kind: kindRead, keys: oneKey, acl: catSortedSet, tx: cmdZScan},
	"zunion":           {arity: -3, kind: kindRead, keys: keySpec{numkeys: 1}, acl: catSortedSet, tx: cmdZUnion},
	"zinter":           {arity: -3, kind: kindRead, keys: keySpec{numkeys: 1}, acl: catSortedSet, tx: cmdZInter},
	"zdiff":            {arity: -3, kind: kindRead, keys: keySpec{numkeys: 1}, acl: catSortedSet, tx: cmdZDiff},
	"zintercard":       {arity: -3, kind: kindRead, keys: keySpec{numkeys: 1}, acl: catSortedSet, tx: cmdZInterCard},
	"zunionstore":      {arity: -4, kind: kindWrite, keys: keySpec{first: 1, last: 1, step: 1, numkeys: 2}, acl: catSortedSet, tx: cmdZUnionStore},
	"zinterstore":      {arity: -4, kind: kindWrite, keys: keySpec{first: 1, last: 1, step: 1, numkeys: 2}, acl: catSortedSet, tx: cmdZInterStore},
	"zdiffstore":       {arity: -4, kind: kindWrite, keys: keySpec{first: 1, last: 1, step: 1, numkeys: 2}, acl: catSortedSet, tx: cmdZDiffStore},
}

func openZSet(tx *bitcask.Tx, key []byte) (*collection, reply, error) {
	return openCollection(tx, key, typeZSet)
}

func scoreKey(f float64) []byte {
	if f == 0 {
		f = 0
	}
	b := math.Float64bits(f)
	if b>>63 == 0 {
		b |= 1 << 63
	} else {
		b = ^b
	}
	return binary.BigEndian.AppendUint64(nil, b)
}

func keyScore(k []byte) float64 {
	b := binary.BigEndian.Uint64(k)
	if b>>63 == 1 {
		b &^= 1 << 63
	} else {
		b = ^b
	}
	return math.Float64frombits(b)
}

func formatScore(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == math.Trunc(f) && math.Abs(f) <= math.MaxInt64/2:
		return strconv.FormatInt(int64(f), 10)
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	if exp, _ := strconv.Atoi(s[strings.IndexByte(s, 'e')+1:]); exp < -4 || exp >= 17 {
		return s
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func scoreReply(k []byte) reply {
	return doubleReply(formatScore(keyScore(k)))
}

type zitem struct {
	member string
	score  []byte
}

func compareZItems(a, b zitem) int {
	if c := bytes.Compare(a.score, b.score); c != 0 {
		return c
	}
	return strings.Compare(a.member, b.member)
}

func scoredReply(items []zitem, withScores bool) reply {
	if withScores {
		return pairsReply(itemsReply(items, true))
	}
	return itemsReply(items, false)
}

func itemsReply(items []zitem, withScores bool) arrayReply {
	out := make(arrayReply, 0, len(items)*2)
	for _, it := range items {
		out = append(out, bulkReply(it.member))
		if withScores {
			out = append(out, scoreReply(it.score))
		}
	}
	return out
}

func (c *collection) sortedBlob() []zitem {
	var items []zitem
	c.blob.each(func(member, score []byte) { items = append(items, zitem{string(member), score}) })
	slices.SortFunc(items, compareZItems)
	return items
}

func (c *collection) countBelow(below func(score []byte, member string) bool) (int, error) {
	if c.table {
		return c.tx.MemberCount(c.key, below)
	}
	items := c.sortedBlob()
	return sort.Search(len(items), func(i int) bool { return !below(items[i].score, items[i].member) }), nil
}

func (c *collection) walk(from int, reverse bool, fn func(zitem) bool) error {
	if c.table {
		return c.tx.MemberRange(c.key, from, reverse, func(member string, score []byte) bool {
			return fn(zitem{member, score})
		})
	}
	items := c.sortedBlob()
	for i := from; 0 <= i && i < len(items) && fn(items[i]); {
		if reverse {
			i--
		} else {
			i++
		}
	}
	return nil
}

type zspan struct {
	under, upTo func(score []byte, member string) bool
}

type bound struct {
	open bool
	cmp  func(score []byte, member string) int
}

func spanOf(lo, hi bound) zspan {
	return zspan{
		under: func(s []byte, m string) bool {
			c := lo.cmp(s, m)
			return c < 0 || c == 0 && lo.open
		},
		upTo: func(s []byte, m string) bool {
			c := hi.cmp(s, m)
			return c < 0 || c == 0 && !hi.open
		},
	}
}

func scoreBound(b []byte) (bound, bool) {
	open := len(b) > 0 && b[0] == '('
	if open {
		b = b[1:]
	}
	f, ok := parseFloat(b)
	k := scoreKey(f)
	return bound{open: open, cmp: func(s []byte, _ string) int { return bytes.Compare(s, k) }}, ok
}

func scoreSpan(minArg, maxArg []byte) (zspan, reply) {
	lo, ok1 := scoreBound(minArg)
	hi, ok2 := scoreBound(maxArg)
	if !ok1 || !ok2 {
		return zspan{}, errorReply("ERR min or max is not a float")
	}
	return spanOf(lo, hi), nil
}

func lexBound(b []byte) (bound, bool) {
	switch {
	case string(b) == "-":
		return bound{cmp: func([]byte, string) int { return 1 }}, true
	case string(b) == "+":
		return bound{cmp: func([]byte, string) int { return -1 }}, true
	case len(b) > 0 && (b[0] == '[' || b[0] == '('):
		v := string(b[1:])
		return bound{open: b[0] == '(', cmp: func(_ []byte, m string) int { return strings.Compare(m, v) }}, true
	}
	return bound{}, false
}

func lexSpan(minArg, maxArg []byte) (zspan, reply) {
	lo, ok1 := lexBound(minArg)
	hi, ok2 := lexBound(maxArg)
	if !ok1 || !ok2 {
		return zspan{}, errorReply("ERR min or max not valid string range item")
	}
	return spanOf(lo, hi), nil
}

type zquery struct {
	byRank        bool
	start, stop   int64
	span          zspan
	reverse       bool
	offset, limit int64
}

func (c *collection) query(q zquery) ([]zitem, error) {
	var items []zitem
	n := int64(c.len())
	if q.byRank {
		start, stop := q.start, q.stop
		if start < 0 {
			start += n
		}
		if stop < 0 {
			stop += n
		}
		start, stop = max(start, 0), min(stop, n-1)
		if start > stop {
			return nil, nil
		}
		from := start
		if q.reverse {
			from = n - 1 - start
		}
		err := c.walk(int(from), q.reverse, func(it zitem) bool {
			items = append(items, it)
			return int64(len(items)) <= stop-start
		})
		return items, err
	}
	if q.offset < 0 || q.offset >= n || q.limit == 0 {
		return nil, nil
	}
	below, while := q.span.under, q.span.upTo
	if q.reverse {
		below, while = q.span.upTo, func(s []byte, m string) bool { return !q.span.under(s, m) }
	}
	from, err := c.countBelow(below)
	if err != nil {
		return nil, err
	}
	if q.reverse {
		from -= 1 + int(q.offset)
	} else {
		from += int(q.offset)
	}
	err = c.walk(from, q.reverse, func(it zitem) bool {
		if !while(it.score, it.member) {
			return false
		}
		items = append(items, it)
		return q.limit < 0 || int64(len(items)) < q.limit
	})
	return items, err
}

func storeZSet(tx *bitcask.Tx, dest []byte, items []zitem) (reply, error) {
	tx.Delete(string(dest))
	z, _, err := openZSet(tx, dest)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if _, err := z.set([]byte(it.member), it.score); err != nil {
			return nil, err
		}
	}
	z.store()
	return intReply(len(items)), nil
}

type zaddOptions struct {
	nx, xx, gt, lt, ch, incr bool
}

func cmdZAdd(tx *bitcask.Tx, args [][]byte) (reply, error) {
	var o zaddOptions
	i := 2
options:
	for ; i < len(args); i++ {
		switch upper(args[i]) {
		case "NX":
			o.nx = true
		case "XX":
			o.xx = true
		case "GT":
			o.gt = true
		case "LT":
			o.lt = true
		case "CH":
			o.ch = true
		case "INCR":
			o.incr = true
		default:
			break options
		}
	}
	return zadd(tx, args[1], args[i:], o)
}

func cmdZIncrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return zadd(tx, args[1], args[2:], zaddOptions{incr: true})
}

func zadd(tx *bitcask.Tx, key []byte, pairs [][]byte, o zaddOptions) (reply, error) {
	switch {
	case len(pairs) == 0 || len(pairs)%2 == 1:
		return errorReply(errSyntax), nil
	case o.nx && o.xx:
		return errorReply("ERR XX and NX options at the same time are not compatible"), nil
	case o.nx && (o.gt || o.lt) || o.gt && o.lt:
		return errorReply("ERR GT, LT, and/or NX options at the same time are not compatible"), nil
	case o.incr && len(pairs) > 2:
		return errorReply("ERR INCR option supports a single increment-element pair"), nil
	}
	scores := make([]float64, len(pairs)/2)
	members := make([][]byte, len(pairs)/2)
	for j := range scores {
		var ok bool
		if scores[j], ok = parseFloat(pairs[2*j]); !ok {
			return errorReply(errNotFloat), nil
		}
		members[j] = pairs[2*j+1]
	}
	return addScores(tx, key, scores, members, o)
}

func addScores(tx *bitcask.Tx, key []byte, scores []float64, members [][]byte, o zaddOptions) (reply, error) {
	z, bad, err := openZSet(tx, key)
	if bad != nil || err != nil {
		return bad, err
	}
	added, updated, processed := 0, 0, 0
	var result float64
	for j, score := range scores {
		member := members[j]
		value, found, err := z.get(member)
		if err != nil {
			return nil, err
		}
		if found && o.nx || !found && o.xx {
			continue
		}
		var current float64
		if found {
			current = keyScore(value)
			if o.incr {
				if score += current; math.IsNaN(score) {
					return errorReply("ERR resulting score is not a number (NaN)"), nil
				}
			}
			if o.gt && score <= current || o.lt && score >= current {
				continue
			}
		}
		processed++
		result = score
		switch {
		case !found:
			added++
		case score == current:
			continue
		default:
			updated++
		}
		if _, err := z.set(member, scoreKey(score)); err != nil {
			return nil, err
		}
	}
	z.store()
	switch {
	case o.incr && processed == 0:
		return nilReply, nil
	case o.incr:
		return doubleReply(formatScore(result)), nil
	case o.ch:
		return intReply(added + updated), nil
	}
	return intReply(added), nil
}

func cmdZRem(tx *bitcask.Tx, args [][]byte) (reply, error) {
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	removed := 0
	for _, member := range args[2:] {
		if z.del(member) {
			removed++
		}
	}
	z.store()
	return intReply(removed), nil
}

func cmdZScore(tx *bitcask.Tx, args [][]byte) (reply, error) {
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	score, found, err := z.get(args[2])
	if err != nil || !found {
		return nilReply, err
	}
	return scoreReply(score), nil
}

func cmdZMScore(tx *bitcask.Tx, args [][]byte) (reply, error) {
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	out := make(arrayReply, len(args)-2)
	for i, member := range args[2:] {
		score, found, err := z.get(member)
		if err != nil {
			return nil, err
		}
		out[i] = nilReply
		if found {
			out[i] = scoreReply(score)
		}
	}
	return out, nil
}

func cmdZCard(tx *bitcask.Tx, args [][]byte) (reply, error) {
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(z.len()), nil
}

func zcount(parse func(minArg, maxArg []byte) (zspan, reply)) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		span, bad := parse(args[2], args[3])
		if bad != nil {
			return bad, nil
		}
		z, bad, err := openZSet(tx, args[1])
		if bad != nil || err != nil {
			return bad, err
		}
		hi, err := z.countBelow(span.upTo)
		if err != nil {
			return nil, err
		}
		lo, err := z.countBelow(span.under)
		return intReply(max(hi-lo, 0)), err
	}
}

var (
	cmdZCount    = zcount(scoreSpan)
	cmdZLexCount = zcount(lexSpan)
)

func zrank(reverse bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		if len(args) > 4 {
			return errorReply("ERR wrong number of arguments for '" + strings.ToLower(string(args[0])) + "' command"), nil
		}
		withScore := len(args) == 4
		if withScore && upper(args[3]) != "WITHSCORE" {
			return errorReply(errSyntax), nil
		}
		var missing reply = nilReply
		if withScore {
			missing = nullArrayReply{}
		}
		z, bad, err := openZSet(tx, args[1])
		if bad != nil || err != nil {
			return bad, err
		}
		score, found, err := z.get(args[2])
		if err != nil || !found {
			return missing, err
		}
		member := string(args[2])
		rank, err := z.countBelow(func(s []byte, m string) bool {
			c := bytes.Compare(s, score)
			return c < 0 || c == 0 && m < member
		})
		if err != nil {
			return nil, err
		}
		if reverse {
			rank = z.len() - 1 - rank
		}
		if withScore {
			return arrayReply{intReply(rank), scoreReply(score)}, nil
		}
		return intReply(rank), nil
	}
}

var (
	cmdZRank    = zrank(false)
	cmdZRevRank = zrank(true)
)

const (
	byRank = iota + 1
	byScore
	byLex
)

func zrange(src int, by int, reverse bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		by, reverse := by, reverse
		store := src == 2
		chooseBy, chooseDirection := by == 0, by == 0
		var withScores bool
		q := zquery{limit: -1}
		for j := src + 3; j < len(args); j++ {
			switch opt := upper(args[j]); {
			case !store && opt == "WITHSCORES":
				withScores = true
			case opt == "LIMIT" && j+2 < len(args):
				var ok1, ok2 bool
				q.offset, ok1 = parseInt(args[j+1])
				q.limit, ok2 = parseInt(args[j+2])
				if !ok1 || !ok2 {
					return errorReply(errNotInteger), nil
				}
				j += 2
			case chooseDirection && opt == "REV":
				reverse, chooseDirection = true, false
			case chooseBy && opt == "BYSCORE":
				by, chooseBy = byScore, false
			case chooseBy && opt == "BYLEX":
				by, chooseBy = byLex, false
			default:
				return errorReply(errSyntax), nil
			}
		}
		if by == 0 {
			by = byRank
		}
		if q.limit != -1 && by == byRank {
			return errorReply("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"), nil
		}
		if withScores && by == byLex {
			return errorReply("ERR syntax error, WITHSCORES not supported in combination with BYLEX"), nil
		}
		minArg, maxArg := args[src+1], args[src+2]
		if reverse && by != byRank {
			minArg, maxArg = maxArg, minArg
		}
		q.reverse = reverse
		var bad reply
		switch by {
		case byRank:
			var ok1, ok2 bool
			q.start, ok1 = parseInt(minArg)
			q.stop, ok2 = parseInt(maxArg)
			if !ok1 || !ok2 {
				return errorReply(errNotInteger), nil
			}
			q.byRank = true
		case byScore:
			q.span, bad = scoreSpan(minArg, maxArg)
		case byLex:
			q.span, bad = lexSpan(minArg, maxArg)
		}
		if bad != nil {
			return bad, nil
		}
		z, bad, err := openZSet(tx, args[src])
		if bad != nil || err != nil {
			return bad, err
		}
		items, err := z.query(q)
		if err != nil {
			return nil, err
		}
		if store {
			return storeZSet(tx, args[1], items)
		}
		return scoredReply(items, withScores), nil
	}
}

var (
	cmdZRange           = zrange(1, 0, false)
	cmdZRangeStore      = zrange(2, 0, false)
	cmdZRevRange        = zrange(1, byRank, true)
	cmdZRangeByScore    = zrange(1, byScore, false)
	cmdZRevRangeByScore = zrange(1, byScore, true)
	cmdZRangeByLex      = zrange(1, byLex, false)
	cmdZRevRangeByLex   = zrange(1, byLex, true)
)

func zremRange(by int) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		q := zquery{limit: -1}
		var bad reply
		switch by {
		case byRank:
			var ok1, ok2 bool
			q.start, ok1 = parseInt(args[2])
			q.stop, ok2 = parseInt(args[3])
			if !ok1 || !ok2 {
				return errorReply(errNotInteger), nil
			}
			q.byRank = true
		case byScore:
			q.span, bad = scoreSpan(args[2], args[3])
		case byLex:
			q.span, bad = lexSpan(args[2], args[3])
		}
		if bad != nil {
			return bad, nil
		}
		z, bad, err := openZSet(tx, args[1])
		if bad != nil || err != nil {
			return bad, err
		}
		items, err := z.query(q)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			z.del([]byte(it.member))
		}
		z.store()
		return intReply(len(items)), nil
	}
}

var (
	cmdZRemRangeByRank  = zremRange(byRank)
	cmdZRemRangeByScore = zremRange(byScore)
	cmdZRemRangeByLex   = zremRange(byLex)
)

func (c *collection) pop(count int64, highest bool) ([]zitem, error) {
	items, err := c.query(zquery{byRank: true, stop: count - 1, reverse: highest})
	for _, it := range items {
		c.del([]byte(it.member))
	}
	c.store()
	return items, err
}

func zpop(highest bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		if len(args) > 3 {
			return errorReply(errSyntax), nil
		}
		count := int64(1)
		if len(args) == 3 {
			var ok bool
			if count, ok = parseInt(args[2]); !ok || count < 0 {
				return errorReply("ERR value is out of range, must be positive"), nil
			}
		}
		z, bad, err := openZSet(tx, args[1])
		if bad != nil || err != nil {
			return bad, err
		}
		if count == 0 {
			return arrayReply{}, nil
		}
		items, err := z.pop(count, highest)
		if len(args) == 3 {
			return pairsReply(itemsReply(items, true)), err
		}
		return itemsReply(items, true), err
	}
}

var (
	cmdZPopMin = zpop(false)
	cmdZPopMax = zpop(true)
)

func cmdZMPop(tx *bitcask.Tx, args [][]byte) (reply, error) {
	p, bad := parseMPop(args, 1, "MIN", "MAX")
	if bad != nil {
		return bad, nil
	}
	return zmpop(tx, p)
}

func zmpop(tx *bitcask.Tx, p mpop) (reply, error) {
	for _, key := range p.keys {
		z, bad, err := openZSet(tx, key)
		if bad != nil || err != nil {
			return bad, err
		}
		if z.len() == 0 {
			continue
		}
		items, err := z.pop(p.count, !p.first)
		pairs := make(arrayReply, len(items))
		for i, it := range items {
			pairs[i] = arrayReply{bulkReply(it.member), scoreReply(it.score)}
		}
		return arrayReply{bulkReply(key), pairs}, err
	}
	return nullArrayReply{}, nil
}

func cmdZRandMember(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return randomMembers(tx, args, "WITHSCORES", openZSet, scoreReply)
}

func cmdZScan(tx *bitcask.Tx, args [][]byte) (reply, error) {
	match, bad := parseScan(args, nil)
	if bad != nil {
		return bad, nil
	}
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	items := arrayReply{}
	err = z.each(true, func(member, score []byte) {
		if match(member) {
			items = append(items, bulkReply(member), bulkReply(formatScore(keyScore(score))))
		}
	})
	return arrayReply{bulkReply("0"), items}, err
}

const (
	opUnion = iota
	opInter
	opDiff
)

type zsource struct {
	c      *collection
	weight float64
}

func (s zsource) score(member []byte) (float64, bool, error) {
	if s.c.typ == typeSet {
		found, err := s.c.has(member)
		return s.weighted(1), found, err
	}
	v, found, err := s.c.get(member)
	if err != nil || !found {
		return 0, false, err
	}
	return s.weighted(keyScore(v)), true, nil
}

func (s zsource) each(fn func(member []byte, score float64)) error {
	scored := s.c.typ == typeZSet
	return s.c.each(scored, func(member, v []byte) {
		score := 1.0
		if scored {
			score = keyScore(v)
		}
		fn(member, s.weighted(score))
	})
}

func (s zsource) weighted(score float64) float64 {
	if score = s.weight * score; math.IsNaN(score) {
		return 0
	}
	return score
}

func aggregate(how string, total, score float64) float64 {
	switch how {
	case "MIN":
		return min(total, score)
	case "MAX":
		return max(total, score)
	}
	if total += score; math.IsNaN(total) {
		return 0
	}
	return total
}

func openZSource(tx *bitcask.Tx, key []byte) (*collection, reply, error) {
	_, kind, _, err := tx.GetKind(string(key))
	if err != nil {
		return nil, nil, err
	}
	if kind&^bitcask.Table == typeSet {
		return openSet(tx, key)
	}
	return openZSet(tx, key)
}

func zsetAlgebra(op int, store, card bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		numIndex := 1
		if store {
			numIndex = 2
		}
		numkeys, ok := parseInt(args[numIndex])
		if !ok {
			return errorReply(errNotInteger), nil
		}
		if numkeys < 1 {
			return errorReply("ERR at least 1 input key is needed for '" + strings.ToLower(string(args[0])) + "' command"), nil
		}
		if numkeys > int64(len(args)-numIndex-1) {
			return errorReply(errSyntax), nil
		}
		keys := args[numIndex+1 : numIndex+1+int(numkeys)]
		srcs := make([]zsource, len(keys))
		for i, key := range keys {
			c, bad, err := openZSource(tx, key)
			if bad != nil || err != nil {
				return bad, err
			}
			srcs[i] = zsource{c, 1}
		}
		how := "SUM"
		var withScores bool
		var limit int64
		for j := numIndex + 1 + len(keys); j < len(args); j++ {
			rest := len(args) - j
			switch opt := upper(args[j]); {
			case op != opDiff && !card && opt == "WEIGHTS" && rest > len(srcs):
				for i := range srcs {
					j++
					if srcs[i].weight, ok = parseFloat(args[j]); !ok {
						return errorReply("ERR weight value is not a float"), nil
					}
				}
			case op != opDiff && !card && opt == "AGGREGATE" && rest >= 2:
				j++
				if how = upper(args[j]); how != "SUM" && how != "MIN" && how != "MAX" {
					return errorReply(errSyntax), nil
				}
			case !store && !card && opt == "WITHSCORES":
				withScores = true
			case card && opt == "LIMIT" && rest >= 2:
				j++
				if limit, ok = parseInt(args[j]); !ok || limit < 0 {
					return errorReply("ERR LIMIT can't be negative"), nil
				}
			default:
				return errorReply(errSyntax), nil
			}
		}
		scores := make(map[string]float64)
		var err error
		switch op {
		case opUnion:
			for _, s := range srcs {
				err = s.each(func(member []byte, score float64) {
					if total, ok := scores[string(member)]; ok {
						score = aggregate(how, total, score)
					}
					scores[string(member)] = score
				})
				if err != nil {
					return nil, err
				}
			}
		case opInter:
			slices.SortStableFunc(srcs, func(a, b zsource) int { return a.c.len() - b.c.len() })
			var members [][]byte
			var first []float64
			if err = srcs[0].each(func(member []byte, score float64) {
				members, first = append(members, member), append(first, score)
			}); err != nil {
				return nil, err
			}
		members:
			for i, member := range members {
				total := first[i]
				for _, s := range srcs[1:] {
					score, found, err := s.score(member)
					if err != nil {
						return nil, err
					}
					if !found {
						continue members
					}
					total = aggregate(how, total, score)
				}
				scores[string(member)] = total
				if card && int64(len(scores)) == limit {
					break
				}
			}
		case opDiff:
			if err = srcs[0].each(func(member []byte, score float64) {
				scores[string(member)] = score
			}); err != nil {
				return nil, err
			}
			for member := range scores {
				for _, s := range srcs[1:] {
					_, found, err := s.score([]byte(member))
					if err != nil {
						return nil, err
					}
					if found {
						delete(scores, member)
						break
					}
				}
			}
		}
		if card {
			return intReply(len(scores)), nil
		}
		items := make([]zitem, 0, len(scores))
		for member, score := range scores {
			items = append(items, zitem{member, scoreKey(score)})
		}
		slices.SortFunc(items, compareZItems)
		if store {
			return storeZSet(tx, args[1], items)
		}
		return scoredReply(items, withScores), nil
	}
}

var (
	cmdZUnion      = zsetAlgebra(opUnion, false, false)
	cmdZInter      = zsetAlgebra(opInter, false, false)
	cmdZDiff       = zsetAlgebra(opDiff, false, false)
	cmdZInterCard  = zsetAlgebra(opInter, false, true)
	cmdZUnionStore = zsetAlgebra(opUnion, true, false)
	cmdZInterStore = zsetAlgebra(opInter, true, false)
	cmdZDiffStore  = zsetAlgebra(opDiff, true, false)
)
