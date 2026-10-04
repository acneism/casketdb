package server

import (
	"errors"
	"math"
	"strconv"

	"github.com/acneism/casketdb/internal/bitcask"
)

var stringCommands = map[string]command{
	"get":    {arity: 2, kind: kindRead, keys: oneKey, acl: catString | catFast, tx: cmdGet},
	"set":    {arity: -3, kind: kindWrite, keys: oneKey, acl: catString, tx: cmdSet},
	"setnx":  {arity: 3, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdSetNX},
	"setex":  {arity: 4, kind: kindWrite, keys: oneKey, acl: catString, tx: cmdSetEX},
	"psetex": {arity: 4, kind: kindWrite, keys: oneKey, acl: catString, tx: cmdPSetEX},
	"getdel": {arity: 2, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdGetDel},
	"mget":   {arity: -2, kind: kindRead, keys: allArgs, acl: catString | catFast, tx: cmdMGet},
	"mset":   {arity: -3, kind: kindWrite, keys: pairs, acl: catString, tx: cmdMSet},
	"append": {arity: 3, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdAppend},
	"strlen": {arity: 2, kind: kindRead, keys: oneKey, acl: catString | catFast, tx: cmdStrlen},
	"incr":   {arity: 2, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdIncr},
	"decr":   {arity: 2, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdDecr},
	"incrby": {arity: 3, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdIncrBy},
	"decrby": {arity: 3, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdDecrBy},

	"getset":      {arity: 3, kind: kindWrite, keys: oneKey, acl: catString, tx: cmdGetSet},
	"getex":       {arity: -2, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdGetEx},
	"getrange":    {arity: 4, kind: kindRead, keys: oneKey, acl: catString, tx: cmdGetRange},
	"setrange":    {arity: 4, kind: kindWrite, keys: oneKey, acl: catString, tx: cmdSetRange},
	"incrbyfloat": {arity: 3, kind: kindWrite, keys: oneKey, acl: catString | catFast, tx: cmdIncrByFloat},
	"msetnx":      {arity: -3, kind: kindWrite, keys: pairs, acl: catString, tx: cmdMSetNX},
	"lcs":         {arity: -3, kind: kindRead, keys: keySpec{first: 1, last: 2, step: 1}, acl: catString, tx: cmdLCS},
}

const (
	maxString    = 512 << 20
	errTooBig    = "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
	errNotFloat  = "ERR value is not a valid float"
	errFloatEdge = "ERR increment would produce NaN or Infinity"
)

func cmdGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	value, found, err := tx.Get(string(args[1]))
	if err != nil {
		return readError(err)
	}
	if !found {
		return nilReply, nil
	}
	return bulkReply(value), nil
}

func cmdSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key, value := string(args[1]), args[2]
	var nx, xx, keepTTL, get, hasExpire bool
	var expireAt int64
	for i := 3; i < len(args); i++ {
		switch opt := upper(args[i]); opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpire || i+1 >= len(args) {
				return errorReply(errSyntax), nil
			}
			at, bad := expireArg(tx.Now(), opt, args[i+1], "set")
			if bad != nil {
				return bad, nil
			}
			expireAt, hasExpire = at, true
			i++
		default:
			return errorReply(errSyntax), nil
		}
	}
	if (nx && xx) || (keepTTL && hasExpire) {
		return errorReply(errSyntax), nil
	}
	var result reply = okReply
	existed := tx.Exists(key)
	if get {
		old, found, err := tx.Get(key)
		if err != nil {
			return readError(err)
		}
		result, existed = nilReply, found
		if found {
			result = bulkReply(old)
		}
	}
	if (nx && existed) || (xx && !existed) {
		if !get {
			result = nilReply
		}
		return result, nil
	}
	if keepTTL {
		expireAt, _ = tx.ExpireAt(key)
	}
	tx.Put(key, value, expireAt)
	return result, nil
}

func cmdSetNX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	if tx.Exists(key) {
		return intReply(0), nil
	}
	tx.Put(key, args[2], 0)
	return intReply(1), nil
}

func cmdSetEX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return setWithExpire(tx, args, "EX", "setex")
}

func cmdPSetEX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return setWithExpire(tx, args, "PX", "psetex")
}

func setWithExpire(tx *bitcask.Tx, args [][]byte, unit, name string) (reply, error) {
	at, bad := expireArg(tx.Now(), unit, args[2], name)
	if bad != nil {
		return bad, nil
	}
	tx.Put(string(args[1]), args[3], at)
	return okReply, nil
}

func cmdGetDel(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	value, found, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	if !found {
		return nilReply, nil
	}
	tx.Delete(key)
	return bulkReply(value), nil
}

func cmdMGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	out := make(arrayReply, len(args)-1)
	for i, key := range args[1:] {
		value, found, err := tx.Get(string(key))
		if err != nil && !errors.Is(err, bitcask.ErrWrongKind) {
			return nil, err
		}
		if found {
			out[i] = bulkReply(value)
		} else {
			out[i] = nilReply
		}
	}
	return out, nil
}

func cmdMSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if len(args)%2 != 1 {
		return errorReply("ERR wrong number of arguments for 'mset' command"), nil
	}
	for i := 1; i < len(args); i += 2 {
		tx.Put(string(args[i]), args[i+1], 0)
	}
	return okReply, nil
}

func cmdAppend(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	old, _, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	if len(old)+len(args[2]) > maxString {
		return errorReply(errTooBig), nil
	}
	expireAt, _ := tx.ExpireAt(key)
	value := make([]byte, 0, len(old)+len(args[2]))
	value = append(value, old...)
	value = append(value, args[2]...)
	tx.Put(key, value, expireAt)
	return intReply(len(value)), nil
}

func cmdStrlen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	value, _, err := tx.Get(string(args[1]))
	if err != nil {
		return readError(err)
	}
	return intReply(len(value)), nil
}

func cmdIncr(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return incrBy(tx, args[1], 1)
}

func cmdDecr(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return incrBy(tx, args[1], -1)
}

func cmdIncrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	n, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	return incrBy(tx, args[1], n)
}

func cmdDecrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	n, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	if n == math.MinInt64 {
		return errorReply("ERR decrement would overflow"), nil
	}
	return incrBy(tx, args[1], -n)
}

func incrBy(tx *bitcask.Tx, rawKey []byte, delta int64) (reply, error) {
	key := string(rawKey)
	value, found, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	var current int64
	if found {
		n, ok := parseInt(value)
		if !ok {
			return errorReply(errNotInteger), nil
		}
		current = n
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return errorReply(errOverflow), nil
	}
	result := current + delta
	expireAt, _ := tx.ExpireAt(key)
	tx.Put(key, strconv.AppendInt(nil, result, 10), expireAt)
	return intReply(result), nil
}

func expireArg(now int64, unit string, arg []byte, cmd string) (int64, reply) {
	n, ok := parseInt(arg)
	if !ok {
		return 0, errorReply(errNotInteger)
	}
	at, ok := expireTime(now, n, unit)
	if !ok || n <= 0 {
		return 0, errorReply("ERR invalid expire time in '" + cmd + "' command")
	}
	return at, nil
}

func cmdGetSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return cmdSet(tx, [][]byte{args[0], args[1], args[2], []byte("GET")})
}

func cmdGetEx(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	var expireAt int64
	var set, persist bool
	for i := 2; i < len(args); i++ {
		switch opt := upper(args[i]); opt {
		case "PERSIST":
			if set || persist {
				return errorReply(errSyntax), nil
			}
			persist = true
		case "EX", "PX", "EXAT", "PXAT":
			if set || persist || i+1 >= len(args) {
				return errorReply(errSyntax), nil
			}
			at, bad := expireArg(tx.Now(), opt, args[i+1], "getex")
			if bad != nil {
				return bad, nil
			}
			expireAt, set = at, true
			i++
		default:
			return errorReply(errSyntax), nil
		}
	}
	value, found, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	if !found {
		return nilReply, nil
	}
	if at, _ := tx.ExpireAt(key); set || (persist && at != 0) {
		tx.Put(key, value, expireAt)
	}
	return bulkReply(value), nil
}

func byteRange(n, start, end int64) (int64, int64) {
	if start < 0 {
		start = max(n+start, 0)
	}
	if end < 0 {
		end = max(n+end, 0)
	}
	end = min(end, n-1)
	if start > end {
		return 0, 0
	}
	return start, end + 1
}

func cmdGetRange(tx *bitcask.Tx, args [][]byte) (reply, error) {
	start, ok1 := parseInt(args[2])
	end, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		return errorReply(errNotInteger), nil
	}
	value, _, err := tx.Get(string(args[1]))
	if err != nil {
		return readError(err)
	}
	if start < 0 && end < 0 && start > end {
		return bulkReply(""), nil
	}
	from, to := byteRange(int64(len(value)), start, end)
	return bulkReply(value[from:to]), nil
}

func cmdSetRange(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key, value := string(args[1]), args[3]
	offset, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	if offset < 0 {
		return errorReply("ERR offset is out of range"), nil
	}
	old, _, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	if len(value) == 0 {
		return intReply(len(old)), nil
	}
	if offset > maxString-int64(len(value)) {
		return errorReply(errTooBig), nil
	}
	buf := make([]byte, max(int64(len(old)), offset+int64(len(value))))
	copy(buf, old)
	copy(buf[offset:], value)
	expireAt, _ := tx.ExpireAt(key)
	tx.Put(key, buf, expireAt)
	return intReply(len(buf)), nil
}

func parseFloat(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	return f, err == nil && !math.IsNaN(f)
}

func cmdIncrByFloat(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	value, found, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	var current float64
	ok := true
	if found {
		current, ok = parseFloat(value)
	}
	incr, incrOK := parseFloat(args[2])
	if !ok || !incrOK {
		return errorReply(errNotFloat), nil
	}
	result := current + incr
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return errorReply(errFloatEdge), nil
	}
	if result == 0 {
		result = 0
	}
	out := strconv.AppendFloat(nil, result, 'f', -1, 64)
	expireAt, _ := tx.ExpireAt(key)
	tx.Put(key, out, expireAt)
	return bulkReply(out), nil
}

func cmdMSetNX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if len(args)%2 != 1 {
		return errorReply("ERR wrong number of arguments for 'msetnx' command"), nil
	}
	for i := 1; i < len(args); i += 2 {
		if tx.Exists(string(args[i])) {
			return intReply(0), nil
		}
	}
	for i := 1; i < len(args); i += 2 {
		tx.Put(string(args[i]), args[i+1], 0)
	}
	return intReply(1), nil
}

func cmdLCS(tx *bitcask.Tx, args [][]byte) (reply, error) {
	a, _, errA := tx.Get(string(args[1]))
	b, _, errB := tx.Get(string(args[2]))
	if err := errors.Join(errA, errB); err != nil {
		if errors.Is(err, bitcask.ErrWrongKind) {
			return errorReply("ERR The specified keys must contain string values"), nil
		}
		return nil, err
	}
	var getLen, getIdx, withLen bool
	var minLen int64
	for i := 3; i < len(args); i++ {
		switch upper(args[i]) {
		case "IDX":
			getIdx = true
		case "LEN":
			getLen = true
		case "WITHMATCHLEN":
			withLen = true
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return errorReply(errSyntax), nil
			}
			n, ok := parseInt(args[i+1])
			if !ok {
				return errorReply(errNotInteger), nil
			}
			minLen = max(n, 0)
			i++
		default:
			return errorReply(errSyntax), nil
		}
	}
	if getIdx && getLen {
		return errorReply("ERR If you want both the length and indexes, please just use IDX."), nil
	}
	if (int64(len(a))+1)*(int64(len(b))+1)*4 > maxString {
		return errorReply("ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len"), nil
	}
	cols := len(b) + 1
	lcs := make([]uint32, (len(a)+1)*cols)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				lcs[i*cols+j] = lcs[(i-1)*cols+j-1] + 1
			} else {
				lcs[i*cols+j] = max(lcs[(i-1)*cols+j], lcs[i*cols+j-1])
			}
		}
	}
	total := int64(lcs[len(a)*cols+len(b)])
	if getLen {
		return intReply(total), nil
	}
	result := make([]byte, total)
	matches := arrayReply{}
	k := total
	aStart, aEnd, bStart, bEnd := -1, 0, 0, 0
	for i, j := len(a), len(b); i > 0 && j > 0; {
		emit := false
		if a[i-1] == b[j-1] {
			result[k-1] = a[i-1]
			switch {
			case aStart == -1:
				aStart, aEnd, bStart, bEnd = i-1, i-1, j-1, j-1
			case aStart == i && bStart == j:
				aStart, bStart = aStart-1, bStart-1
			default:
				emit = true
			}
			emit = emit || aStart == 0 || bStart == 0
			k, i, j = k-1, i-1, j-1
		} else {
			if lcs[(i-1)*cols+j] > lcs[i*cols+j-1] {
				i--
			} else {
				j--
			}
			emit = aStart != -1
		}
		if emit {
			if n := int64(aEnd - aStart + 1); n >= minLen {
				match := arrayReply{arrayReply{intReply(aStart), intReply(aEnd)}, arrayReply{intReply(bStart), intReply(bEnd)}}
				if withLen {
					match = append(match, intReply(n))
				}
				matches = append(matches, match)
			}
			aStart = -1
		}
	}
	if getIdx {
		return mapReply{bulkReply("matches"), matches, bulkReply("len"), intReply(total)}, nil
	}
	return bulkReply(result), nil
}
