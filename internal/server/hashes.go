package server

import (
	"math"
	"math/rand/v2"
	"strconv"

	"github.com/acneism/casketdb/internal/bitcask"
)

var hashCommands = map[string]command{
	"hset":         {arity: -4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHSet},
	"hmset":        {arity: -4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHMSet},
	"hsetnx":       {arity: 4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHSetNX},
	"hget":         {arity: 3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHGet},
	"hmget":        {arity: -3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHMGet},
	"hdel":         {arity: -3, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHDel},
	"hlen":         {arity: 2, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHLen},
	"hexists":      {arity: 3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHExists},
	"hstrlen":      {arity: 3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHStrlen},
	"hgetall":      {arity: 2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHGetAll},
	"hkeys":        {arity: 2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHKeys},
	"hvals":        {arity: 2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHVals},
	"hincrby":      {arity: 4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHIncrBy},
	"hincrbyfloat": {arity: 4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHIncrByFloat},
	"hscan":        {arity: -3, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHScan},
	"hrandfield":   {arity: -2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHRandField},
}

func openHash(tx *bitcask.Tx, key []byte) (*collection, reply, error) {
	return openCollection(tx, key, typeHash)
}

func hset(tx *bitcask.Tx, args [][]byte, name string) (int, reply, error) {
	if len(args)%2 == 1 {
		return 0, errorReply("ERR wrong number of arguments for '" + name + "' command"), nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return 0, bad, err
	}
	added := 0
	for i := 2; i < len(args); i += 2 {
		isNew, err := h.set(args[i], args[i+1])
		if err != nil {
			return 0, nil, err
		}
		if isNew {
			added++
		}
	}
	h.store()
	return added, nil, nil
}

func cmdHSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	added, bad, err := hset(tx, args, "hset")
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(added), nil
}

func cmdHMSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	_, bad, err := hset(tx, args, "hmset")
	if bad != nil || err != nil {
		return bad, err
	}
	return okReply, nil
}

func cmdHSetNX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if _, found, err := h.get(args[2]); err != nil || found {
		return intReply(0), err
	}
	if _, err := h.set(args[2], args[3]); err != nil {
		return nil, err
	}
	h.store()
	return intReply(1), nil
}

func cmdHGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, found, err := h.get(args[2])
	if err != nil || !found {
		return nilReply, err
	}
	return bulkReply(value), nil
}

func cmdHMGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	out := make(arrayReply, len(args)-2)
	for i, field := range args[2:] {
		value, found, err := h.get(field)
		if err != nil {
			return nil, err
		}
		out[i] = nilReply
		if found {
			out[i] = bulkReply(value)
		}
	}
	return out, nil
}

func cmdHDel(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	deleted := 0
	for _, field := range args[2:] {
		if h.del(field) {
			deleted++
		}
	}
	h.store()
	return intReply(deleted), nil
}

func cmdHLen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(h.len()), nil
}

func cmdHExists(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	_, found, err := h.get(args[2])
	if err != nil || !found {
		return intReply(0), err
	}
	return intReply(1), nil
}

func cmdHStrlen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, _, err := h.get(args[2])
	return intReply(len(value)), err
}

func hashItems(tx *bitcask.Tx, key []byte, fields, values bool) (reply, error) {
	h, bad, err := openHash(tx, key)
	if bad != nil || err != nil {
		return bad, err
	}
	out := arrayReply{}
	err = h.each(values, func(field, value []byte) {
		if fields {
			out = append(out, bulkReply(field))
		}
		if values {
			out = append(out, bulkReply(value))
		}
	})
	if fields && values {
		return mapReply(out), err
	}
	return out, err
}

func cmdHGetAll(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return hashItems(tx, args[1], true, true)
}

func cmdHKeys(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return hashItems(tx, args[1], true, false)
}

func cmdHVals(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return hashItems(tx, args[1], false, true)
}

func cmdHIncrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	incr, ok := parseInt(args[3])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, found, err := h.get(args[2])
	if err != nil {
		return nil, err
	}
	var current int64
	if found {
		if current, ok = parseInt(value); !ok {
			return errorReply("ERR hash value is not an integer"), nil
		}
	}
	if (incr > 0 && current > math.MaxInt64-incr) || (incr < 0 && current < math.MinInt64-incr) {
		return errorReply(errOverflow), nil
	}
	if _, err := h.set(args[2], strconv.AppendInt(nil, current+incr, 10)); err != nil {
		return nil, err
	}
	h.store()
	return intReply(current + incr), nil
}

func cmdHIncrByFloat(tx *bitcask.Tx, args [][]byte) (reply, error) {
	incr, ok := parseFloat(args[3])
	if !ok {
		return errorReply(errNotFloat), nil
	}
	if math.IsInf(incr, 0) {
		return errorReply("ERR value is NaN or Infinity"), nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, found, err := h.get(args[2])
	if err != nil {
		return nil, err
	}
	var current float64
	if found {
		if current, ok = parseFloat(value); !ok {
			return errorReply("ERR hash value is not a float"), nil
		}
	}
	result := current + incr
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return errorReply(errFloatEdge), nil
	}
	if result == 0 {
		result = 0
	}
	out := strconv.AppendFloat(nil, result, 'f', -1, 64)
	if _, err := h.set(args[2], out); err != nil {
		return nil, err
	}
	h.store()
	return bulkReply(out), nil
}

func parseScan(args [][]byte, values *bool) (func([]byte) bool, reply) {
	if _, err := strconv.ParseUint(string(args[2]), 10, 64); err != nil {
		return nil, errorReply("ERR invalid cursor")
	}
	match := func([]byte) bool { return true }
	for i := 3; i < len(args); i++ {
		switch opt := upper(args[i]); {
		case opt == "NOVALUES" && values != nil:
			*values = false
		case opt == "MATCH" && i+1 < len(args):
			pattern := string(args[i+1])
			match = func(field []byte) bool { return matchGlob(pattern, string(field)) }
			i++
		case opt == "COUNT" && i+1 < len(args):
			if n, ok := parseInt(args[i+1]); !ok {
				return nil, errorReply(errNotInteger)
			} else if n < 1 {
				return nil, errorReply(errSyntax)
			}
			i++
		default:
			return nil, errorReply(errSyntax)
		}
	}
	return match, nil
}

func cmdHScan(tx *bitcask.Tx, args [][]byte) (reply, error) {
	values := true
	match, bad := parseScan(args, &values)
	if bad != nil {
		return bad, nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	items := arrayReply{}
	err = h.each(values, func(field, value []byte) {
		if !match(field) {
			return
		}
		items = append(items, bulkReply(field))
		if values {
			items = append(items, bulkReply(value))
		}
	})
	return arrayReply{bulkReply("0"), items}, err
}

func randomCount(args [][]byte) (int64, reply) {
	count, ok := parseInt(args[2])
	if !ok {
		return 0, errorReply(errNotInteger)
	}
	if count < -maxRandomCount || count > maxRandomCount {
		return 0, errorReply("ERR value is out of range")
	}
	return count, nil
}

func cmdHRandField(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return randomMembers(tx, args, "WITHVALUES", openHash, func(v []byte) reply { return bulkReply(v) })
}

func randomMembers(tx *bitcask.Tx, args [][]byte, option string, open func(*bitcask.Tx, []byte) (*collection, reply, error), format func([]byte) reply) (reply, error) {
	if len(args) > 4 || (len(args) == 4 && upper(args[3]) != option) {
		return errorReply(errSyntax), nil
	}
	var count int64
	if len(args) >= 3 {
		var bad reply
		if count, bad = randomCount(args); bad != nil {
			return bad, nil
		}
	}
	c, bad, err := open(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	var fields, values [][]byte
	if err := c.each(len(args) == 4, func(field, value []byte) {
		fields, values = append(fields, field), append(values, value)
	}); err != nil {
		return nil, err
	}
	if len(args) == 2 {
		if len(fields) == 0 {
			return nilReply, nil
		}
		return bulkReply(fields[rand.IntN(len(fields))]), nil
	}
	out := arrayReply{}
	for _, i := range randomPicks(len(fields), count) {
		out = append(out, bulkReply(fields[i]))
		if len(args) == 4 {
			out = append(out, format(values[i]))
		}
	}
	if len(args) == 4 {
		return pairsReply(out), nil
	}
	return out, nil
}
