package server

import (
	"slices"

	"github.com/acneism/casketdb/internal/bitcask"
)

var setCommands = map[string]command{
	"sadd":        {arity: -3, kind: kindWrite, keys: oneKey, acl: catSet | catFast, tx: cmdSAdd},
	"srem":        {arity: -3, kind: kindWrite, keys: oneKey, acl: catSet | catFast, tx: cmdSRem},
	"sismember":   {arity: 3, kind: kindRead, keys: oneKey, acl: catSet | catFast, tx: cmdSIsMember},
	"smismember":  {arity: -3, kind: kindRead, keys: oneKey, acl: catSet | catFast, tx: cmdSMIsMember},
	"smembers":    {arity: 2, kind: kindRead, keys: oneKey, acl: catSet, tx: cmdSMembers},
	"scard":       {arity: 2, kind: kindRead, keys: oneKey, acl: catSet | catFast, tx: cmdSCard},
	"spop":        {arity: -2, kind: kindWrite, keys: oneKey, acl: catSet | catFast, tx: cmdSPop},
	"srandmember": {arity: -2, kind: kindRead, keys: oneKey, acl: catSet, tx: cmdSRandMember},
	"smove":       {arity: 4, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catSet | catFast, tx: cmdSMove},
	"sinter":      {arity: -2, kind: kindRead, keys: allArgs, acl: catSet, tx: cmdSInter},
	"sinterstore": {arity: -3, kind: kindWrite, keys: allArgs, acl: catSet, tx: cmdSInterStore},
	"sintercard":  {arity: -3, kind: kindRead, keys: keySpec{numkeys: 1}, acl: catSet, tx: cmdSInterCard},
	"sunion":      {arity: -2, kind: kindRead, keys: allArgs, acl: catSet, tx: cmdSUnion},
	"sunionstore": {arity: -3, kind: kindWrite, keys: allArgs, acl: catSet, tx: cmdSUnionStore},
	"sdiff":       {arity: -2, kind: kindRead, keys: allArgs, acl: catSet, tx: cmdSDiff},
	"sdiffstore":  {arity: -3, kind: kindWrite, keys: allArgs, acl: catSet, tx: cmdSDiffStore},
	"sscan":       {arity: -3, kind: kindRead, keys: oneKey, acl: catSet, tx: cmdSScan},
}

func openSet(tx *bitcask.Tx, key []byte) (*collection, reply, error) {
	return openCollection(tx, key, typeSet)
}

func (c *collection) members() ([][]byte, error) {
	var out [][]byte
	err := c.each(false, func(member, _ []byte) { out = append(out, member) })
	return out, err
}

func cmdSAdd(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	added := 0
	for _, member := range args[2:] {
		isNew, err := s.set(member, nil)
		if err != nil {
			return nil, err
		}
		if isNew {
			added++
		}
	}
	s.store()
	return intReply(added), nil
}

func cmdSRem(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	removed := 0
	for _, member := range args[2:] {
		if s.del(member) {
			removed++
		}
	}
	s.store()
	return intReply(removed), nil
}

func cmdSIsMember(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	found, err := s.has(args[2])
	if err != nil || !found {
		return intReply(0), err
	}
	return intReply(1), nil
}

func cmdSMIsMember(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	out := make(arrayReply, len(args)-2)
	for i, member := range args[2:] {
		found, err := s.has(member)
		if err != nil {
			return nil, err
		}
		out[i] = intReply(0)
		if found {
			out[i] = intReply(1)
		}
	}
	return out, nil
}

func membersReply(members [][]byte) arrayReply {
	out := make(arrayReply, len(members))
	for i, m := range members {
		out[i] = bulkReply(m)
	}
	return out
}

func cmdSMembers(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	members, err := s.members()
	return setReply(membersReply(members)), err
}

func cmdSCard(tx *bitcask.Tx, args [][]byte) (reply, error) {
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(s.len()), nil
}

func cmdSPop(tx *bitcask.Tx, args [][]byte) (reply, error) {
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
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	popped, _, err := s.random(count, false)
	if err != nil {
		return nil, err
	}
	for _, member := range popped {
		s.del(member)
	}
	s.store()
	if len(args) == 3 {
		return setReply(membersReply(popped)), nil
	}
	if len(popped) == 0 {
		return nilReply, nil
	}
	return bulkReply(popped[0]), nil
}

func cmdSRandMember(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if len(args) > 3 {
		return errorReply(errSyntax), nil
	}
	var count int64
	if len(args) == 3 {
		var bad reply
		if count, bad = randomCount(args); bad != nil {
			return bad, nil
		}
	}
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if len(args) == 2 {
		count = 1
	}
	picked, _, err := s.random(count, false)
	if err != nil {
		return nil, err
	}
	if len(args) == 2 {
		if len(picked) == 0 {
			return nilReply, nil
		}
		return bulkReply(picked[0]), nil
	}
	out := arrayReply{}
	for _, member := range picked {
		out = append(out, bulkReply(member))
	}
	return out, nil
}

func cmdSMove(tx *bitcask.Tx, args [][]byte) (reply, error) {
	src, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if src.len() == 0 {
		return intReply(0), nil
	}
	dst, bad, err := openSet(tx, args[2])
	if bad != nil || err != nil {
		return bad, err
	}
	found, err := src.has(args[3])
	if err != nil || !found {
		return intReply(0), err
	}
	if string(args[1]) != string(args[2]) {
		src.del(args[3])
		src.store()
		if _, err := dst.set(args[3], nil); err != nil {
			return nil, err
		}
		dst.store()
	}
	return intReply(1), nil
}

func openSets(tx *bitcask.Tx, keys [][]byte) ([]*collection, reply, error) {
	sets := make([]*collection, len(keys))
	for i, key := range keys {
		s, bad, err := openSet(tx, key)
		if bad != nil || err != nil {
			return nil, bad, err
		}
		sets[i] = s
	}
	return sets, nil, nil
}

func setInter(sets []*collection, limit int) ([][]byte, error) {
	slices.SortStableFunc(sets, func(a, b *collection) int { return a.len() - b.len() })
	if sets[0].len() == 0 {
		return nil, nil
	}
	first, err := sets[0].members()
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, m := range first {
		in := true
		for _, s := range sets[1:] {
			if in, err = s.has(m); err != nil {
				return nil, err
			} else if !in {
				break
			}
		}
		if in {
			out = append(out, m)
			if limit > 0 && len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func setUnion(sets []*collection) ([][]byte, error) {
	seen := make(map[string]bool)
	var out [][]byte
	for _, s := range sets {
		err := s.each(false, func(m, _ []byte) {
			if !seen[string(m)] {
				seen[string(m)] = true
				out = append(out, m)
			}
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func setDiff(sets []*collection) ([][]byte, error) {
	first, err := sets[0].members()
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, m := range first {
		in := false
		for _, s := range sets[1:] {
			if in, err = s.has(m); err != nil {
				return nil, err
			} else if in {
				break
			}
		}
		if !in {
			out = append(out, m)
		}
	}
	return out, nil
}

func setAlgebra(tx *bitcask.Tx, keys [][]byte, op func([]*collection) ([][]byte, error)) ([][]byte, reply, error) {
	sets, bad, err := openSets(tx, keys)
	if bad != nil || err != nil {
		return nil, bad, err
	}
	members, err := op(sets)
	return members, nil, err
}

func storeSet(tx *bitcask.Tx, dest []byte, members [][]byte) (reply, error) {
	tx.Delete(string(dest))
	s, _, err := openSet(tx, dest)
	if err != nil {
		return nil, err
	}
	for _, m := range members {
		if _, err := s.set(m, nil); err != nil {
			return nil, err
		}
	}
	s.store()
	return intReply(len(members)), nil
}

func setCommand(op func([]*collection) ([][]byte, error), store bool) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		keys := args[1:]
		if store {
			keys = args[2:]
		}
		members, bad, err := setAlgebra(tx, keys, op)
		if bad != nil || err != nil {
			return bad, err
		}
		if store {
			return storeSet(tx, args[1], members)
		}
		return setReply(membersReply(members)), nil
	}
}

var (
	cmdSInter      = setCommand(func(s []*collection) ([][]byte, error) { return setInter(s, 0) }, false)
	cmdSInterStore = setCommand(func(s []*collection) ([][]byte, error) { return setInter(s, 0) }, true)
	cmdSUnion      = setCommand(setUnion, false)
	cmdSUnionStore = setCommand(setUnion, true)
	cmdSDiff       = setCommand(setDiff, false)
	cmdSDiffStore  = setCommand(setDiff, true)
)

func cmdSInterCard(tx *bitcask.Tx, args [][]byte) (reply, error) {
	numkeys, ok := parseInt(args[1])
	if !ok || numkeys < 1 {
		return errorReply("ERR numkeys should be greater than 0"), nil
	}
	if numkeys > int64(len(args)-2) {
		return errorReply("ERR Number of keys can't be greater than number of args"), nil
	}
	keys, rest := args[2:2+numkeys], args[2+numkeys:]
	var limit int64
	for i := 0; i < len(rest); i += 2 {
		if upper(rest[i]) != "LIMIT" || i+1 >= len(rest) {
			return errorReply(errSyntax), nil
		}
		if limit, ok = parseInt(rest[i+1]); !ok || limit < 0 {
			return errorReply("ERR LIMIT can't be negative"), nil
		}
	}
	members, bad, err := setAlgebra(tx, keys, func(s []*collection) ([][]byte, error) { return setInter(s, int(limit)) })
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(len(members)), nil
}

func cmdSScan(tx *bitcask.Tx, args [][]byte) (reply, error) {
	match, bad := parseScan(args, nil)
	if bad != nil {
		return bad, nil
	}
	s, bad, err := openSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	items := arrayReply{}
	err = s.each(false, func(m, _ []byte) {
		if match(m) {
			items = append(items, bulkReply(m))
		}
	})
	return arrayReply{bulkReply("0"), items}, err
}
