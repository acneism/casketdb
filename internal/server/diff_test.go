package server

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

type diffGen struct {
	rng *rand.Rand
	ids map[string]int
}

func (g *diffGen) pick(xs ...string) string {
	return xs[g.rng.IntN(len(xs))]
}

func (g *diffGen) n(lo, hi int) string {
	return strconv.Itoa(lo + g.rng.IntN(hi-lo+1))
}

func (g *diffGen) key(prefix string) string {
	if g.rng.IntN(25) == 0 {
		prefix = g.pick("s", "h", "l", "S", "z", "x", "g")
	}
	return prefix + g.n(0, 2)
}

func (g *diffGen) lexKey() string {
	return "y" + g.n(0, 2)
}

func (g *diffGen) member() string {
	if g.rng.IntN(4) == 0 {
		return g.n(-2, 9)
	}
	return "m" + g.n(0, 15)
}

func (g *diffGen) value() string {
	return g.pick("0", "1", "7", "-3", "12", "1.5", "abc", "", "hello world", "9223372036854775807", "007", " 1")
}

func (g *diffGen) score() string {
	return g.pick("0", "1", "2", "-1", "1.5", "2.5", "10", "-0.25", "3", "1e3")
}

func (g *diffGen) bound() string {
	return g.pick("-inf", "+inf", "0", "1", "(1", "2", "(2.5", "10", "-1")
}

func (g *diffGen) lex() string {
	return g.pick("-", "+", "[a", "(a", "[c", "(e", "[m3", "(m1")
}

func (g *diffGen) index() string {
	return g.n(-6, 6)
}

func (g *diffGen) maybe(opts ...string) []string {
	if g.rng.IntN(2) == 0 {
		return nil
	}
	return []string{g.pick(opts...)}
}

func (g *diffGen) sometimes(args ...string) []string {
	if g.rng.IntN(2) == 0 {
		return nil
	}
	return args
}

func (g *diffGen) flat(lo, hi int, f func() []string) []string {
	var out []string
	for range lo + g.rng.IntN(hi-lo+1) {
		out = append(out, f()...)
	}
	return out
}

func (g *diffGen) list(f func() string, lo, hi int) []string {
	out := make([]string, lo+g.rng.IntN(hi-lo+1))
	for i := range out {
		out[i] = f()
	}
	return out
}

func (g *diffGen) bulk(f func(i int) []string) []string {
	var out []string
	for i := range 130 + g.rng.IntN(20) {
		out = append(out, f(i)...)
	}
	return out
}

func (g *diffGen) streamID(key string) string {
	if g.rng.IntN(10) == 0 {
		return g.n(0, 3) + "-" + g.n(0, 1)
	}
	g.ids[key] += 1 + g.rng.IntN(3)
	return strconv.Itoa(g.ids[key]) + "-" + g.n(0, 2)
}

func cmd(name string, args ...[]string) []string {
	out := []string{name}
	for _, a := range args {
		out = append(out, a...)
	}
	return out
}

func one(s ...string) []string {
	return s
}

var diffCommands = []func(g *diffGen) []string{
	func(g *diffGen) []string {
		return cmd("SET", one(g.key("s"), g.value()), g.maybe("NX", "XX", "GET"))
	},
	func(g *diffGen) []string { return cmd("GET", one(g.key("s"))) },
	func(g *diffGen) []string { return cmd("APPEND", one(g.key("s"), g.value())) },
	func(g *diffGen) []string { return cmd(g.pick("INCR", "DECR"), one(g.key("s"))) },
	func(g *diffGen) []string { return cmd(g.pick("INCRBY", "DECRBY"), one(g.key("s"), g.n(-5, 5))) },
	func(g *diffGen) []string {
		return cmd("INCRBYFLOAT", one(g.key("s"), g.pick("1.5", "-0.25", "3", "0.5", "abc")))
	},
	func(g *diffGen) []string { return cmd("STRLEN", one(g.key("s"))) },
	func(g *diffGen) []string { return cmd("GETRANGE", one(g.key("s"), g.index(), g.index())) },
	func(g *diffGen) []string { return cmd("SETRANGE", one(g.key("s"), g.n(0, 12), g.value())) },
	func(g *diffGen) []string { return cmd(g.pick("GETSET", "SETNX"), one(g.key("s"), g.value())) },
	func(g *diffGen) []string { return cmd("GETDEL", one(g.key("s"))) },
	func(g *diffGen) []string { return cmd("MSET", one(g.key("s"), g.value(), g.key("s"), g.value())) },
	func(g *diffGen) []string { return cmd("MSETNX", one(g.key("s"), g.value(), g.key("s"), g.value())) },
	func(g *diffGen) []string { return cmd("MGET", g.list(func() string { return g.key("s") }, 1, 4)) },
	func(g *diffGen) []string { return cmd("SETBIT", one(g.key("s"), g.n(0, 70), g.n(0, 1))) },
	func(g *diffGen) []string { return cmd("GETBIT", one(g.key("s"), g.n(0, 80))) },
	func(g *diffGen) []string {
		args := one(g.key("s"))
		if g.rng.IntN(2) == 0 {
			args = append(args, g.index(), g.index())
			args = append(args, g.maybe("BYTE", "BIT")...)
		}
		return cmd("BITCOUNT", args)
	},
	func(g *diffGen) []string {
		args := one(g.key("s"), g.n(0, 1))
		switch g.rng.IntN(3) {
		case 1:
			args = append(args, g.index())
		case 2:
			args = append(args, g.index(), g.index())
			args = append(args, g.maybe("BYTE", "BIT")...)
		}
		return cmd("BITPOS", args)
	},
	func(g *diffGen) []string {
		op := g.pick("AND", "OR", "XOR", "NOT")
		srcs := g.list(func() string { return g.key("s") }, 1, 3)
		if op == "NOT" {
			srcs = srcs[:1]
		}
		return cmd("BITOP", one(op, g.key("s")), srcs)
	},
	func(g *diffGen) []string {
		return cmd("BITFIELD", one(g.key("s"), "INCRBY", g.pick("u4", "i8", "u8"), g.n(0, 20), g.n(-20, 20)), one("GET", g.pick("u4", "i8"), g.n(0, 20)))
	},
	func(g *diffGen) []string { return cmd("OBJECT", one("ENCODING", g.key("s"))) },
	func(g *diffGen) []string {
		return cmd(g.pick("DEL", "EXISTS"), g.list(func() string { return g.key(g.pick("s", "h", "l", "S", "z")) }, 1, 3))
	},
	func(g *diffGen) []string {
		return cmd("TYPE", one(g.key(g.pick("s", "h", "l", "S", "z", "y", "x", "p", "g"))))
	},
	func(g *diffGen) []string {
		p := g.pick("s", "h", "l", "S", "z")
		return cmd(g.pick("RENAME", "RENAMENX"), one(g.key(p), g.key(p)))
	},
	func(g *diffGen) []string {
		p := g.pick("s", "h", "l", "S", "z")
		return cmd("COPY", one(g.key(p), g.key(p)), g.maybe("REPLACE"))
	},
	func(g *diffGen) []string { return cmd("DBSIZE") },
	func(g *diffGen) []string { return cmd("KEYS", one(g.pick("*", "s*", "?0", "[hl]*"))) },

	func(g *diffGen) []string {
		return cmd("HSET", one(g.key("h")), g.flat(1, 3, func() []string { return one(g.member(), g.value()) }))
	},
	func(g *diffGen) []string {
		return cmd("HSET", one(g.key("h")), g.bulk(func(i int) []string { return one("f"+strconv.Itoa(i), strconv.Itoa(i)) }))
	},
	func(g *diffGen) []string {
		return cmd(g.pick("HGET", "HEXISTS", "HSTRLEN"), one(g.key("h"), g.member()))
	},
	func(g *diffGen) []string { return cmd("HMGET", one(g.key("h")), g.list(g.member, 1, 3)) },
	func(g *diffGen) []string { return cmd("HDEL", one(g.key("h")), g.list(g.member, 1, 3)) },
	func(g *diffGen) []string { return cmd(g.pick("HLEN", "HGETALL", "HKEYS", "HVALS"), one(g.key("h"))) },
	func(g *diffGen) []string { return cmd("HINCRBY", one(g.key("h"), g.member(), g.n(-5, 5))) },
	func(g *diffGen) []string {
		return cmd("HINCRBYFLOAT", one(g.key("h"), g.member(), g.pick("1.5", "-0.25", "2", "x")))
	},
	func(g *diffGen) []string { return cmd("HSETNX", one(g.key("h"), g.member(), g.value())) },

	func(g *diffGen) []string {
		return cmd(g.pick("LPUSH", "RPUSH", "LPUSHX", "RPUSHX"), one(g.key("l")), g.list(g.member, 1, 4))
	},
	func(g *diffGen) []string {
		return cmd("RPUSH", one(g.key("l")), g.bulk(func(i int) []string { return one("e" + strconv.Itoa(i)) }))
	},
	func(g *diffGen) []string {
		args := one(g.key("l"))
		if g.rng.IntN(2) == 0 {
			args = append(args, g.n(0, 4))
		}
		return cmd(g.pick("LPOP", "RPOP"), args)
	},
	func(g *diffGen) []string { return cmd("LLEN", one(g.key("l"))) },
	func(g *diffGen) []string { return cmd("LINDEX", one(g.key("l"), g.index())) },
	func(g *diffGen) []string { return cmd("LRANGE", one(g.key("l"), g.index(), g.index())) },
	func(g *diffGen) []string { return cmd("LSET", one(g.key("l"), g.index(), g.member())) },
	func(g *diffGen) []string { return cmd("LREM", one(g.key("l"), g.n(-2, 2), g.member())) },
	func(g *diffGen) []string { return cmd("LTRIM", one(g.key("l"), g.index(), g.index())) },
	func(g *diffGen) []string {
		return cmd("LINSERT", one(g.key("l"), g.pick("BEFORE", "AFTER"), g.member(), g.member()))
	},
	func(g *diffGen) []string {
		args := one(g.key("l"), g.member())
		if g.rng.IntN(2) == 0 {
			args = append(args, "RANK", g.pick("1", "-1", "2", "-2"))
		}
		if g.rng.IntN(2) == 0 {
			args = append(args, "COUNT", g.n(0, 3))
		}
		return cmd("LPOS", args)
	},
	func(g *diffGen) []string {
		return cmd("LMOVE", one(g.key("l"), g.key("l"), g.pick("LEFT", "RIGHT"), g.pick("LEFT", "RIGHT")))
	},
	func(g *diffGen) []string {
		return cmd("LMPOP", one("2", g.key("l"), g.key("l"), g.pick("LEFT", "RIGHT")), g.sometimes("COUNT", "2"))
	},

	func(g *diffGen) []string { return cmd("SADD", one(g.key("S")), g.list(g.member, 1, 4)) },
	func(g *diffGen) []string {
		return cmd("SADD", one(g.key("S")), g.bulk(func(i int) []string { return one(strconv.Itoa(i)) }))
	},
	func(g *diffGen) []string { return cmd("SREM", one(g.key("S")), g.list(g.member, 1, 3)) },
	func(g *diffGen) []string { return cmd("SISMEMBER", one(g.key("S"), g.member())) },
	func(g *diffGen) []string { return cmd("SMISMEMBER", one(g.key("S")), g.list(g.member, 1, 3)) },
	func(g *diffGen) []string { return cmd(g.pick("SMEMBERS", "SCARD"), one(g.key("S"))) },
	func(g *diffGen) []string {
		return cmd(g.pick("SINTER", "SUNION", "SDIFF"), g.list(func() string { return g.key("S") }, 1, 3))
	},
	func(g *diffGen) []string {
		return cmd(g.pick("SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"), one(g.key("S")), g.list(func() string { return g.key("S") }, 1, 3))
	},
	func(g *diffGen) []string {
		return cmd("SINTERCARD", one("2", g.key("S"), g.key("S")), g.sometimes("LIMIT", "1"))
	},
	func(g *diffGen) []string { return cmd("SMOVE", one(g.key("S"), g.key("S"), g.member())) },

	func(g *diffGen) []string {
		flags := g.maybe("NX", "XX")
		flags = append(flags, g.maybe("GT", "LT")...)
		flags = append(flags, g.maybe("CH")...)
		pairs := g.flat(1, 3, func() []string { return one(g.score(), g.member()) })
		if g.rng.IntN(5) == 0 {
			flags, pairs = append(flags, "INCR"), pairs[:2]
		}
		return cmd("ZADD", one(g.key("z")), flags, pairs)
	},
	func(g *diffGen) []string {
		return cmd("ZADD", one(g.key("z")), g.bulk(func(i int) []string { return one(strconv.Itoa(i%17), "b"+strconv.Itoa(i)) }))
	},
	func(g *diffGen) []string {
		return cmd("ZADD", one(g.lexKey()), g.flat(1, 3, func() []string { return one("0", g.pick("a", "b", "c", "d", "e", "m1", "m3")) }))
	},
	func(g *diffGen) []string { return cmd("ZREM", one(g.key("z")), g.list(g.member, 1, 3)) },
	func(g *diffGen) []string { return cmd("ZSCORE", one(g.key("z"), g.member())) },
	func(g *diffGen) []string { return cmd("ZMSCORE", one(g.key("z")), g.list(g.member, 1, 3)) },
	func(g *diffGen) []string { return cmd("ZINCRBY", one(g.key("z"), g.score(), g.member())) },
	func(g *diffGen) []string { return cmd("ZCARD", one(g.key("z"))) },
	func(g *diffGen) []string { return cmd("ZCOUNT", one(g.key("z"), g.bound(), g.bound())) },
	func(g *diffGen) []string { return cmd("ZLEXCOUNT", one(g.lexKey(), g.lex(), g.lex())) },
	func(g *diffGen) []string {
		return cmd(g.pick("ZRANK", "ZREVRANK"), one(g.key("z"), g.member()), g.maybe("WITHSCORE"))
	},
	func(g *diffGen) []string {
		args := one(g.key("z"))
		switch g.rng.IntN(3) {
		case 0:
			args = append(args, g.index(), g.index())
		case 1:
			args = append(args, g.bound(), g.bound(), "BYSCORE")
		default:
			args = append(args[:0], g.lexKey(), g.lex(), g.lex(), "BYLEX")
		}
		args = append(args, g.maybe("REV")...)
		if args[len(args)-1] != "REV" && len(args) > 3 && g.rng.IntN(3) == 0 {
			args = append(args, "LIMIT", g.n(0, 2), g.n(-1, 3))
		}
		if !slices.Contains(args, "BYLEX") {
			args = append(args, g.maybe("WITHSCORES")...)
		}
		return cmd("ZRANGE", args)
	},
	func(g *diffGen) []string {
		return cmd(g.pick("ZRANGEBYSCORE", "ZREVRANGEBYSCORE"), one(g.key("z"), g.bound(), g.bound()), g.maybe("WITHSCORES"))
	},
	func(g *diffGen) []string {
		return cmd(g.pick("ZRANGEBYLEX", "ZREVRANGEBYLEX"), one(g.lexKey(), g.lex(), g.lex()))
	},
	func(g *diffGen) []string {
		return cmd("ZREVRANGE", one(g.key("z"), g.index(), g.index()), g.maybe("WITHSCORES"))
	},
	func(g *diffGen) []string {
		args := one(g.key("z"))
		if g.rng.IntN(2) == 0 {
			args = append(args, g.n(0, 3))
		}
		return cmd(g.pick("ZPOPMIN", "ZPOPMAX"), args)
	},
	func(g *diffGen) []string { return cmd("ZREMRANGEBYRANK", one(g.key("z"), g.index(), g.index())) },
	func(g *diffGen) []string { return cmd("ZREMRANGEBYSCORE", one(g.key("z"), g.bound(), g.bound())) },
	func(g *diffGen) []string { return cmd("ZREMRANGEBYLEX", one(g.lexKey(), g.lex(), g.lex())) },
	func(g *diffGen) []string {
		args := one(g.key("z"), "2", g.key("z"), g.key("z"))
		if g.rng.IntN(2) == 0 {
			args = append(args, "WEIGHTS", g.pick("1", "2", "-1", "0.5"), g.pick("1", "3"))
		}
		if g.rng.IntN(2) == 0 {
			args = append(args, "AGGREGATE", g.pick("SUM", "MIN", "MAX"))
		}
		return cmd(g.pick("ZUNIONSTORE", "ZINTERSTORE"), args)
	},
	func(g *diffGen) []string {
		return cmd(g.pick("ZUNION", "ZINTER", "ZDIFF"), one("2", g.key("z"), g.key("z")), g.maybe("WITHSCORES"))
	},
	func(g *diffGen) []string { return cmd("ZDIFFSTORE", one(g.key("z"), "2", g.key("z"), g.key("z"))) },
	func(g *diffGen) []string {
		return cmd("ZRANGESTORE", one(g.key("z"), g.key("z"), g.index(), g.index()))
	},

	func(g *diffGen) []string {
		return cmd("PFADD", one(g.key("p")), g.list(func() string { return "e" + g.n(0, 300) }, 0, 5))
	},
	func(g *diffGen) []string {
		return cmd("PFADD", one(g.key("p")), g.bulk(func(i int) []string { return one("big" + strconv.Itoa(g.rng.IntN(100000))) }))
	},
	func(g *diffGen) []string { return cmd("PFCOUNT", g.list(func() string { return g.key("p") }, 1, 2)) },
	func(g *diffGen) []string {
		return cmd("PFMERGE", one(g.key("p")), g.list(func() string { return g.key("p") }, 1, 2))
	},
	func(g *diffGen) []string { return cmd("GET", one(g.key("p"))) },

	func(g *diffGen) []string {
		place := g.pick("13.361389 38.115556 Palermo", "15.087269 37.502669 Catania", "12.496366 41.902782 Rome", "9.189982 45.464204 Milan", "2.352222 48.856613 Paris")
		return cmd("GEOADD", one(g.key("g")), strings.Fields(place))
	},
	func(g *diffGen) []string {
		cities := func() string { return g.pick("Palermo", "Catania", "Rome", "Milan", "Paris", "Nowhere") }
		return cmd("GEODIST", one(g.key("g"), cities(), cities()), g.maybe("m", "km", "mi"))
	},
	func(g *diffGen) []string {
		return cmd("GEOHASH", one(g.key("g")), g.list(func() string { return g.pick("Palermo", "Rome", "Paris", "Nowhere") }, 1, 3))
	},
	func(g *diffGen) []string {
		return cmd("GEOSEARCH", one(g.key("g"), "FROMMEMBER", g.pick("Palermo", "Rome", "Nowhere"), "BYRADIUS", g.pick("200", "600", "2000"), "km", g.pick("ASC", "DESC")), g.sometimes("COUNT", "2"))
	},

	func(g *diffGen) []string {
		k := g.key("x")
		return cmd("XADD", one(k, g.streamID(k), "f", g.value()))
	},
	func(g *diffGen) []string { return cmd("XLEN", one(g.key("x"))) },
	func(g *diffGen) []string {
		return cmd(g.pick("XRANGE", "XREVRANGE"), one(g.key("x"), "-", "+"), g.sometimes("COUNT", "2"))
	},
	func(g *diffGen) []string { return cmd("XDEL", one(g.key("x"), g.n(1, 20)+"-"+g.n(0, 2))) },
	func(g *diffGen) []string { return cmd("XTRIM", one(g.key("x"), "MAXLEN", g.n(0, 5))) },
	func(g *diffGen) []string {
		return cmd("XGROUP", one("CREATE", g.key("x"), g.pick("g1", "g2"), g.pick("0", "$", "3-0")), g.maybe("MKSTREAM"))
	},
	func(g *diffGen) []string {
		return cmd("XGROUP", one(g.pick("CREATECONSUMER", "DELCONSUMER"), g.key("x"), g.pick("g1", "g2"), g.pick("c1", "c2")))
	},
	func(g *diffGen) []string { return cmd("XGROUP", one("DESTROY", g.key("x"), g.pick("g1", "g2"))) },
	func(g *diffGen) []string {
		return cmd("XREADGROUP", one("GROUP", g.pick("g1", "g2"), g.pick("c1", "c2")), g.sometimes("COUNT", g.n(1, 3)), g.maybe("NOACK"), one("STREAMS", g.key("x"), g.pick(">", "0")))
	},
	func(g *diffGen) []string {
		return cmd("XACK", one(g.key("x"), g.pick("g1", "g2")), g.list(func() string { return g.n(1, 20) + "-" + g.n(0, 2) }, 1, 3))
	},
	func(g *diffGen) []string { return cmd("XPENDING", one(g.key("x"), g.pick("g1", "g2"))) },
	func(g *diffGen) []string {
		return cmd("XCLAIM", one(g.key("x"), g.pick("g1", "g2"), g.pick("c1", "c2"), "0"), g.list(func() string { return g.n(1, 20) + "-" + g.n(0, 2) }, 1, 3), one("JUSTID"))
	},
	func(g *diffGen) []string {
		return cmd("LCS", one(g.key("s"), g.key("s")), g.maybe("LEN", "IDX"))
	},
	func(g *diffGen) []string {
		return cmd("LCS", one(g.key("s"), g.key("s"), "IDX", "MINMATCHLEN", g.n(0, 2)), g.maybe("WITHMATCHLEN"))
	},
	func(g *diffGen) []string {
		name := g.pick("GET", "SET", "HSET", "LPUSH", "SADD", "ZADD", "XADD", "PFADD", "LRANGE", "ZRANGE", "HGET", "GETRANGE", "EXISTS")
		return cmd(name, g.list(func() string { return g.key("s") }, 0, 1))
	},
}

var diffUnordered = map[string]bool{"SMEMBERS": true, "SINTER": true, "SUNION": true, "SDIFF": true, "KEYS": true, "HKEYS": true, "HVALS": true}

func diffNormalize(args []string, reply any) any {
	if reply == "-0" {
		return "0"
	}
	arr, ok := reply.([]any)
	if !ok {
		return reply
	}
	if slices.Contains(arr, any("-0")) {
		arr = slices.Clone(arr)
		for i := range arr {
			if arr[i] == "-0" {
				arr[i] = "0"
			}
		}
		reply = arr
	}
	byString := func(a, b any) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) }
	switch name := strings.ToUpper(args[0]); {
	case diffUnordered[name]:
		out := slices.Clone(arr)
		slices.SortFunc(out, byString)
		return out
	case name == "HGETALL":
		var pairs [][2]any
		for i := 0; i+1 < len(arr); i += 2 {
			pairs = append(pairs, [2]any{arr[i], arr[i+1]})
		}
		slices.SortFunc(pairs, func(a, b [2]any) int { return byString(a[0], b[0]) })
		return pairs
	}
	return reply
}

func diffTolerated(args []string, got, want any) (tolerated, reset bool) {
	g, gok := got.(string)
	w, wok := want.(string)
	if !gok || !wok {
		return false, false
	}
	switch name := strings.ToUpper(args[0]); {
	case name == "OBJECT":
		both := func(encodings ...string) bool { return slices.Contains(encodings, g) && slices.Contains(encodings, w) }
		history := both("int", "embstr", "raw") && (w == "raw" || w == "embstr" && g == "int")
		return history || both("listpack", "hashtable", "quicklist", "intset", "skiplist"), false
	case name == "INCRBYFLOAT", name == "HINCRBYFLOAT":
		a, err := strconv.ParseFloat(g, 64)
		b, err2 := strconv.ParseFloat(w, 64)
		return err == nil && err2 == nil && math.Abs(a-b) <= 1e-15*math.Abs(b), true
	case name == "GET" && strings.HasPrefix(g, "HYLL") && len(g) == len(w) && len(g) >= 16:
		return g[:8] == w[:8] && g[16:] == w[16:], false
	}
	return false, false
}

func TestDifferential(t *testing.T) {
	addr := os.Getenv("CASKETDB_REDIS_ADDR")
	if addr == "" {
		t.Skip("set CASKETDB_REDIS_ADDR to a Redis 7.2 server that the test may flush")
	}
	steps, seed := 20000, uint64(1)
	if n, err := strconv.Atoi(os.Getenv("CASKETDB_DIFF_STEPS")); err == nil {
		steps = n
	}
	if n, err := strconv.ParseUint(os.Getenv("CASKETDB_DIFF_SEED"), 10, 64); err == nil {
		seed = n
	}
	srv, db, ours := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	a, b := dial(t, ours), dial(t, addr)
	g := &diffGen{rng: rand.New(rand.NewPCG(seed, 0)), ids: map[string]int{}}
	var history []string
	reset := func() {
		a.do("FLUSHALL")
		b.do("FLUSHALL")
		clear(g.ids)
		history = history[:0]
	}
	reset()
	seen := map[string]int{}
	for i := range steps {
		args := diffCommands[g.rng.IntN(len(diffCommands))](g)
		for _, c := range []*testConn{a, b} {
			_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
		}
		got, want := diffNormalize(args, a.do(args...)), diffNormalize(args, b.do(args...))
		history = append(history, fmt.Sprintf("%d %q", i, args))
		if reflect.DeepEqual(got, want) {
			continue
		}
		if tolerated, again := diffTolerated(args, got, want); tolerated {
			if again {
				reset()
			}
			continue
		}
		name := strings.ToUpper(args[0])
		if seen[name]++; seen[name] <= 3 {
			var related []string
			for _, h := range history {
				if slices.ContainsFunc(args[1:], func(k string) bool { return len(k) == 2 && strings.Contains(h, `"`+k+`"`) }) {
					related = append(related, h)
				}
			}
			related = related[max(0, len(related)-25):]
			t.Errorf("step %d %q:\n  casketdb %#v\n  redis    %#v\n  earlier commands on these keys:\n    %s", i, args, got, want, strings.Join(related, "\n    "))
		}
		reset()
	}
	if len(seen) > 0 {
		t.Logf("differences by command: %v", seen)
	}
}
