package server

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func clampRange(start, end, n int64, early bool) (int64, int64, bool) {
	if early && start < 0 && end < 0 && start > end {
		return 0, -1, false
	}
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start, end = max(start, 0), min(max(end, 0), n-1)
	return start, end, start <= end
}

func TestStringModel(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	var s []byte
	exists := false
	rng := rand.New(rand.NewPCG(3, 4))
	value := func() string {
		b := make([]byte, 1+rng.IntN(12))
		for i := range b {
			b[i] = [3]byte{0, 0xff, byte(rng.IntN(256))}[rng.IntN(3)]
		}
		return string(b)
	}
	pick := func(n int64) int64 { return rng.Int64N(2*n+41) - n - 20 }
	bitAt := func(i int64) int64 {
		if i/8 < int64(len(s)) && s[i/8]&(0x80>>(i%8)) != 0 {
			return 1
		}
		return 0
	}
	grow := func(n int) {
		if n > len(s) {
			s = append(s, make([]byte, n-len(s))...)
		}
	}
	units := func() (int64, []string) {
		switch rng.IntN(3) {
		case 1:
			return 8, []string{"BYTE"}
		case 2:
			return 1, []string{"BIT"}
		}
		return 8, nil
	}
	for range 8000 {
		n := int64(len(s))
		switch rng.IntN(11) {
		case 0:
			v := value()
			s, exists = []byte(v), true
			c.expect(status("OK"), "SET", "s", v)
		case 1:
			v := value()
			s, exists = append(s, v...), true
			c.expect(int64(len(s)), "APPEND", "s", v)
		case 2:
			off, v := rng.IntN(300), value()
			grow(off + len(v))
			copy(s[off:], v)
			exists = true
			c.expect(int64(len(s)), "SETRANGE", "s", fmt.Sprint(off), v)
		case 3:
			bit, on := rng.Int64N(3000), rng.IntN(2)
			old := bitAt(bit)
			grow(int(bit/8) + 1)
			if mask := byte(0x80 >> (bit % 8)); on == 1 {
				s[bit/8] |= mask
			} else {
				s[bit/8] &^= mask
			}
			exists = true
			c.expect(old, "SETBIT", "s", fmt.Sprint(bit), fmt.Sprint(on))
		case 4:
			bit := rng.Int64N(4000)
			c.expect(bitAt(bit), "GETBIT", "s", fmt.Sprint(bit))
		case 5:
			start, end := pick(n), pick(n)
			want := ""
			if lo, hi, ok := clampRange(start, end, n, true); ok {
				want = string(s[lo : hi+1])
			}
			c.expect(want, "GETRANGE", "s", fmt.Sprint(start), fmt.Sprint(end))
		case 6:
			args, lo, hi := []string{"BITCOUNT", "s"}, int64(0), n*8-1
			if rng.IntN(3) > 0 {
				unit, mode := units()
				start, end := pick(n*8/unit), pick(n*8/unit)
				args = append(append(args, fmt.Sprint(start), fmt.Sprint(end)), mode...)
				a, b, ok := clampRange(start, end, n*8/unit, true)
				lo, hi = a*unit, b*unit+unit-1
				if !ok {
					lo, hi = 0, -1
				}
			}
			count := int64(0)
			for i := lo; i <= hi; i++ {
				count += bitAt(i)
			}
			c.expect(count, args...)
		case 7:
			bit := rng.Int64N(2)
			args, lo, hi, endGiven := []string{"BITPOS", "s", fmt.Sprint(bit)}, int64(0), n*8-1, false
			switch rng.IntN(3) {
			case 1:
				start := pick(n)
				args = append(args, fmt.Sprint(start))
				a, b, ok := clampRange(start, n-1, n, false)
				lo, hi = a*8, b*8+7
				if !ok {
					lo, hi = 0, -1
				}
			case 2:
				unit, mode := units()
				start, end := pick(n*8/unit), pick(n*8/unit)
				args = append(append(args, fmt.Sprint(start), fmt.Sprint(end)), mode...)
				a, b, ok := clampRange(start, end, n*8/unit, false)
				lo, hi, endGiven = a*unit, b*unit+unit-1, true
				if !ok {
					lo, hi = 0, -1
				}
			}
			want := -bit
			if exists {
				want = -1
				for i := lo; i <= hi && want == -1; i++ {
					if bitAt(i) == bit {
						want = i
					}
				}
				if want == -1 && bit == 0 && !endGiven && lo <= hi {
					want = hi + 1
				}
			}
			c.expect(want, args...)
		case 8:
			c.expect(n, "STRLEN", "s")
		case 9:
			if exists {
				c.expect(string(s), "GET", "s")
			} else {
				c.expect(nil, "GET", "s")
			}
		default:
			if rng.IntN(4) == 0 {
				deleted := int64(0)
				if exists {
					deleted = 1
				}
				c.expect(deleted, "DEL", "s")
				s, exists = nil, false
			}
		}
	}
}
