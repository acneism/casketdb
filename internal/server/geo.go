package server

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/acneism/casketdb/internal/bitcask"
)

const (
	geoStepMax   = 26
	geoLatMin    = -85.05112878
	geoLatMax    = 85.05112878
	geoLongMin   = -180
	geoLongMax   = 180
	earthRadius  = 6372797.560856
	mercatorMax  = 20037726.37
	errGeoMember = "ERR could not decode requested zset member"
)

const (
	geoByCoords = 1 << iota
	geoByMember
	geoNoStore
	geoSearch
	geoSearchStore
)

var geoCommands = map[string]command{
	"geoadd":               {arity: -5, kind: kindWrite, keys: oneKey, acl: catGeo, tx: cmdGeoAdd},
	"geodist":              {arity: -4, kind: kindRead, keys: oneKey, acl: catGeo, tx: cmdGeoDist},
	"geohash":              {arity: -2, kind: kindRead, keys: oneKey, acl: catGeo, tx: cmdGeoHash},
	"geopos":               {arity: -2, kind: kindRead, keys: oneKey, acl: catGeo, tx: cmdGeoPos},
	"georadius":            {arity: -6, kind: kindWrite, keys: keySpec{first: 1, last: 1, step: 1, storeFrom: 6}, acl: catGeo, tx: geoSearchCommand(geoByCoords)},
	"georadius_ro":         {arity: -6, kind: kindRead, keys: oneKey, acl: catGeo, tx: geoSearchCommand(geoByCoords | geoNoStore)},
	"georadiusbymember":    {arity: -5, kind: kindWrite, keys: keySpec{first: 1, last: 1, step: 1, storeFrom: 5}, acl: catGeo, tx: geoSearchCommand(geoByMember)},
	"georadiusbymember_ro": {arity: -5, kind: kindRead, keys: oneKey, acl: catGeo, tx: geoSearchCommand(geoByMember | geoNoStore)},
	"geosearch":            {arity: -7, kind: kindRead, keys: oneKey, acl: catGeo, tx: geoSearchCommand(geoSearch)},
	"geosearchstore":       {arity: -8, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catGeo, tx: geoSearchCommand(geoSearch | geoSearchStore)},
}

var degToRad = func() float64 {
	pi := math.Pi
	return pi / 180
}()

type geoRange struct{ min, max float64 }

var (
	lonRange = geoRange{geoLongMin, geoLongMax}
	latRange = geoRange{geoLatMin, geoLatMax}
)

type geoHash struct {
	bits uint64
	step uint
}

type geoArea struct {
	lonMin, lonMax, latMin, latMax float64
}

func interleave64(lat, lon uint32) uint64 {
	masks := [...]uint64{0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF}
	x, y := uint64(lat), uint64(lon)
	for i := 4; i >= 0; i-- {
		x = (x | x<<(1<<i)) & masks[i]
		y = (y | y<<(1<<i)) & masks[i]
	}
	return x | y<<1
}

func deinterleave64(v uint64) uint64 {
	masks := [...]uint64{0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF, 0x00000000FFFFFFFF}
	x, y := v, v>>1
	for i, mask := range masks {
		shift := uint(0)
		if i > 0 {
			shift = 1 << (i - 1)
		}
		x = (x | x>>shift) & mask
		y = (y | y>>shift) & mask
	}
	return x | y<<32
}

func geoEncode(lons, lats geoRange, lon, lat float64, step uint) uint64 {
	latOffset := (lat - lats.min) / (lats.max - lats.min) * float64(uint64(1)<<step)
	lonOffset := (lon - lons.min) / (lons.max - lons.min) * float64(uint64(1)<<step)
	return interleave64(uint32(latOffset), uint32(lonOffset))
}

func geoDecode(h geoHash) geoArea {
	sep := deinterleave64(h.bits)
	ilat, ilon := uint32(sep), uint32(sep>>32)
	div := float64(uint64(1) << h.step)
	latScale, lonScale := latRange.max-latRange.min, lonRange.max-lonRange.min
	return geoArea{
		lonMin: lonRange.min + float64(float64(ilon)/div*lonScale),
		lonMax: lonRange.min + float64(float64(ilon+1)/div*lonScale),
		latMin: latRange.min + float64(float64(ilat)/div*latScale),
		latMax: latRange.min + float64(float64(ilat+1)/div*latScale),
	}
}

func (h geoHash) move(dx, dy int) geoHash {
	shift := 64 - 2*h.step
	if dx != 0 {
		x, y := h.bits&0xaaaaaaaaaaaaaaaa, h.bits&0x5555555555555555
		zz := uint64(0x5555555555555555) >> shift
		if dx > 0 {
			x += zz + 1
		} else {
			x = (x | zz) - (zz + 1)
		}
		h.bits = x&(0xaaaaaaaaaaaaaaaa>>shift) | y
	}
	if dy != 0 {
		x, y := h.bits&0xaaaaaaaaaaaaaaaa, h.bits&0x5555555555555555
		zz := uint64(0xaaaaaaaaaaaaaaaa) >> shift
		if dy > 0 {
			y += zz + 1
		} else {
			y = (y | zz) - (zz + 1)
		}
		h.bits = x | y&(0x5555555555555555>>shift)
	}
	return h
}

func scoreBits(score float64) uint64 {
	if score < 0 || score >= 1<<63 {
		return 0
	}
	return uint64(score)
}

func geoPointOf(score float64) (lon, lat float64) {
	a := geoDecode(geoHash{scoreBits(score), geoStepMax})
	lon = min(max((a.lonMin+a.lonMax)/2, geoLongMin), geoLongMax)
	lat = min(max((a.latMin+a.latMax)/2, geoLatMin), geoLatMax)
	return lon, lat
}

func geoLatDistance(lat1, lat2 float64) float64 {
	return earthRadius * math.Abs(float64(lat2*degToRad)-float64(lat1*degToRad))
}

func geoDistance(lon1, lat1, lon2, lat2 float64) float64 {
	v := math.Sin((float64(lon2*degToRad) - float64(lon1*degToRad)) / 2)
	if v == 0 {
		return geoLatDistance(lat1, lat2)
	}
	lat1r, lat2r := float64(lat1*degToRad), float64(lat2*degToRad)
	u := math.Sin((lat2r - lat1r) / 2)
	a := float64(u*u) + float64(float64(float64(math.Cos(lat1r)*math.Cos(lat2r))*v)*v)
	return 2.0 * earthRadius * math.Asin(math.Sqrt(a))
}

type geoShape struct {
	box                   bool
	lon, lat              float64
	radius, width, height float64
	conversion            float64
}

func (s *geoShape) contains(lon, lat float64) (float64, bool) {
	if !s.box {
		d := geoDistance(s.lon, s.lat, lon, lat)
		return d, d <= s.radius*s.conversion
	}
	if geoLatDistance(lat, s.lat) > s.height*s.conversion/2 || geoDistance(lon, lat, s.lon, lat) > s.width*s.conversion/2 {
		return 0, false
	}
	return geoDistance(s.lon, s.lat, lon, lat), true
}

func (s *geoShape) bounds() (minLon, minLat, maxLon, maxLat float64) {
	height, width := s.conversion*s.radius, s.conversion*s.radius
	if s.box {
		height, width = s.conversion*(s.height/2), s.conversion*(s.width/2)
	}
	latDelta := height / earthRadius / degToRad
	lonDelta := width / earthRadius / math.Cos((s.lat+latDelta)*degToRad) / degToRad
	if s.lat < 0 {
		lonDelta = width / earthRadius / math.Cos((s.lat-latDelta)*degToRad) / degToRad
	}
	return s.lon - lonDelta, s.lat - latDelta, s.lon + lonDelta, s.lat + latDelta
}

func geoSteps(meters, lat float64) uint {
	if meters == 0 {
		return geoStepMax
	}
	step := 1
	for ; meters < mercatorMax; step++ {
		meters *= 2
	}
	step -= 2
	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}
	return uint(min(max(step, 1), geoStepMax))
}

func (s *geoShape) boxes() [9]geoHash {
	minLon, minLat, maxLon, maxLat := s.bounds()
	meters := s.radius
	if s.box {
		meters = math.Sqrt(float64(s.width/2*(s.width/2)) + float64(s.height/2*(s.height/2)))
	}
	steps := geoSteps(meters*s.conversion, s.lat)
	h := geoHash{geoEncode(lonRange, latRange, s.lon, s.lat, steps), steps}
	if steps > 1 && (geoDecode(h.move(0, 1)).latMax < maxLat || geoDecode(h.move(0, -1)).latMin > minLat ||
		geoDecode(h.move(1, 0)).lonMax < maxLon || geoDecode(h.move(-1, 0)).lonMin > minLon) {
		steps--
		h = geoHash{geoEncode(lonRange, latRange, s.lon, s.lat, steps), steps}
	}
	n, sth, e, w := h.move(0, 1), h.move(0, -1), h.move(1, 0), h.move(-1, 0)
	ne, nw, se, sw := h.move(1, 1), h.move(-1, 1), h.move(1, -1), h.move(-1, -1)
	if area := geoDecode(h); steps >= 2 {
		if area.latMin < minLat {
			sth, sw, se = geoHash{}, geoHash{}, geoHash{}
		}
		if area.latMax > maxLat {
			n, ne, nw = geoHash{}, geoHash{}, geoHash{}
		}
		if area.lonMin < minLon {
			w, sw, nw = geoHash{}, geoHash{}, geoHash{}
		}
		if area.lonMax > maxLon {
			e, se, ne = geoHash{}, geoHash{}, geoHash{}
		}
	}
	return [9]geoHash{h, n, sth, e, w, ne, nw, se, sw}
}

type geoPoint struct {
	member   string
	score    float64
	dist     float64
	lon, lat float64
}

func (c *collection) geoPoints(s *geoShape, limit int) ([]geoPoint, error) {
	boxes := s.boxes()
	var points []geoPoint
	last := 0
	for i, h := range boxes {
		if h == (geoHash{}) || last > 0 && h == boxes[last] {
			continue
		}
		if limit > 0 && len(points) >= limit {
			break
		}
		lo := scoreKey(float64(h.bits << (52 - 2*h.step)))
		hi := scoreKey(float64((h.bits + 1) << (52 - 2*h.step)))
		from, err := c.countBelow(func(score []byte, _ string) bool { return bytes.Compare(score, lo) < 0 })
		if err != nil {
			return nil, err
		}
		err = c.walk(from, false, func(it zitem) bool {
			if bytes.Compare(it.score, hi) >= 0 {
				return false
			}
			score := keyScore(it.score)
			lon, lat := geoPointOf(score)
			if dist, ok := s.contains(lon, lat); ok {
				points = append(points, geoPoint{it.member, score, dist, lon, lat})
			}
			return limit == 0 || len(points) < limit
		})
		if err != nil {
			return nil, err
		}
		last = i
	}
	return points, nil
}

func formatC(f float64) string {
	if math.IsInf(f, 0) {
		return strings.TrimPrefix(strconv.FormatFloat(f, 'f', -1, 64), "+")
	}
	return strconv.FormatFloat(f, 'f', 6, 64)
}

func parseLonLat(lonArg, latArg []byte) (float64, float64, reply) {
	lon, ok1 := parseFloat(lonArg)
	lat, ok2 := parseFloat(latArg)
	switch {
	case !ok1 || !ok2:
		return 0, 0, errorReply(errNotFloat)
	case lon < geoLongMin || lon > geoLongMax || lat < geoLatMin || lat > geoLatMax:
		return 0, 0, errorReply("ERR invalid longitude,latitude pair " + formatC(lon) + "," + formatC(lat))
	}
	return lon, lat, nil
}

func geoUnit(b []byte) (float64, reply) {
	switch strings.ToLower(string(b)) {
	case "m":
		return 1, nil
	case "km":
		return 1000, nil
	case "ft":
		return 0.3048, nil
	case "mi":
		return 1609.34, nil
	}
	return 0, errorReply("ERR unsupported unit provided. please use M, KM, FT, MI")
}

func (s *geoShape) parseRadius(args [][]byte) reply {
	r, ok := parseFloat(args[0])
	switch {
	case !ok:
		return errorReply("ERR need numeric radius")
	case r < 0:
		return errorReply("ERR radius cannot be negative")
	}
	var bad reply
	s.box, s.radius = false, r
	s.conversion, bad = geoUnit(args[1])
	return bad
}

func (s *geoShape) parseBox(args [][]byte) reply {
	w, ok := parseFloat(args[0])
	if !ok {
		return errorReply("ERR need numeric width")
	}
	h, ok := parseFloat(args[1])
	switch {
	case !ok:
		return errorReply("ERR need numeric height")
	case h < 0 || w < 0:
		return errorReply("ERR height or width cannot be negative")
	}
	var bad reply
	s.box, s.width, s.height = true, w, h
	s.conversion, bad = geoUnit(args[2])
	return bad
}

func (c *collection) memberPoint(member []byte) (float64, float64, bool, error) {
	score, found, err := c.get(member)
	if err != nil || !found {
		return 0, 0, false, err
	}
	lon, lat := geoPointOf(keyScore(score))
	return lon, lat, true, nil
}

func formatCoord(f float64) string {
	s := strings.TrimSuffix(strings.TrimRight(strconv.FormatFloat(f, 'f', 17, 64), "0"), ".")
	if s == "-0" {
		return "0"
	}
	return s
}

func formatDistance(d float64) string {
	n := int64(math.RoundToEven(d * 10000))
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	return fmt.Sprintf("%s%d.%04d", sign, n/10000, n%10000)
}

func cmdGeoAdd(tx *bitcask.Tx, args [][]byte) (reply, error) {
	var o zaddOptions
	i := 2
options:
	for ; i < len(args); i++ {
		switch upper(args[i]) {
		case "NX":
			o.nx = true
		case "XX":
			o.xx = true
		case "CH":
			o.ch = true
		default:
			break options
		}
	}
	triples := args[i:]
	if len(triples) == 0 || len(triples)%3 != 0 || o.nx && o.xx {
		return errorReply(errSyntax), nil
	}
	scores := make([]float64, len(triples)/3)
	members := make([][]byte, len(scores))
	for j := range scores {
		lon, lat, bad := parseLonLat(triples[3*j], triples[3*j+1])
		if bad != nil {
			return bad, nil
		}
		scores[j] = float64(geoEncode(lonRange, latRange, lon, lat, geoStepMax))
		members[j] = triples[3*j+2]
	}
	return addScores(tx, args[1], scores, members, o)
}

func cmdGeoDist(tx *bitcask.Tx, args [][]byte) (reply, error) {
	conversion := 1.0
	switch {
	case len(args) > 5:
		return errorReply(errSyntax), nil
	case len(args) == 5:
		var bad reply
		if conversion, bad = geoUnit(args[4]); bad != nil {
			return bad, nil
		}
	}
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	lon1, lat1, ok1, err := z.memberPoint(args[2])
	if err != nil {
		return nil, err
	}
	lon2, lat2, ok2, err := z.memberPoint(args[3])
	if err != nil || !ok1 || !ok2 {
		return nilReply, err
	}
	return bulkReply(formatDistance(geoDistance(lon1, lat1, lon2, lat2) / conversion)), nil
}

func cmdGeoHash(tx *bitcask.Tx, args [][]byte) (reply, error) {
	const alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	out := make(arrayReply, len(args)-2)
	for i, member := range args[2:] {
		lon, lat, ok, err := z.memberPoint(member)
		if err != nil {
			return nil, err
		}
		out[i] = nilReply
		if !ok {
			continue
		}
		bits := geoEncode(geoRange{-180, 180}, geoRange{-90, 90}, lon, lat, geoStepMax)
		buf := make([]byte, 11)
		for j := range 10 {
			buf[j] = alphabet[bits>>(52-(j+1)*5)&0x1f]
		}
		buf[10] = alphabet[0]
		out[i] = bulkReply(buf)
	}
	return out, nil
}

func cmdGeoPos(tx *bitcask.Tx, args [][]byte) (reply, error) {
	z, bad, err := openZSet(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	out := make(arrayReply, len(args)-2)
	for i, member := range args[2:] {
		lon, lat, ok, err := z.memberPoint(member)
		if err != nil {
			return nil, err
		}
		out[i] = nullArrayReply{}
		if ok {
			out[i] = arrayReply{doubleReply(formatCoord(lon)), doubleReply(formatCoord(lat))}
		}
	}
	return out, nil
}

func geoSearchCommand(flags int) txFunc {
	return func(tx *bitcask.Tx, args [][]byte) (reply, error) {
		src := 1
		if flags&geoSearchStore != 0 {
			src = 2
		}
		z, bad, err := openZSet(tx, args[src])
		if bad != nil || err != nil {
			return bad, err
		}
		exists := z.len() > 0
		var s geoShape
		var store []byte
		base := 2
		switch {
		case flags&geoByCoords != 0:
			base = 6
			if s.lon, s.lat, bad = parseLonLat(args[2], args[3]); bad != nil {
				return bad, nil
			}
			if bad = s.parseRadius(args[4:6]); bad != nil {
				return bad, nil
			}
		case flags&geoByMember != 0:
			base = 5
			if exists {
				var ok bool
				if s.lon, s.lat, ok, err = z.memberPoint(args[2]); err != nil || !ok {
					return errorReply(errGeoMember), err
				}
				if bad = s.parseRadius(args[3:5]); bad != nil {
					return bad, nil
				}
			}
		case flags&geoSearchStore != 0:
			base, store = 3, args[1]
		}
		var withDist, withHash, withCoord, stopEarly, storeDist, fromMember, fromLonLat, byRadius, byBox bool
		var order int
		var count int64
		for i := base; i < len(args); i++ {
			switch opt := upper(args[i]); {
			case opt == "WITHDIST":
				withDist = true
			case opt == "WITHHASH":
				withHash = true
			case opt == "WITHCOORD":
				withCoord = true
			case opt == "ANY":
				stopEarly = true
			case opt == "ASC":
				order = 1
			case opt == "DESC":
				order = -1
			case opt == "COUNT" && i+1 < len(args):
				var ok bool
				if count, ok = parseInt(args[i+1]); !ok {
					return errorReply(errNotInteger), nil
				}
				if count <= 0 {
					return errorReply("ERR COUNT must be > 0"), nil
				}
				i++
			case (opt == "STORE" || opt == "STOREDIST") && i+1 < len(args) && flags&(geoNoStore|geoSearch) == 0:
				store, storeDist = args[i+1], opt == "STOREDIST"
				i++
			case opt == "STOREDIST" && flags&geoSearchStore != 0:
				storeDist = true
			case opt == "FROMMEMBER" && i+1 < len(args) && flags&geoSearch != 0 && !fromLonLat:
				if exists {
					var ok bool
					if s.lon, s.lat, ok, err = z.memberPoint(args[i+1]); err != nil || !ok {
						return errorReply(errGeoMember), err
					}
				}
				fromMember = true
				i++
			case opt == "FROMLONLAT" && i+2 < len(args) && flags&geoSearch != 0 && !fromMember:
				if s.lon, s.lat, bad = parseLonLat(args[i+1], args[i+2]); bad != nil {
					return bad, nil
				}
				fromLonLat = true
				i += 2
			case opt == "BYRADIUS" && i+2 < len(args) && flags&geoSearch != 0 && !byBox:
				if bad = s.parseRadius(args[i+1 : i+3]); bad != nil {
					return bad, nil
				}
				byRadius = true
				i += 2
			case opt == "BYBOX" && i+3 < len(args) && flags&geoSearch != 0 && !byRadius:
				if bad = s.parseBox(args[i+1 : i+4]); bad != nil {
					return bad, nil
				}
				byBox = true
				i += 3
			default:
				return errorReply(errSyntax), nil
			}
		}
		switch {
		case store != nil && (withDist || withHash || withCoord):
			name := "STORE option in GEORADIUS"
			if flags&geoSearchStore != 0 {
				name = "GEOSEARCHSTORE"
			}
			return errorReply("ERR " + name + " is not compatible with WITHDIST, WITHHASH and WITHCOORD options"), nil
		case flags&geoSearch != 0 && !fromMember && !fromLonLat:
			return errorReply("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for " + string(args[0])), nil
		case flags&geoSearch != 0 && !byRadius && !byBox:
			return errorReply("ERR exactly one of BYRADIUS and BYBOX can be specified for " + string(args[0])), nil
		case stopEarly && count == 0:
			return errorReply("ERR the ANY argument requires COUNT argument"), nil
		case !exists && store != nil:
			return storeZSet(tx, store, nil)
		case !exists:
			return arrayReply{}, nil
		}
		if count != 0 && order == 0 && !stopEarly {
			order = 1
		}
		limit := 0
		if stopEarly {
			limit = int(count)
		}
		points, err := z.geoPoints(&s, limit)
		if err != nil {
			return nil, err
		}
		if order != 0 {
			slices.SortStableFunc(points, func(a, b geoPoint) int { return order * cmp.Compare(a.dist, b.dist) })
		}
		if count > 0 && int64(len(points)) > count {
			points = points[:count]
		}
		for i := range points {
			points[i].dist /= s.conversion
		}
		if store != nil {
			items := make([]zitem, len(points))
			for i, p := range points {
				score := p.score
				if storeDist {
					score = p.dist
				}
				items[i] = zitem{p.member, scoreKey(score)}
			}
			return storeZSet(tx, store, items)
		}
		out := make(arrayReply, len(points))
		for i, p := range points {
			if !withDist && !withHash && !withCoord {
				out[i] = bulkReply(p.member)
				continue
			}
			item := arrayReply{bulkReply(p.member)}
			if withDist {
				item = append(item, bulkReply(formatDistance(p.dist)))
			}
			if withHash {
				item = append(item, intReply(int64(p.score)))
			}
			if withCoord {
				item = append(item, arrayReply{doubleReply(formatCoord(p.lon)), doubleReply(formatCoord(p.lat))})
			}
			out[i] = item
		}
		return out, nil
	}
}
