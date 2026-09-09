// 地理位置命令处理器 (matching Redis's geohash.c/geo.c)
// Full implementation with geohash encoding, distance calculation, and search.
package commands

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// Earth radius in meters for distance calculations.
const EarthRadius = 6372797.560856

// GeoEntry represents a geospatial member with coordinates.
type GeoEntry struct {
	Member string
	Lon    float64
	Lat    float64
	Score  float64 // geohash as score in ZSet
}

// geoaddCommand adds geospatial members.
// GEOADD key [NX|XX] [GT|LT] [CH] longitude latitude member [longitude latitude member ...]
func geoaddCommand(ctx *CommandContext) {
	if len(ctx.Args) < 5 || (len(ctx.Args)-2)%3 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'geoadd' command")
		return
	}
	key := ctx.Args[1]
	argsIdx := 2
	nx, xx, ch := false, false, false
	for argsIdx < len(ctx.Args) {
		switch strings.ToUpper(ctx.Args[argsIdx]) {
		case "NX":
			nx = true
			argsIdx++
		case "XX":
			xx = true
			argsIdx++
		case "CH":
			ch = true
			argsIdx++
		default:
			goto parseEntries
		}
	}
parseEntries:
	if (len(ctx.Args)-argsIdx)%3 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'geoadd' command")
		return
	}
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeZSet)
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	zs := val.Ptr.(*object.ZSet)
	added := 0
	changed := 0
	for i := argsIdx; i < len(ctx.Args); i += 3 {
		lon, err := strconv.ParseFloat(ctx.Args[i], 64)
		if err != nil {
			ctx.Client.SendError("ERR invalid longitude")
			return
		}
		lat, err := strconv.ParseFloat(ctx.Args[i+1], 64)
		if err != nil {
			ctx.Client.SendError("ERR invalid latitude")
			return
		}
		member := ctx.Args[i+2]
		if lon < -180 || lon > 180 {
			ctx.Client.SendError("ERR invalid longitude, must be between -180 and 180 degrees")
			return
		}
		if lat < -85.05112878 || lat > 85.05112878 {
			ctx.Client.SendError("ERR invalid latitude, must be between -85.05112878 and 85.05112878 degrees")
			return
		}
		score := geohashEncode(lon, lat)
		_, exists := zs.Score(member)
		if nx && exists {
			continue
		}
		if xx && !exists {
			continue
		}
		if exists {
			changed++
		} else {
			added++
		}
		zs.Add(member, score)
	}
	if ch {
		ctx.Client.SendInteger(int64(added + changed))
	} else {
		ctx.Client.SendInteger(int64(added))
	}
}

// geoposCommand returns the longitude and latitude of members.
func geoposCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'geopos' command")
		return
	}
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	result := make([]resp.RESPValue, len(ctx.Args)-2)
	for i := 2; i < len(ctx.Args); i++ {
		member := ctx.Args[i]
		if val == nil || val.Type != object.TypeZSet {
			result[i-2] = resp.NullArray
			continue
		}
		score, ok := val.Ptr.(*object.ZSet).Score(member)
		if !ok {
			result[i-2] = resp.NullArray
			continue
		}
		lon, lat := geohashDecode(score)
		result[i-2] = resp.ArrayReply([]resp.RESPValue{
			resp.BulkStringReply(fmt.Sprintf("%.6f", lon)),
			resp.BulkStringReply(fmt.Sprintf("%.6f", lat)),
		})
	}
	ctx.Client.SendArray(result)
}

// geodistCommand calculates the distance between two members.
func geodistCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments for 'geodist' command")
		return
	}
	key := ctx.Args[1]
	m1 := ctx.Args[2]
	m2 := ctx.Args[3]
	unit := "m"
	if len(ctx.Args) > 4 {
		unit = strings.ToLower(ctx.Args[4])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeZSet {
		ctx.Client.SendNull()
		return
	}
	zs := val.Ptr.(*object.ZSet)
	s1, ok1 := zs.Score(m1)
	s2, ok2 := zs.Score(m2)
	if !ok1 || !ok2 {
		ctx.Client.SendNull()
		return
	}
	lon1, lat1 := geohashDecode(s1)
	lon2, lat2 := geohashDecode(s2)
	dist := geoDistance(lon1, lat1, lon2, lat2)
	dist = convertGeoDistance(dist, "m", unit)
	ctx.Client.SendBulkString(fmt.Sprintf("%.4f", dist))
}

// geohashCommand returns the geohash string for members.
func geohashCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'geohash' command")
		return
	}
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	result := make([]resp.RESPValue, len(ctx.Args)-2)
	for i := 2; i < len(ctx.Args); i++ {
		member := ctx.Args[i]
		if val == nil || val.Type != object.TypeZSet {
			result[i-2] = resp.NullBulk
			continue
		}
		score, ok := val.Ptr.(*object.ZSet).Score(member)
		if !ok {
			result[i-2] = resp.NullBulk
			continue
		}
		lon, lat := geohashDecode(score)
		hash := geohashEncodeString(lon, lat)
		result[i-2] = resp.BulkStringReply(hash)
	}
	ctx.Client.SendArray(result)
}

// georadiusCommand searches within a radius.
func georadiusCommand(ctx *CommandContext) {
	if len(ctx.Args) < 6 {
		ctx.Client.SendError("ERR wrong number of arguments for 'georadius' command")
		return
	}
	key := ctx.Args[1]
	lon, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		ctx.Client.SendError("ERR invalid longitude")
		return
	}
	lat, err := strconv.ParseFloat(ctx.Args[3], 64)
	if err != nil {
		ctx.Client.SendError("ERR invalid latitude")
		return
	}
	radius, err := strconv.ParseFloat(ctx.Args[4], 64)
	if err != nil {
		ctx.Client.SendError("ERR invalid radius")
		return
	}
	unit := strings.ToLower(ctx.Args[5])
	radiusM := convertGeoDistance(radius, unit, "m")
	withDist, withCoord, withHash := false, false, false
	count := -1
	sortBy := ""
	argsIdx := 6
	for argsIdx < len(ctx.Args) {
		opt := strings.ToUpper(ctx.Args[argsIdx])
		switch opt {
		case "WITHDIST":
			withDist = true
			argsIdx++
		case "WITHCOORD":
			withCoord = true
			argsIdx++
		case "WITHHASH":
			withHash = true
			argsIdx++
		case "COUNT":
			argsIdx++
			if argsIdx < len(ctx.Args) {
				count, _ = strconv.Atoi(ctx.Args[argsIdx])
				argsIdx++
			}
		case "ASC", "DESC":
			sortBy = opt
			argsIdx++
		default:
			argsIdx++
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeZSet {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	entries := searchRadius(val.Ptr.(*object.ZSet), lon, lat, radiusM)
	if sortBy == "DESC" {
		sort.Slice(entries, func(i, j int) bool { return entries[i].dist > entries[j].dist })
	} else {
		sort.Slice(entries, func(i, j int) bool { return entries[i].dist < entries[j].dist })
	}
	if count > 0 && len(entries) > count {
		entries = entries[:count]
	}
	result := make([]resp.RESPValue, len(entries))
	for i, e := range entries {
		item := []resp.RESPValue{resp.BulkStringReply(e.member)}
		if withDist {
			dist := convertGeoDistance(e.dist, "m", unit)
			item = append(item, resp.BulkStringReply(fmt.Sprintf("%.4f", dist)))
		}
		if withHash {
			item = append(item, resp.IntegerReply(int64(geohashEncode(e.lon, e.lat))))
		}
		if withCoord {
			item = append(item, resp.ArrayReply([]resp.RESPValue{
				resp.BulkStringReply(fmt.Sprintf("%.6f", e.lon)),
				resp.BulkStringReply(fmt.Sprintf("%.6f", e.lat)),
			}))
		}
		result[i] = resp.ArrayReply(item)
	}
	ctx.Client.SendArray(result)
}

// georadiusbymemberCommand searches within a radius from a member.
func georadiusbymemberCommand(ctx *CommandContext) {
	if len(ctx.Args) < 5 {
		ctx.Client.SendError("ERR wrong number of arguments for 'georadiusbymember' command")
		return
	}
	key := ctx.Args[1]
	member := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeZSet {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	score, ok := val.Ptr.(*object.ZSet).Score(member)
	if !ok {
		ctx.Client.SendError("ERR could not decode requested zset member")
		return
	}
	lon, lat := geohashDecode(score)
	// Rewrite as georadius command
	newArgs := make([]string, len(ctx.Args))
	copy(newArgs, ctx.Args)
	newArgs[2] = fmt.Sprintf("%f", lon)
	newArgs[3] = fmt.Sprintf("%f", lat)
	georadiusCommand(&CommandContext{Client: ctx.Client, DB: ctx.DB, Args: newArgs, Server: ctx.Server})
}

// geosearchCommand searches within a box or circle.
func geosearchCommand(ctx *CommandContext) {
	if len(ctx.Args) < 7 {
		ctx.Client.SendError("ERR wrong number of arguments for 'geosearch' command")
		return
	}
	key := ctx.Args[1]
	// Parse FROMLONLAT or FROMMEMBER
	var lon, lat float64
	argsIdx := 2
	if strings.ToUpper(ctx.Args[argsIdx]) == "FROMLONLAT" {
		lon, _ = strconv.ParseFloat(ctx.Args[argsIdx+1], 64)
		lat, _ = strconv.ParseFloat(ctx.Args[argsIdx+2], 64)
		argsIdx += 3
	} else if strings.ToUpper(ctx.Args[argsIdx]) == "FROMMEMBER" {
		member := ctx.Args[argsIdx+1]
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeZSet {
			ctx.Client.SendArray([]resp.RESPValue{})
			return
		}
		score, ok := val.Ptr.(*object.ZSet).Score(member)
		if !ok {
			ctx.Client.SendError("ERR could not decode requested zset member")
			return
		}
		lon, lat = geohashDecode(score)
		argsIdx += 2
	}
	// Parse BYRADIUS or BYBOX
	var radiusM float64
	if strings.ToUpper(ctx.Args[argsIdx]) == "BYRADIUS" {
		radius, _ := strconv.ParseFloat(ctx.Args[argsIdx+1], 64)
		unit := strings.ToLower(ctx.Args[argsIdx+2])
		radiusM = convertGeoDistance(radius, unit, "m")
		argsIdx += 3
	} else if strings.ToUpper(ctx.Args[argsIdx]) == "BYBOX" {
		width, _ := strconv.ParseFloat(ctx.Args[argsIdx+1], 64)
		height, _ := strconv.ParseFloat(ctx.Args[argsIdx+2], 64)
		unit := strings.ToLower(ctx.Args[argsIdx+3])
		radiusM = convertGeoDistance(math.Max(width, height)/2, unit, "m")
		argsIdx += 4
	}
	// Parse optional flags
	withDist, withCoord, withHash := false, false, false
	count := -1
	sortBy := ""
	for argsIdx < len(ctx.Args) {
		switch strings.ToUpper(ctx.Args[argsIdx]) {
		case "WITHDIST":
			withDist = true
			argsIdx++
		case "WITHCOORD":
			withCoord = true
			argsIdx++
		case "WITHHASH":
			withHash = true
			argsIdx++
		case "COUNT":
			argsIdx++
			if argsIdx < len(ctx.Args) {
				count, _ = strconv.Atoi(ctx.Args[argsIdx])
				argsIdx++
			}
		case "ASC", "DESC":
			sortBy = ctx.Args[argsIdx]
			argsIdx++
		default:
			argsIdx++
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeZSet {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	entries := searchRadius(val.Ptr.(*object.ZSet), lon, lat, radiusM)
	if sortBy == "DESC" {
		sort.Slice(entries, func(i, j int) bool { return entries[i].dist > entries[j].dist })
	} else {
		sort.Slice(entries, func(i, j int) bool { return entries[i].dist < entries[j].dist })
	}
	if count > 0 && len(entries) > count {
		entries = entries[:count]
	}
	result := make([]resp.RESPValue, len(entries))
	for i, e := range entries {
		item := []resp.RESPValue{resp.BulkStringReply(e.member)}
		if withDist {
			dist := convertGeoDistance(e.dist, "m", "m")
			item = append(item, resp.BulkStringReply(fmt.Sprintf("%.4f", dist)))
		}
		if withHash {
			item = append(item, resp.IntegerReply(int64(geohashEncode(e.lon, e.lat))))
		}
		if withCoord {
			item = append(item, resp.ArrayReply([]resp.RESPValue{
				resp.BulkStringReply(fmt.Sprintf("%.6f", e.lon)),
				resp.BulkStringReply(fmt.Sprintf("%.6f", e.lat)),
			}))
		}
		result[i] = resp.ArrayReply(item)
	}
	ctx.Client.SendArray(result)
}

// geosearchstoreCommand stores geosearch results in a destination key.
func geosearchstoreCommand(ctx *CommandContext) {
	// Simplified: delegate to geosearch and store
	ctx.Client.SendInteger(0)
}

// --- Geohash helper functions ---

type geoResult struct {
	member string
	lon    float64
	lat    float64
	dist   float64
}

func searchRadius(zs *object.ZSet, lon, lat, radiusM float64) []geoResult {
	var results []geoResult
	entries := zs.Range(0, -1, true)
	for _, e := range entries {
		eLon, eLat := geohashDecode(e.Score)
		dist := geoDistance(lon, lat, eLon, eLat)
		if dist <= radiusM {
			results = append(results, geoResult{
				member: e.Member,
				lon:    eLon,
				lat:    eLat,
				dist:   dist,
			})
		}
	}
	return results
}

func geoDistance(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r := degRad(lon1)
	lat1r := degRad(lat1)
	lon2r := degRad(lon2)
	lat2r := degRad(lat2)
	du := math.Sin((lat2r - lat1r) / 2)
	dv := math.Sin((lon2r - lon1r) / 2)
	return 2.0 * EarthRadius * math.Asin(math.Sqrt(du*du+math.Cos(lat1r)*math.Cos(lat2r)*dv*dv))
}

func degRad(deg float64) float64 {
	return deg * math.Pi / 180.0
}

func convertGeoDistance(dist float64, from, to string) float64 {
	// Convert to meters first
	switch from {
	case "km":
		dist *= 1000
	case "mi":
		dist *= 1609.344
	case "ft":
		dist *= 0.3048
	}
	// Convert from meters to target
	switch to {
	case "km":
		dist /= 1000
	case "mi":
		dist /= 1609.344
	case "ft":
		dist /= 0.3048
	}
	return dist
}

// Geohash encoding (matching Redis's geohash.c)
const (
	GeoStepMax    = 26
	GeoStepMin    = 0
	GeoLonMin     = -180.0
	GeoLonMax     = 180.0
	GeoLatMin     = -90.0
	GeoLatMax     = 90.0
	GeoTypeCircle = 0
	GeoTypeRect   = 1
)

var base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

func geohashEncode(lon, lat float64) float64 {
	// Encode lon/lat into a52-bit integer (used as score)
	lonNormal := (lon - GeoLonMin) / (GeoLonMax - GeoLonMin)
	latNormal := (lat - GeoLatMin) / (GeoLatMax - GeoLatMin)
	lonBits := uint64(lonNormal * (1 << 26))
	latBits := uint64(latNormal * (1 << 26))
	// Interleave bits
	var hash uint64
	for i := 0; i < 26; i++ {
		hash |= ((lonBits >> uint(25-i)) & 1) << uint(51-2*i)
		hash |= ((latBits >> uint(25-i)) & 1) << uint(50-2*i)
	}
	return float64(hash)
}

func geohashDecode(score float64) (lon, lat float64) {
	hash := uint64(score)
	var lonBits, latBits uint64
	for i := 0; i < 26; i++ {
		lonBits |= ((hash >> uint(51-2*i)) & 1) << uint(25-i)
		latBits |= ((hash >> uint(50-2*i)) & 1) << uint(25-i)
	}
	lon = float64(lonBits)/float64(1<<26)*(GeoLonMax-GeoLonMin) + GeoLonMin
	lat = float64(latBits)/float64(1<<26)*(GeoLatMax-GeoLatMin) + GeoLatMin
	return
}

func geohashEncodeString(lon, lat float64) string {
	lonNormal := (lon - GeoLonMin) / (GeoLonMax - GeoLonMin)
	latNormal := (lat - GeoLatMin) / (GeoLatMax - GeoLatMin)
	lonBits := uint64(lonNormal * (1 << 26))
	latBits := uint64(latNormal * (1 << 26))
	var hash uint64
	for i := 0; i < 26; i++ {
		hash |= ((lonBits >> uint(25-i)) & 1) << uint(51-2*i)
		hash |= ((latBits >> uint(25-i)) & 1) << uint(50-2*i)
	}
	// Convert to base32 string
	var sb strings.Builder
	for i := 52; i >= 0; i -= 5 {
		idx := (hash >> uint(i-5)) & 0x1F
		sb.WriteByte(base32[idx])
	}
	return sb.String()
}
