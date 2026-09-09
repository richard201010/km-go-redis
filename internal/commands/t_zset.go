// 有序集合命令处理器 (t_zset.c equivalent)
package commands

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

func zaddCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	// Parse flags: NX, XX, GT, LT, CH, INCR
	nx, xx, ch, incr := false, false, false, false
	args := ctx.Args[2:]
	for len(args) > 0 {
		switch args[0] {
		case "NX":
			nx = true
			args = args[1:]
		case "XX":
			xx = true
			args = args[1:]
		case "GT":
			args = args[1:]
		case "LT":
			args = args[1:]
		case "CH":
			ch = true
			args = args[1:]
		case "INCR":
			incr = true
			args = args[1:]
		default:
			goto parseScores
		}
	}
parseScores:
	if len(args) < 2 || len(args)%2 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'zadd' command")
		return
	}
	if incr {
		score, err := strconv.ParseFloat(args[0], 64)
		if err != nil {
			ctx.Client.SendError("ERR value is not a valid float")
			return
		}
		member := args[1]
		val := ctx.DB.LookupKeyOrCreate(key, object.TypeZSet)
		if val.Type != object.TypeZSet {
			ctx.Client.ReplyTypeMismatch()
			return
		}
		newScore := val.Ptr.(*object.ZSet).IncrementBy(member, score)
		ctx.Client.SendBulkString(fmt.Sprintf("%g", newScore))
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
	for i := 0; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			ctx.Client.SendError("ERR value is not a valid float")
			return
		}
		member := args[i+1]
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

func zincrbyCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	delta, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not a valid float")
		return
	}
	member := ctx.Args[3]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeZSet)
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	newScore := val.Ptr.(*object.ZSet).IncrementBy(member, delta)
	ctx.Client.SendBulkString(fmt.Sprintf("%g", newScore))
}

func zremCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	zs := val.Ptr.(*object.ZSet)
	removed := 0
	for i := 2; i < len(ctx.Args); i++ {
		if zs.Remove(ctx.Args[i]) {
			removed++
		}
	}
	ctx.Client.SendInteger(int64(removed))
}

func zrangeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	start, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	stop, err := strconv.Atoi(ctx.Args[3])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	withScores := false
	for i := 4; i < len(ctx.Args); i++ {
		if ctx.Args[i] == "WITHSCORES" || ctx.Args[i] == "withscores" {
			withScores = true
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).Range(start, stop, withScores)
	result := make([]resp.RESPValue, 0, len(entries))
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
		}
	}
	ctx.Client.SendArray(result)
}

func zrevrangeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	start, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	stop, err := strconv.Atoi(ctx.Args[3])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	withScores := false
	for i := 4; i < len(ctx.Args); i++ {
		if ctx.Args[i] == "WITHSCORES" || ctx.Args[i] == "withscores" {
			withScores = true
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	zs := val.Ptr.(*object.ZSet)
	total := zs.Len()
	// Reverse: start from end
	rStart := total - 1 - stop
	rStop := total - 1 - start
	if rStart < 0 {
		rStart = 0
	}
	if rStop >= total {
		rStop = total - 1
	}
	entries := zs.Range(rStart, rStop, true)
	// Reverse the entries
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	result := make([]resp.RESPValue, 0, len(entries))
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
		}
	}
	ctx.Client.SendArray(result)
}

func zrangebyscoreCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	min, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		ctx.Client.SendError("ERR min or max is not a float")
		return
	}
	max, err := strconv.ParseFloat(ctx.Args[3], 64)
	if err != nil {
		ctx.Client.SendError("ERR min or max is not a float")
		return
	}
	withScores := false
	offset, count := 0, -1
	for i := 4; i < len(ctx.Args); i++ {
		switch ctx.Args[i] {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 < len(ctx.Args) {
				offset, _ = strconv.Atoi(ctx.Args[i+1])
				count, _ = strconv.Atoi(ctx.Args[i+2])
				i += 2
			}
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).RangeByScore(min, max, withScores, offset, count)
	result := make([]resp.RESPValue, 0, len(entries))
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
		}
	}
	ctx.Client.SendArray(result)
}

func zrevrangebyscoreCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	max, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		ctx.Client.SendError("ERR min or max is not a float")
		return
	}
	min, err := strconv.ParseFloat(ctx.Args[3], 64)
	if err != nil {
		ctx.Client.SendError("ERR min or max is not a float")
		return
	}
	withScores := false
	for i := 4; i < len(ctx.Args); i++ {
		if ctx.Args[i] == "WITHSCORES" {
			withScores = true
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).RangeByScore(min, max, withScores, 0, -1)
	// Reverse
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	result := make([]resp.RESPValue, 0, len(entries))
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
		}
	}
	ctx.Client.SendArray(result)
}

func zrangebylexCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	// Simplified: return all members as string comparison
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).Range(0, -1, false)
	result := make([]resp.RESPValue, 0)
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member))
	}
	ctx.Client.SendArray(result)
}

func zcardCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(int64(val.Ptr.(*object.ZSet).Len()))
}

func zscoreCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	member := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	score, ok := val.Ptr.(*object.ZSet).Score(member)
	if !ok {
		ctx.Client.SendNull()
		return
	}
	ctx.Client.SendBulkString(fmt.Sprintf("%g", score))
}

func zrankCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	member := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	rank, ok := val.Ptr.(*object.ZSet).Rank(member)
	if !ok {
		ctx.Client.SendNull()
		return
	}
	ctx.Client.SendInteger(int64(rank))
}

func zrevrankCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	member := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	rank, ok := val.Ptr.(*object.ZSet).RevRank(member)
	if !ok {
		ctx.Client.SendNull()
		return
	}
	ctx.Client.SendInteger(int64(rank))
}

func zcountCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	min, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		ctx.Client.SendError("ERR min or max is not a float")
		return
	}
	max, err := strconv.ParseFloat(ctx.Args[3], 64)
	if err != nil {
		ctx.Client.SendError("ERR min or max is not a float")
		return
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).RangeByScore(min, max, false, 0, -1)
	ctx.Client.SendInteger(int64(len(entries)))
}

func zremrangebyrankCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	start, _ := strconv.Atoi(ctx.Args[2])
	stop, _ := strconv.Atoi(ctx.Args[3])
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	removed := val.Ptr.(*object.ZSet).RemoveRangeByRank(start, stop)
	ctx.Client.SendInteger(int64(removed))
}

func zremrangebyscoreCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	min, _ := strconv.ParseFloat(ctx.Args[2], 64)
	max, _ := strconv.ParseFloat(ctx.Args[3], 64)
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	removed := val.Ptr.(*object.ZSet).RemoveRangeByScore(min, max)
	ctx.Client.SendInteger(int64(removed))
}

func zunionstoreCommand(ctx *CommandContext) {
	dest := ctx.Args[1]
	numKeys, _ := strconv.Atoi(ctx.Args[2])
	// Parse weights and aggregate
	weights := make([]float64, numKeys)
	aggregate := "SUM"
	argsOffset := 3 + numKeys
	for i := argsOffset; i < len(ctx.Args); i++ {
		switch ctx.Args[i] {
		case "WEIGHTS":
			for j := 0; j < numKeys && i+1+j < len(ctx.Args); j++ {
				weights[j], _ = strconv.ParseFloat(ctx.Args[i+1+j], 64)
			}
			i += numKeys
		case "AGGREGATE":
			if i+1 < len(ctx.Args) {
				aggregate = ctx.Args[i+1]
				i++
			}
		}
	}
	for i := range weights {
		if weights[i] == 0 {
			weights[i] = 1
		}
	}
	union := make(map[string]float64)
	for i := 0; i < numKeys; i++ {
		val := ctx.DB.LookupKey(ctx.Args[3+i])
		if val != nil && val.Type == object.TypeZSet {
			entries := val.Ptr.(*object.ZSet).Range(0, -1, true)
			for _, e := range entries {
				w := e.Score * weights[i]
				if existing, ok := union[e.Member]; ok {
					switch aggregate {
					case "SUM":
						union[e.Member] = existing + w
					case "MIN":
						if w < existing {
							union[e.Member] = w
						}
					case "MAX":
						if w > existing {
							union[e.Member] = w
						}
					}
				} else {
					union[e.Member] = w
				}
			}
		}
	}
	destVal := object.NewZSetObject()
	destZS := destVal.Ptr.(*object.ZSet)
	for m, s := range union {
		destZS.Add(m, s)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendInteger(int64(len(union)))
}

func zinterstoreCommand(ctx *CommandContext) {
	// Same as zunionstore but with intersection semantics
	dest := ctx.Args[1]
	numKeys, _ := strconv.Atoi(ctx.Args[2])
	weights := make([]float64, numKeys)
	aggregate := "SUM"
	argsOffset := 3 + numKeys
	for i := argsOffset; i < len(ctx.Args); i++ {
		switch ctx.Args[i] {
		case "WEIGHTS":
			for j := 0; j < numKeys && i+1+j < len(ctx.Args); j++ {
				weights[j], _ = strconv.ParseFloat(ctx.Args[i+1+j], 64)
			}
			i += numKeys
		case "AGGREGATE":
			if i+1 < len(ctx.Args) {
				aggregate = ctx.Args[i+1]
				i++
			}
		}
	}
	for i := range weights {
		if weights[i] == 0 {
			weights[i] = 1
		}
	}
	// Find intersection
	var inter map[string]float64
	for i := 0; i < numKeys; i++ {
		val := ctx.DB.LookupKey(ctx.Args[3+i])
		if val == nil || val.Type != object.TypeZSet {
			inter = make(map[string]float64)
			break
		}
		entries := val.Ptr.(*object.ZSet).Range(0, -1, true)
		current := make(map[string]float64)
		for _, e := range entries {
			current[e.Member] = e.Score * weights[i]
		}
		if inter == nil {
			inter = current
		} else {
			for m := range inter {
				if s, ok := current[m]; ok {
					switch aggregate {
					case "SUM":
						inter[m] = inter[m] + s
					case "MIN":
						if s < inter[m] {
							inter[m] = s
						}
					case "MAX":
						if s > inter[m] {
							inter[m] = s
						}
					}
				} else {
					delete(inter, m)
				}
			}
		}
	}
	if inter == nil {
		inter = make(map[string]float64)
	}
	destVal := object.NewZSetObject()
	destZS := destVal.Ptr.(*object.ZSet)
	for m, s := range inter {
		destZS.Add(m, s)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendInteger(int64(len(inter)))
}

func zlexcountCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(int64(val.Ptr.(*object.ZSet).Len()))
}

func zpopminCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		count, _ = strconv.Atoi(ctx.Args[2])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray(nil)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	zs := val.Ptr.(*object.ZSet)
	entries := zs.Range(0, count-1, true)
	for _, e := range entries {
		zs.Remove(e.Member)
	}
	result := make([]resp.RESPValue, 0, len(entries)*2)
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member), resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
	}
	ctx.Client.SendArray(result)
}

func zpopmaxCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		count, _ = strconv.Atoi(ctx.Args[2])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray(nil)
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	zs := val.Ptr.(*object.ZSet)
	total := zs.Len()
	start := total - count
	if start < 0 {
		start = 0
	}
	entries := zs.Range(start, total-1, true)
	for _, e := range entries {
		zs.Remove(e.Member)
	}
	// Reverse
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	result := make([]resp.RESPValue, 0, len(entries)*2)
	for _, e := range entries {
		result = append(result, resp.BulkStringReply(e.Member), resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
	}
	ctx.Client.SendArray(result)
}

func zscanCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	cursor, _ := strconv.ParseUint(ctx.Args[2], 10, 64)
	pattern := ""
	count := 10
	for i := 3; i < len(ctx.Args); i += 2 {
		switch ctx.Args[i] {
		case "MATCH":
			if i+1 < len(ctx.Args) {
				pattern = ctx.Args[i+1]
			}
		case "COUNT":
			if i+1 < len(ctx.Args) {
				count, _ = strconv.Atoi(ctx.Args[i+1])
			}
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("0"),
			resp.ArrayReply([]resp.RESPValue{}),
		})
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).Range(0, -1, true)
	if int(cursor) >= len(entries) {
		cursor = 0
	}
	result := make([]resp.RESPValue, 0)
	for i := int(cursor); i < len(entries); i++ {
		if pattern != "" && pattern != "*" && !matchPattern(pattern, entries[i].Member) {
			continue
		}
		result = append(result, resp.BulkStringReply(entries[i].Member), resp.BulkStringReply(fmt.Sprintf("%g", entries[i].Score)))
		if len(result) >= count*2 {
			break
		}
	}
	newCursor := uint64(int(cursor) + count)
	if newCursor >= uint64(len(entries)) {
		newCursor = 0
	}
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply(strconv.FormatUint(newCursor, 10)),
		resp.ArrayReply(result),
	})
}

func zrandmemberCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		count, _ = strconv.Atoi(ctx.Args[2])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		if len(ctx.Args) > 2 {
			ctx.Client.SendArray(nil)
		} else {
			ctx.Client.SendNull()
		}
		return
	}
	if val.Type != object.TypeZSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	entries := val.Ptr.(*object.ZSet).Range(0, -1, false)
	if len(entries) == 0 {
		if len(ctx.Args) > 2 {
			ctx.Client.SendArray(nil)
		} else {
			ctx.Client.SendNull()
		}
		return
	}
	negative := count < 0
	if negative {
		count = -count
	}
	result := make([]resp.RESPValue, 0, count)
	if negative {
		for i := 0; i < count; i++ {
			e := entries[rand.Intn(len(entries))]
			result = append(result, resp.BulkStringReply(e.Member))
		}
	} else {
		if count > len(entries) {
			count = len(entries)
		}
		shuffled := make([]object.ZSetEntry, len(entries))
		copy(shuffled, entries)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		for i := 0; i < count; i++ {
			result = append(result, resp.BulkStringReply(shuffled[i].Member))
		}
	}
	ctx.Client.SendArray(result)
}

func zmscoreCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	result := make([]resp.RESPValue, len(ctx.Args)-2)
	for i := 2; i < len(ctx.Args); i++ {
		if val == nil || val.Type != object.TypeZSet {
			result[i-2] = resp.NullBulk
			continue
		}
		score, ok := val.Ptr.(*object.ZSet).Score(ctx.Args[i])
		if !ok {
			result[i-2] = resp.NullBulk
		} else {
			result[i-2] = resp.BulkStringReply(fmt.Sprintf("%g", score))
		}
	}
	ctx.Client.SendArray(result)
}
