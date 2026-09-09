// 有序集合新增命令（Redis 6.2+）。
// 对应 Redis t_zset.c 中的 zdiff/zinter/zunion/zmpop 等命令。
package commands

import (
	"fmt"
	"strconv"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// ZDIFF numkeys key [key ...] [WITHSCORES]
func zdiffCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	if numKeys < 1 || numKeys > len(ctx.Args)-2 {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	withScores := false
	keyEnd := 2 + numKeys
	if keyEnd < len(ctx.Args) && ctx.Args[keyEnd] == "WITHSCORES" {
		withScores = true
	}
	// 获取第一个集合的所有成员
	first := ctx.DB.LookupKey(ctx.Args[2])
	if first == nil || first.Type != object.TypeZSet {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	diff := make(map[string]float64)
	entries := first.Ptr.(*object.ZSet).Range(0, -1, true)
	for _, e := range entries {
		diff[e.Member] = e.Score
	}
	// 移除其他集合中的成员
	for i := 3; i < 2+numKeys; i++ {
		other := ctx.DB.LookupKey(ctx.Args[i])
		if other != nil && other.Type == object.TypeZSet {
			for _, e := range other.Ptr.(*object.ZSet).Range(0, -1, true) {
				delete(diff, e.Member)
			}
		}
	}
	result := make([]resp.RESPValue, 0, len(diff))
	for member, score := range diff {
		result = append(result, resp.BulkStringReply(member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", score)))
		}
	}
	ctx.Client.SendArray(result)
}

// ZDIFFSTORE destination numkeys key [key ...]
func zdiffstoreCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	dest := ctx.Args[1]
	numKeys, _ := strconv.Atoi(ctx.Args[2])
	if numKeys < 1 || numKeys > len(ctx.Args)-3 {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	first := ctx.DB.LookupKey(ctx.Args[3])
	if first == nil || first.Type != object.TypeZSet {
		ctx.DB.DeleteKey(dest)
		ctx.Client.SendInteger(0)
		return
	}
	diff := make(map[string]float64)
	entries := first.Ptr.(*object.ZSet).Range(0, -1, true)
	for _, e := range entries {
		diff[e.Member] = e.Score
	}
	for i := 4; i < 3+numKeys; i++ {
		other := ctx.DB.LookupKey(ctx.Args[i])
		if other != nil && other.Type == object.TypeZSet {
			for _, e := range other.Ptr.(*object.ZSet).Range(0, -1, true) {
				delete(diff, e.Member)
			}
		}
	}
	destVal := object.NewZSetObject()
	for member, score := range diff {
		destVal.Ptr.(*object.ZSet).Add(member, score)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendInteger(int64(len(diff)))
}

// ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
func zinterCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	withScores := false
	argsIdx := 2 + numKeys
	for argsIdx < len(ctx.Args) {
		switch ctx.Args[argsIdx] {
		case "WITHSCORES":
			withScores = true
			argsIdx++
		case "WEIGHTS":
			argsIdx += numKeys
		case "AGGREGATE":
			argsIdx += 2
		default:
			argsIdx++
		}
	}
	// 计算交集
	inter := make(map[string]float64)
	for i, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeZSet {
			ctx.Client.SendArray([]resp.RESPValue{})
			return
		}
		entries := val.Ptr.(*object.ZSet).Range(0, -1, true)
		if i == 0 {
			for _, e := range entries {
				inter[e.Member] = e.Score
			}
		} else {
			newInter := make(map[string]float64)
			for _, e := range entries {
				if _, exists := inter[e.Member]; exists {
					newInter[e.Member] = inter[e.Member] + e.Score
				}
			}
			inter = newInter
		}
	}
	result := make([]resp.RESPValue, 0, len(inter))
	for member, score := range inter {
		result = append(result, resp.BulkStringReply(member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", score)))
		}
	}
	ctx.Client.SendArray(result)
}

// ZINTERCARD numkeys key [key ...] [LIMIT limit]
func zintercardCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	inter := make(map[string]bool)
	for i, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeZSet {
			ctx.Client.SendInteger(0)
			return
		}
		entries := val.Ptr.(*object.ZSet).Range(0, -1, false)
		if i == 0 {
			for _, e := range entries {
				inter[e.Member] = true
			}
		} else {
			newInter := make(map[string]bool)
			for _, e := range entries {
				if inter[e.Member] {
					newInter[e.Member] = true
				}
			}
			inter = newInter
		}
	}
	ctx.Client.SendInteger(int64(len(inter)))
}

// ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
func zunionCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	withScores := false
	argsIdx := 2 + numKeys
	for argsIdx < len(ctx.Args) {
		switch ctx.Args[argsIdx] {
		case "WITHSCORES":
			withScores = true
			argsIdx++
		case "WEIGHTS":
			argsIdx += numKeys
		case "AGGREGATE":
			argsIdx += 2
		default:
			argsIdx++
		}
	}
	union := make(map[string]float64)
	for _, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeZSet {
			for _, e := range val.Ptr.(*object.ZSet).Range(0, -1, true) {
				if existing, ok := union[e.Member]; !ok || e.Score > existing {
					union[e.Member] = e.Score
				}
			}
		}
	}
	result := make([]resp.RESPValue, 0, len(union))
	for member, score := range union {
		result = append(result, resp.BulkStringReply(member))
		if withScores {
			result = append(result, resp.BulkStringReply(fmt.Sprintf("%g", score)))
		}
	}
	ctx.Client.SendArray(result)
}

// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
func zmpopCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	where := ctx.Args[2+numKeys]
	count := 1
	argsIdx := 3 + numKeys
	if argsIdx < len(ctx.Args) && ctx.Args[argsIdx] == "COUNT" && argsIdx+1 < len(ctx.Args) {
		count, _ = strconv.Atoi(ctx.Args[argsIdx+1])
	}
	for _, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeZSet {
			continue
		}
		zs := val.Ptr.(*object.ZSet)
		if zs.Len() == 0 {
			continue
		}
		var entries []object.ZSetEntry
		if where == "MIN" {
			entries = zs.Range(0, count-1, true)
		} else {
			total := zs.Len()
			start := total - count
			if start < 0 {
				start = 0
			}
			entries = zs.Range(start, total-1, true)
		}
		for _, e := range entries {
			zs.Remove(e.Member)
		}
		result := make([]resp.RESPValue, 0, len(entries))
		for _, e := range entries {
			result = append(result, resp.BulkStringReply(e.Member), resp.BulkStringReply(fmt.Sprintf("%g", e.Score)))
		}
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply(key),
			resp.ArrayReply(result),
		})
		return
	}
	ctx.Client.SendNull()
}

// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
func bzmpopCommand(ctx *CommandContext) {
	// 简化版：非阻塞
	zmpopCommand(ctx)
}

// BZPOPMAX key [key ...] timeout
func bzpopmaxCommand(ctx *CommandContext) {
	// 简化版：非阻塞
	for i := 1; i < len(ctx.Args)-1; i++ {
		key := ctx.Args[i]
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeZSet {
			zs := val.Ptr.(*object.ZSet)
			if zs.Len() > 0 {
				entries := zs.Range(zs.Len()-1, zs.Len()-1, true)
				if len(entries) > 0 {
					zs.Remove(entries[0].Member)
					ctx.Client.SendArray([]resp.RESPValue{
						resp.BulkStringReply(key),
						resp.BulkStringReply(entries[0].Member),
						resp.BulkStringReply(fmt.Sprintf("%g", entries[0].Score)),
					})
					return
				}
			}
		}
	}
	ctx.Client.SendNull()
}

// BZPOPMIN key [key ...] timeout
func bzpopminCommand(ctx *CommandContext) {
	// 简化版：非阻塞
	for i := 1; i < len(ctx.Args)-1; i++ {
		key := ctx.Args[i]
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeZSet {
			zs := val.Ptr.(*object.ZSet)
			if zs.Len() > 0 {
				entries := zs.Range(0, 0, true)
				if len(entries) > 0 {
					zs.Remove(entries[0].Member)
					ctx.Client.SendArray([]resp.RESPValue{
						resp.BulkStringReply(key),
						resp.BulkStringReply(entries[0].Member),
						resp.BulkStringReply(fmt.Sprintf("%g", entries[0].Score)),
					})
					return
				}
			}
		}
	}
	ctx.Client.SendNull()
}

// ZREMRANGEBYLEX key min max
func zremrangebylexCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
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
	// 简化：按字典序范围删除
	entries := val.Ptr.(*object.ZSet).Range(0, -1, false)
	removed := 0
	for _, e := range entries {
		if e.Member >= ctx.Args[2] && e.Member <= ctx.Args[3] {
			val.Ptr.(*object.ZSet).Remove(e.Member)
			removed++
		}
	}
	ctx.Client.SendInteger(int64(removed))
}

// ZREVRANGEBYLEX key max min [LIMIT offset count]
func zrevrangebylexCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
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
		if e.Member >= ctx.Args[3] && e.Member <= ctx.Args[2] {
			result = append([]resp.RESPValue{resp.BulkStringReply(e.Member)}, result...)
		}
	}
	ctx.Client.SendArray(result)
}
