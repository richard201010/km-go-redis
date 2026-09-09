// 集合新增命令（Redis 6.2+）。
// 对应 Redis t_set.c 中的 smismember/sintercard/sdiffcard/sunioncard。
package commands

import (
	"strconv"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// SMISMEMBER key member [member ...]
func smismemberCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	members := ctx.Args[2:]
	val := ctx.DB.LookupKey(key)
	result := make([]resp.RESPValue, len(members))
	if val == nil || val.Type != object.TypeSet {
		for i := range result {
			result[i] = resp.IntegerReply(0)
		}
		ctx.Client.SendArray(result)
		return
	}
	s := val.Ptr.(*object.Set)
	for i, m := range members {
		if s.Contains(m) {
			result[i] = resp.IntegerReply(1)
		} else {
			result[i] = resp.IntegerReply(0)
		}
	}
	ctx.Client.SendArray(result)
}

// SINTERCARD numkeys key [key ...] [LIMIT limit]
func sintercardCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	inter := make(map[string]bool)
	for i, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeSet {
			ctx.Client.SendInteger(0)
			return
		}
		members := val.Ptr.(*object.Set).Members()
		if i == 0 {
			for _, m := range members {
				inter[m] = true
			}
		} else {
			newInter := make(map[string]bool)
			for _, m := range members {
				if inter[m] {
					newInter[m] = true
				}
			}
			inter = newInter
		}
	}
	ctx.Client.SendInteger(int64(len(inter)))
}

// SDIFFCARD numkeys key [key ...]
func sdiffcardCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	diff := make(map[string]bool)
	for i, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeSet {
			if i == 0 {
				ctx.Client.SendInteger(0)
				return
			}
			continue
		}
		members := val.Ptr.(*object.Set).Members()
		if i == 0 {
			for _, m := range members {
				diff[m] = true
			}
		} else {
			for _, m := range members {
				delete(diff, m)
			}
		}
	}
	ctx.Client.SendInteger(int64(len(diff)))
}

// SUNIONCARD numkeys key [key ...]
func sunioncardCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	numKeys, _ := strconv.Atoi(ctx.Args[1])
	keys := ctx.Args[2 : 2+numKeys]
	union := make(map[string]bool)
	for _, key := range keys {
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeSet {
			for _, m := range val.Ptr.(*object.Set).Members() {
				union[m] = true
			}
		}
	}
	ctx.Client.SendInteger(int64(len(union)))
}
