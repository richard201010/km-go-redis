// 哈希子键过期命令（Redis 7.4+ 新特性）。
// 对应 Redis t_hash.c 中的 hexpire/hexpireat/httl/hpersist 等命令。
// Hash 字段级过期需要在 Object 层面存储 subexpires。
package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// hexpireGeneric 通用 Hash 字段过期设置。
// 对应 Redis 的 hexpireGenericCommand()。
func hexpireGeneric(ctx *CommandContext, unit string) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	ttlVal, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	fields := ctx.Args[3:]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		// 返回 -2 数组（键不存在）
		result := make([]resp.RESPValue, len(fields))
		for i := range result {
			result[i] = resp.IntegerReply(-2)
		}
		ctx.Client.SendArray(result)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		_, exists := h.Get(field)
		if !exists {
			result[i] = resp.IntegerReply(-2) // 字段不存在
			continue
		}
		var expireAt time.Time
		switch unit {
		case "s":
			if ttlVal <= 0 {
				result[i] = resp.IntegerReply(-1)
				continue
			}
			expireAt = time.Now().Add(time.Duration(ttlVal) * time.Second)
		case "ms":
			if ttlVal <= 0 {
				result[i] = resp.IntegerReply(-1)
				continue
			}
			expireAt = time.Now().Add(time.Duration(ttlVal) * time.Millisecond)
		case "at-s":
			expireAt = time.Unix(ttlVal, 0)
		case "at-ms":
			expireAt = time.UnixMilli(ttlVal)
		}
		// 存储字段过期（通过特殊的键名格式）
		expireKey := fmt.Sprintf("__hexpire:%s:%s", key, field)
		ctx.DB.SetExpire(expireKey, expireAt)
		result[i] = resp.IntegerReply(1) // 设置成功
	}
	ctx.Client.SendArray(result)
}

// HEXPIRE key seconds field [field ...]
func hexpireCommand(ctx *CommandContext) {
	hexpireGeneric(ctx, "s")
}

// HEXPIREAT key unix-time-seconds field [field ...]
func hexpireatCommand(ctx *CommandContext) {
	hexpireGeneric(ctx, "at-s")
}

// HPEXPIRE key milliseconds field [field ...]
func hpexpireCommand(ctx *CommandContext) {
	hexpireGeneric(ctx, "ms")
}

// HPEXPIREAT key unix-time-milliseconds field [field ...]
func hpexpireatCommand(ctx *CommandContext) {
	hexpireGeneric(ctx, "at-ms")
}

// HPERSIST key field [field ...]
func hpersistCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	fields := ctx.Args[2:]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		result := make([]resp.RESPValue, len(fields))
		for i := range result {
			result[i] = resp.IntegerReply(-2)
		}
		ctx.Client.SendArray(result)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		_, exists := h.Get(field)
		if !exists {
			result[i] = resp.IntegerReply(-2)
			continue
		}
		expireKey := fmt.Sprintf("__hexpire:%s:%s", key, field)
		if ctx.DB.Persist(expireKey) {
			result[i] = resp.IntegerReply(1) // 过期被移除
		} else {
			result[i] = resp.IntegerReply(-1) // 没有设置过期
		}
	}
	ctx.Client.SendArray(result)
}

// httlGeneric 通用 Hash 字段 TTL 查询。
func httlGeneric(ctx *CommandContext, unit string) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	fields := ctx.Args[2:]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		result := make([]resp.RESPValue, len(fields))
		for i := range result {
			result[i] = resp.IntegerReply(-2)
		}
		ctx.Client.SendArray(result)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		_, exists := h.Get(field)
		if !exists {
			result[i] = resp.IntegerReply(-2)
			continue
		}
		expireKey := fmt.Sprintf("__hexpire:%s:%s", key, field)
		expireAt, ok := ctx.DB.GetExpire(expireKey)
		if !ok {
			result[i] = resp.IntegerReply(-1) // 没有设置过期
			continue
		}
		var ttl int64
		switch unit {
		case "s":
			ttl = int64(time.Until(expireAt).Seconds())
		case "ms":
			ttl = time.Until(expireAt).Milliseconds()
		}
		if ttl <= 0 {
			result[i] = resp.IntegerReply(-2) // 已过期
		} else {
			result[i] = resp.IntegerReply(ttl)
		}
	}
	ctx.Client.SendArray(result)
}

// HTTL key field [field ...]
func httlCommand(ctx *CommandContext) {
	httlGeneric(ctx, "s")
}

// HPTTL key field [field ...]
func hpttlCommand(ctx *CommandContext) {
	httlGeneric(ctx, "ms")
}

// HEXPIRETIME key field [field ...]
func hexpiretimeCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	fields := ctx.Args[2:]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		result := make([]resp.RESPValue, len(fields))
		for i := range result {
			result[i] = resp.IntegerReply(-2)
		}
		ctx.Client.SendArray(result)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		_, exists := h.Get(field)
		if !exists {
			result[i] = resp.IntegerReply(-2)
			continue
		}
		expireKey := fmt.Sprintf("__hexpire:%s:%s", key, field)
		expireAt, ok := ctx.DB.GetExpire(expireKey)
		if !ok {
			result[i] = resp.IntegerReply(-1)
		} else {
			result[i] = resp.IntegerReply(expireAt.Unix())
		}
	}
	ctx.Client.SendArray(result)
}

// HPEXPIRETIME key field [field ...]
func hpexpiretimeCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	fields := ctx.Args[2:]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		result := make([]resp.RESPValue, len(fields))
		for i := range result {
			result[i] = resp.IntegerReply(-2)
		}
		ctx.Client.SendArray(result)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		_, exists := h.Get(field)
		if !exists {
			result[i] = resp.IntegerReply(-2)
			continue
		}
		expireKey := fmt.Sprintf("__hexpire:%s:%s", key, field)
		expireAt, ok := ctx.DB.GetExpire(expireKey)
		if !ok {
			result[i] = resp.IntegerReply(-1)
		} else {
			result[i] = resp.IntegerReply(expireAt.UnixMilli())
		}
	}
	ctx.Client.SendArray(result)
}

// HGETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST] field [field ...]
func hgetexCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	// 简化：只返回字段值
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	fields := ctx.Args[2:]
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		v, ok := h.Get(field)
		if !ok {
			result[i] = resp.NullBulk
		} else {
			result[i] = resp.BulkStringReply(v)
		}
	}
	ctx.Client.SendArray(result)
}

// HGETDEL key field [field ...]
func hgetdelCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	fields := ctx.Args[2:]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		result := make([]resp.RESPValue, len(fields))
		for i := range result {
			result[i] = resp.NullBulk
		}
		ctx.Client.SendArray(result)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	result := make([]resp.RESPValue, len(fields))
	for i, field := range fields {
		v, ok := h.Get(field)
		if !ok {
			result[i] = resp.NullBulk
		} else {
			result[i] = resp.BulkStringReply(v)
			h.Delete(field)
		}
	}
	ctx.Client.SendArray(result)
}

// HSETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds] field value [field value ...]
func hsetexCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	key := ctx.Args[1]
	argsIdx := 2
	// 跳过过期选项
	for argsIdx < len(ctx.Args) {
		opt := strings.ToUpper(ctx.Args[argsIdx])
		if opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT" {
			argsIdx += 2
		} else {
			break
		}
	}
	if (len(ctx.Args)-argsIdx)%2 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments")
		return
	}
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	added := 0
	for i := argsIdx; i < len(ctx.Args); i += 2 {
		if h.Set(ctx.Args[i], ctx.Args[i+1]) {
			added++
		}
	}
	ctx.Client.SendInteger(int64(added))
}
