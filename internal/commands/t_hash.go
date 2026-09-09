// 哈希命令处理器 (t_hash.c equivalent)
package commands

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

func hsetCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	added := 0
	for i := 2; i < len(ctx.Args)-1; i += 2 {
		if h.Set(ctx.Args[i], ctx.Args[i+1]) {
			added++
		}
	}
	ctx.Client.SendInteger(int64(added))
}

func hgetCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	field := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	v, ok := val.Ptr.(*object.Hash).Get(field)
	if !ok {
		ctx.Client.SendNull()
		return
	}
	ctx.Client.SendBulkString(v)
}

func hmsetCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	for i := 2; i < len(ctx.Args)-1; i += 2 {
		h.Set(ctx.Args[i], ctx.Args[i+1])
	}
	ctx.Client.SendOK()
}

func hmgetCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	result := make([]resp.RESPValue, len(ctx.Args)-2)
	for i := 2; i < len(ctx.Args); i++ {
		if val == nil || val.Type != object.TypeHash {
			result[i-2] = resp.NullBulk
			continue
		}
		v, ok := val.Ptr.(*object.Hash).Get(ctx.Args[i])
		if !ok {
			result[i-2] = resp.NullBulk
		} else {
			result[i-2] = resp.BulkStringReply(v)
		}
	}
	ctx.Client.SendArray(result)
}

func hgetallCommand(ctx *CommandContext) {
	key := ctx.Args[1]
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
	result := make([]resp.RESPValue, 0, h.Len()*2)
	for _, field := range h.Fields() {
		v, _ := h.Get(field)
		result = append(result, resp.BulkStringReply(field), resp.BulkStringReply(v))
	}
	ctx.Client.SendArray(result)
}

func hdelCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	removed := 0
	for i := 2; i < len(ctx.Args); i++ {
		if h.Delete(ctx.Args[i]) {
			removed++
		}
	}
	ctx.Client.SendInteger(int64(removed))
}

func hlenCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(int64(val.Ptr.(*object.Hash).Len()))
}

func hexistsCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	field := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	_, ok := val.Ptr.(*object.Hash).Get(field)
	if ok {
		ctx.Client.SendInteger(1)
	} else {
		ctx.Client.SendInteger(0)
	}
}

func hincrbyCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	field := ctx.Args[2]
	delta, err := strconv.ParseInt(ctx.Args[3], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	n, err := val.Ptr.(*object.Hash).IncrementBy(field, delta)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.Client.SendInteger(n)
}

func hincrbyfloatCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	field := ctx.Args[2]
	delta, err := strconv.ParseFloat(ctx.Args[3], 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not a valid float")
		return
	}
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	f, err := val.Ptr.(*object.Hash).IncrementByFloat(field, delta)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.Client.SendBulkString(fmt.Sprintf("%g", f))
}

func hkeysCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	fields := val.Ptr.(*object.Hash).Fields()
	result := make([]resp.RESPValue, len(fields))
	for i, f := range fields {
		result[i] = resp.BulkStringReply(f)
	}
	ctx.Client.SendArray(result)
}

func hvalsCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	vals := val.Ptr.(*object.Hash).Values()
	result := make([]resp.RESPValue, len(vals))
	for i, v := range vals {
		result[i] = resp.BulkStringReply(v)
	}
	ctx.Client.SendArray(result)
}

func hsetnxCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	field := ctx.Args[2]
	value := ctx.Args[3]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	if val.Ptr.(*object.Hash).Set(field, value) {
		ctx.Client.SendInteger(1)
	} else {
		ctx.Client.SendInteger(0)
	}
}

func hstrlenCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	field := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	v, ok := val.Ptr.(*object.Hash).Get(field)
	if !ok {
		ctx.Client.SendInteger(0)
		return
	}
	ctx.Client.SendInteger(int64(len(v)))
}

func hscanCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	cursor, _ := strconv.ParseUint(ctx.Args[2], 10, 64)
	pattern := ""
	count := 10
	for i := 3; i < len(ctx.Args); i += 2 {
		opt := strings.ToUpper(ctx.Args[i])
		if i+1 >= len(ctx.Args) {
			break
		}
		switch opt {
		case "MATCH":
			pattern = ctx.Args[i+1]
		case "COUNT":
			count, _ = strconv.Atoi(ctx.Args[i+1])
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
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	fields := h.Fields()
	if int(cursor) >= len(fields) {
		cursor = 0
	}
	result := make([]resp.RESPValue, 0)
	skipped := 0
	for i := int(cursor); i < len(fields); i++ {
		if pattern != "" && pattern != "*" {
			if !matchPattern(pattern, fields[i]) {
				continue
			}
		}
		if skipped < 0 { // offset logic
			skipped++
			continue
		}
		v, _ := h.Get(fields[i])
		result = append(result, resp.BulkStringReply(fields[i]), resp.BulkStringReply(v))
		if len(result) >= count*2 {
			break
		}
	}
	newCursor := uint64(int(cursor) + count)
	if newCursor >= uint64(len(fields)) {
		newCursor = 0
	}
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply(strconv.FormatUint(newCursor, 10)),
		resp.ArrayReply(result),
	})
}

func hrandfieldCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	withValues := false
	if len(ctx.Args) > 2 {
		count, _ = strconv.Atoi(ctx.Args[2])
		if len(ctx.Args) > 3 && strings.ToLower(ctx.Args[3]) == "withvalues" {
			withValues = true
		}
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		if count == 1 {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendArray([]resp.RESPValue{})
		}
		return
	}
	if val.Type != object.TypeHash {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	h := val.Ptr.(*object.Hash)
	fields := h.Fields()
	if len(fields) == 0 {
		if count == 1 {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendArray([]resp.RESPValue{})
		}
		return
	}
	negative := count < 0
	if negative {
		count = -count
	}
	result := make([]resp.RESPValue, 0, count)
	if negative {
		// With replacement
		for i := 0; i < count; i++ {
			f := fields[rand.Intn(len(fields))]
			if withValues {
				v, _ := h.Get(f)
				result = append(result, resp.BulkStringReply(f), resp.BulkStringReply(v))
			} else {
				result = append(result, resp.BulkStringReply(f))
			}
		}
	} else {
		// Without replacement
		if count > len(fields) {
			count = len(fields)
		}
		shuffled := make([]string, len(fields))
		copy(shuffled, fields)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		for i := 0; i < count; i++ {
			if withValues {
				v, _ := h.Get(shuffled[i])
				result = append(result, resp.BulkStringReply(shuffled[i]), resp.BulkStringReply(v))
			} else {
				result = append(result, resp.BulkStringReply(shuffled[i]))
			}
		}
	}
	ctx.Client.SendArray(result)
}

// matchPattern is a simple glob pattern matcher (also used by SCAN).
func matchPattern(pattern, s string) bool {
	pi, si := 0, 0
	for pi < len(pattern) && si < len(s) {
		switch pattern[pi] {
		case '*':
			pi++
			if pi == len(pattern) {
				return true
			}
			for i := si; i <= len(s); i++ {
				if matchPattern(pattern[pi:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			pi++
			si++
		case '[':
			pi++
			if si >= len(s) {
				return false
			}
			negate := false
			if pi < len(pattern) && pattern[pi] == '^' {
				negate = true
				pi++
			}
			matched := false
			for pi < len(pattern) && pattern[pi] != ']' {
				if pi+2 < len(pattern) && pattern[pi+1] == '-' {
					if s[si] >= pattern[pi] && s[si] <= pattern[pi+2] {
						matched = true
					}
					pi += 3
				} else {
					if s[si] == pattern[pi] {
						matched = true
					}
					pi++
				}
			}
			if pi < len(pattern) {
				pi++ // skip ]
			}
			if negate {
				matched = !matched
			}
			if !matched {
				return false
			}
			si++
		case '\\':
			pi++
			if pi >= len(pattern) {
				return false
			}
			fallthrough
		default:
			if s[si] != pattern[pi] {
				return false
			}
			pi++
			si++
		}
	}
	return pi == len(pattern) && si == len(s)
}
