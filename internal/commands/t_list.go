// 列表命令处理器 (t_list.c equivalent)
package commands

import (
	"strconv"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

func lpushCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeList)
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	for i := 2; i < len(ctx.Args); i++ {
		ql.PushLeft(ctx.Args[i])
	}
	ctx.Client.SendInteger(int64(ql.Len()))
}

func rpushCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeList)
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	for i := 2; i < len(ctx.Args); i++ {
		ql.PushRight(ctx.Args[i])
	}
	ctx.Client.SendInteger(int64(ql.Len()))
}

func lpopCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		c, err := strconv.Atoi(ctx.Args[2])
		if err != nil || c < 0 {
			ctx.Client.SendError("ERR value is not an integer or out of range")
			return
		}
		count = c
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	if count == 1 {
		v, ok := ql.PopLeft()
		if !ok {
			ctx.Client.SendNull()
			return
		}
		ctx.Client.SendBulkString(v.(string))
	} else {
		result := make([]resp.RESPValue, 0, count)
		for i := 0; i < count; i++ {
			v, ok := ql.PopLeft()
			if !ok {
				break
			}
			result = append(result, resp.BulkStringReply(v.(string)))
		}
		if len(result) == 0 {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendArray(result)
		}
	}
}

func rpopCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		c, err := strconv.Atoi(ctx.Args[2])
		if err != nil || c < 0 {
			ctx.Client.SendError("ERR value is not an integer or out of range")
			return
		}
		count = c
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	if count == 1 {
		v, ok := ql.PopRight()
		if !ok {
			ctx.Client.SendNull()
			return
		}
		ctx.Client.SendBulkString(v.(string))
	} else {
		result := make([]resp.RESPValue, 0, count)
		for i := 0; i < count; i++ {
			v, ok := ql.PopRight()
			if !ok {
				break
			}
			result = append(result, resp.BulkStringReply(v.(string)))
		}
		if len(result) == 0 {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendArray(result)
		}
	}
}

func lrangeCommand(ctx *CommandContext) {
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
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	items := ql.Range(start, stop)
	result := make([]resp.RESPValue, len(items))
	for i, v := range items {
		result[i] = resp.BulkStringReply(v.(string))
	}
	ctx.Client.SendArray(result)
}

func llenCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(int64(val.Ptr.(*object.QuickList).Len()))
}

func lindexCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	index, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	v, ok := val.Ptr.(*object.QuickList).Index(index)
	if !ok {
		ctx.Client.SendNull()
		return
	}
	ctx.Client.SendBulkString(v.(string))
}

func lsetCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	index, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	value := ctx.Args[3]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendError("ERR no such key")
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	if !val.Ptr.(*object.QuickList).Set(index, value) {
		ctx.Client.SendError("ERR index out of range")
		return
	}
	ctx.Client.SendOK()
}

func lremCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	value := ctx.Args[3]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	removed := ql.RemoveValue(value, count)
	ctx.Client.SendInteger(int64(removed))
}

func ltrimCommand(ctx *CommandContext) {
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
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendOK()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	if start < 0 {
		start = ql.Len() + start
	}
	if stop < 0 {
		stop = ql.Len() + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= ql.Len() {
		stop = ql.Len() - 1
	}
	if start > stop {
		ql.Clear()
	} else {
		keep := stop - start + 1
		ql.Trim(start, keep)
	}
	ctx.Client.SendOK()
}

func lposCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	value := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	for i := 0; i < ql.Len(); i++ {
		v, _ := ql.Index(i)
		if v.(string) == value {
			ctx.Client.SendInteger(int64(i))
			return
		}
	}
	ctx.Client.SendNull()
}

func linsertCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	where := ctx.Args[2]
	pivot := ctx.Args[3]
	value := ctx.Args[4]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	for i := 0; i < ql.Len(); i++ {
		v, _ := ql.Index(i)
		if v.(string) == pivot {
			if where == "before" {
				ql.Insert(i, value)
			} else {
				ql.Insert(i+1, value)
			}
			ctx.Client.SendInteger(int64(ql.Len()))
			return
		}
	}
	ctx.Client.SendInteger(-1)
}

func lpushxCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	for i := 2; i < len(ctx.Args); i++ {
		ql.PushLeft(ctx.Args[i])
	}
	ctx.Client.SendInteger(int64(ql.Len()))
}

func rpushxCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ql := val.Ptr.(*object.QuickList)
	for i := 2; i < len(ctx.Args); i++ {
		ql.PushRight(ctx.Args[i])
	}
	ctx.Client.SendInteger(int64(ql.Len()))
}

func blpopCommand(ctx *CommandContext) {
	// Simplified: non-blocking version
	for i := 1; i < len(ctx.Args)-1; i++ {
		key := ctx.Args[i]
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeList {
			ql := val.Ptr.(*object.QuickList)
			v, ok := ql.PopLeft()
			if ok {
				ctx.Client.SendArray([]resp.RESPValue{
					resp.BulkStringReply(key),
					resp.BulkStringReply(v.(string)),
				})
				return
			}
		}
	}
	ctx.Client.SendNull()
}

func brpopCommand(ctx *CommandContext) {
	// Simplified: non-blocking version
	for i := 1; i < len(ctx.Args)-1; i++ {
		key := ctx.Args[i]
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeList {
			ql := val.Ptr.(*object.QuickList)
			v, ok := ql.PopRight()
			if ok {
				ctx.Client.SendArray([]resp.RESPValue{
					resp.BulkStringReply(key),
					resp.BulkStringReply(v.(string)),
				})
				return
			}
		}
	}
	ctx.Client.SendNull()
}

func rpoplpushCommand(ctx *CommandContext) {
	source := ctx.Args[1]
	dest := ctx.Args[2]
	val := ctx.DB.LookupKey(source)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	v, ok := val.Ptr.(*object.QuickList).PopRight()
	if !ok {
		ctx.Client.SendNull()
		return
	}
	destVal := ctx.DB.LookupKeyOrCreate(dest, object.TypeList)
	if destVal.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	destVal.Ptr.(*object.QuickList).PushLeft(v)
	ctx.Client.SendBulkString(v.(string))
}

func lmoveCommand(ctx *CommandContext) {
	source := ctx.Args[1]
	dest := ctx.Args[2]
	whereFrom := ctx.Args[3]
	whereTo := ctx.Args[4]
	val := ctx.DB.LookupKey(source)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	var v interface{}
	var ok bool
	if whereFrom == "left" {
		v, ok = val.Ptr.(*object.QuickList).PopLeft()
	} else {
		v, ok = val.Ptr.(*object.QuickList).PopRight()
	}
	if !ok {
		ctx.Client.SendNull()
		return
	}
	destVal := ctx.DB.LookupKeyOrCreate(dest, object.TypeList)
	if destVal.Type != object.TypeList {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	if whereTo == "left" {
		destVal.Ptr.(*object.QuickList).PushLeft(v)
	} else {
		destVal.Ptr.(*object.QuickList).PushRight(v)
	}
	ctx.Client.SendBulkString(v.(string))
}

func blmoveCommand(ctx *CommandContext) {
	// Simplified non-blocking version
	lmoveCommand(ctx)
}

func lmpopCommand(ctx *CommandContext) {
	// Simplified
	count := 1
	for i := 1; i < len(ctx.Args); i++ {
		if ctx.Args[i] == "count" && i+1 < len(ctx.Args) {
			count, _ = strconv.Atoi(ctx.Args[i+1])
			i++
		}
	}
	for i := 1; i < len(ctx.Args); i++ {
		key := ctx.Args[i]
		val := ctx.DB.LookupKey(key)
		if val != nil && val.Type == object.TypeList {
			ql := val.Ptr.(*object.QuickList)
			if ql.Len() > 0 {
				result := make([]resp.RESPValue, 0, count)
				for j := 0; j < count && ql.Len() > 0; j++ {
					v, _ := ql.PopLeft()
					result = append(result, resp.BulkStringReply(v.(string)))
				}
				ctx.Client.SendArray([]resp.RESPValue{
					resp.BulkStringReply(key),
					resp.ArrayReply(result),
				})
				return
			}
		}
	}
	ctx.Client.SendNull()
}

func blmpopCommand(ctx *CommandContext) {
	// Simplified non-blocking version
	lmpopCommand(ctx)
}
