// 集合命令处理器 (t_set.c equivalent)
package commands

import (
	"strconv"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

func saddCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKeyOrCreate(key, object.TypeSet)
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	s := val.Ptr.(*object.Set)
	added := 0
	for i := 2; i < len(ctx.Args); i++ {
		if s.Add(ctx.Args[i]) {
			added++
		}
	}
	ctx.Client.SendInteger(int64(added))
}

func sremCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	s := val.Ptr.(*object.Set)
	removed := 0
	for i := 2; i < len(ctx.Args); i++ {
		if s.Remove(ctx.Args[i]) {
			removed++
		}
	}
	ctx.Client.SendInteger(int64(removed))
}

func smembersCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	members := val.Ptr.(*object.Set).Members()
	result := make([]resp.RESPValue, len(members))
	for i, m := range members {
		result[i] = resp.BulkStringReply(m)
	}
	ctx.Client.SendArray(result)
}

func sismemberCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	member := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	if val.Ptr.(*object.Set).Contains(member) {
		ctx.Client.SendInteger(1)
	} else {
		ctx.Client.SendInteger(0)
	}
}

func scardCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(int64(val.Ptr.(*object.Set).Len()))
}

func spopCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		c, err := strconv.Atoi(ctx.Args[2])
		if err != nil {
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
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	s := val.Ptr.(*object.Set)
	if count == 1 {
		popped := s.Pop(1)
		if len(popped) == 0 {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendBulkString(popped[0])
		}
	} else {
		popped := s.Pop(count)
		result := make([]resp.RESPValue, len(popped))
		for i, p := range popped {
			result[i] = resp.BulkStringReply(p)
		}
		ctx.Client.SendArray(result)
	}
}

func srandmemberCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	count := 1
	if len(ctx.Args) > 2 {
		c, err := strconv.Atoi(ctx.Args[2])
		if err != nil {
			ctx.Client.SendError("ERR value is not an integer or out of range")
			return
		}
		count = c
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
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	members := val.Ptr.(*object.Set).RandomMembers(count)
	if len(ctx.Args) > 2 {
		result := make([]resp.RESPValue, len(members))
		for i, m := range members {
			result[i] = resp.BulkStringReply(m)
		}
		ctx.Client.SendArray(result)
	} else {
		if len(members) == 0 {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendBulkString(members[0])
		}
	}
}

func sunionCommand(ctx *CommandContext) {
	union := make(map[string]bool)
	for i := 1; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val != nil && val.Type == object.TypeSet {
			for _, m := range val.Ptr.(*object.Set).Members() {
				union[m] = true
			}
		}
	}
	result := make([]resp.RESPValue, 0, len(union))
	for m := range union {
		result = append(result, resp.BulkStringReply(m))
	}
	ctx.Client.SendArray(result)
}

func sunionstoreCommand(ctx *CommandContext) {
	dest := ctx.Args[1]
	union := make(map[string]bool)
	for i := 2; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val != nil && val.Type == object.TypeSet {
			for _, m := range val.Ptr.(*object.Set).Members() {
				union[m] = true
			}
		}
	}
	destVal := object.NewSetObject()
	destS := destVal.Ptr.(*object.Set)
	for m := range union {
		destS.Add(m)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendInteger(int64(len(union)))
}

func sinterCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	val := ctx.DB.LookupKey(ctx.Args[1])
	if val == nil || val.Type != object.TypeSet {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	inter := make(map[string]bool)
	for _, m := range val.Ptr.(*object.Set).Members() {
		inter[m] = true
	}
	for i := 2; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val == nil || val.Type != object.TypeSet {
			ctx.Client.SendArray([]resp.RESPValue{})
			return
		}
		other := val.Ptr.(*object.Set)
		for m := range inter {
			if !other.Contains(m) {
				delete(inter, m)
			}
		}
	}
	result := make([]resp.RESPValue, 0, len(inter))
	for m := range inter {
		result = append(result, resp.BulkStringReply(m))
	}
	ctx.Client.SendArray(result)
}

func sinterstoreCommand(ctx *CommandContext) {
	dest := ctx.Args[1]
	sinterCommand(&CommandContext{Client: ctx.Client, DB: ctx.DB, Args: ctx.Args[1:]})
	// Simplified: just use sinter and store
	if len(ctx.Args) < 3 {
		ctx.DB.SetKey(dest, object.NewSetObject())
		ctx.Client.SendInteger(0)
		return
	}
	val := ctx.DB.LookupKey(ctx.Args[2])
	if val == nil || val.Type != object.TypeSet {
		ctx.DB.SetKey(dest, object.NewSetObject())
		ctx.Client.SendInteger(0)
		return
	}
	inter := make(map[string]bool)
	for _, m := range val.Ptr.(*object.Set).Members() {
		inter[m] = true
	}
	for i := 3; i < len(ctx.Args); i++ {
		v := ctx.DB.LookupKey(ctx.Args[i])
		if v == nil || v.Type != object.TypeSet {
			inter = make(map[string]bool)
			break
		}
		other := v.Ptr.(*object.Set)
		for m := range inter {
			if !other.Contains(m) {
				delete(inter, m)
			}
		}
	}
	destVal := object.NewSetObject()
	destS := destVal.Ptr.(*object.Set)
	for m := range inter {
		destS.Add(m)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendInteger(int64(len(inter)))
}

func sdiffCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	val := ctx.DB.LookupKey(ctx.Args[1])
	if val == nil || val.Type != object.TypeSet {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	diff := make(map[string]bool)
	for _, m := range val.Ptr.(*object.Set).Members() {
		diff[m] = true
	}
	for i := 2; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val != nil && val.Type == object.TypeSet {
			for _, m := range val.Ptr.(*object.Set).Members() {
				delete(diff, m)
			}
		}
	}
	result := make([]resp.RESPValue, 0, len(diff))
	for m := range diff {
		result = append(result, resp.BulkStringReply(m))
	}
	ctx.Client.SendArray(result)
}

func sdiffstoreCommand(ctx *CommandContext) {
	dest := ctx.Args[1]
	sdiffCommand(&CommandContext{Client: ctx.Client, DB: ctx.DB, Args: ctx.Args[1:]})
	if len(ctx.Args) < 3 {
		ctx.DB.SetKey(dest, object.NewSetObject())
		ctx.Client.SendInteger(0)
		return
	}
	val := ctx.DB.LookupKey(ctx.Args[2])
	if val == nil || val.Type != object.TypeSet {
		ctx.DB.SetKey(dest, object.NewSetObject())
		ctx.Client.SendInteger(0)
		return
	}
	diff := make(map[string]bool)
	for _, m := range val.Ptr.(*object.Set).Members() {
		diff[m] = true
	}
	for i := 3; i < len(ctx.Args); i++ {
		v := ctx.DB.LookupKey(ctx.Args[i])
		if v != nil && v.Type == object.TypeSet {
			for _, m := range v.Ptr.(*object.Set).Members() {
				delete(diff, m)
			}
		}
	}
	destVal := object.NewSetObject()
	destS := destVal.Ptr.(*object.Set)
	for m := range diff {
		destS.Add(m)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendInteger(int64(len(diff)))
}

func smoveCommand(ctx *CommandContext) {
	source := ctx.Args[1]
	dest := ctx.Args[2]
	member := ctx.Args[3]
	val := ctx.DB.LookupKey(source)
	if val == nil || val.Type != object.TypeSet {
		ctx.Client.SendInteger(0)
		return
	}
	if !val.Ptr.(*object.Set).Remove(member) {
		ctx.Client.SendInteger(0)
		return
	}
	destVal := ctx.DB.LookupKeyOrCreate(dest, object.TypeSet)
	if destVal.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	destVal.Ptr.(*object.Set).Add(member)
	ctx.Client.SendInteger(1)
}

func sscanCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	cursor, _ := strconv.ParseUint(ctx.Args[2], 10, 64)
	pattern := ""
	count := 10
	for i := 3; i < len(ctx.Args); i += 2 {
		opt := ctx.Args[i]
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
	if val.Type != object.TypeSet {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	members := val.Ptr.(*object.Set).Members()
	if int(cursor) >= len(members) {
		cursor = 0
	}
	result := make([]resp.RESPValue, 0)
	for i := int(cursor); i < len(members); i++ {
		if pattern != "" && pattern != "*" && !matchPattern(pattern, members[i]) {
			continue
		}
		result = append(result, resp.BulkStringReply(members[i]))
		if len(result) >= count {
			break
		}
	}
	newCursor := uint64(int(cursor) + count)
	if newCursor >= uint64(len(members)) {
		newCursor = 0
	}
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply(strconv.FormatUint(newCursor, 10)),
		resp.ArrayReply(result),
	})
}
