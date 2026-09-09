// HyperLogLog 命令处理器
package commands

import (
	"github.com/km-dev/km-go-redis/internal/object"
)

func pfaddCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		val = object.NewSetObject()
		ctx.DB.SetKey(key, val)
	}
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
	if added > 0 {
		ctx.Client.SendInteger(1)
	} else {
		ctx.Client.SendInteger(0)
	}
}

func pfcountCommand(ctx *CommandContext) {
	total := 0
	for i := 1; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val != nil && val.Type == object.TypeSet {
			total += val.Ptr.(*object.Set).Len()
		}
	}
	ctx.Client.SendInteger(int64(total))
}

func pfmergeCommand(ctx *CommandContext) {
	dest := ctx.Args[1]
	union := make(map[string]bool)
	for i := 1; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val != nil && val.Type == object.TypeSet {
			for _, m := range val.Ptr.(*object.Set).Members() {
				union[m] = true
			}
		}
	}
	destVal := object.NewSetObject()
	for m := range union {
		destVal.Ptr.(*object.Set).Add(m)
	}
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, destVal)
	ctx.Client.SendOK()
}
