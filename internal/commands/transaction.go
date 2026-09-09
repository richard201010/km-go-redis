// 事务命令处理器
package commands

import (
	"github.com/km-dev/km-go-redis/internal/resp"
)

func multiCommand(ctx *CommandContext) {
	// Simplified: just acknowledge
	ctx.Client.SendOK()
}

func execCommand(ctx *CommandContext) {
	ctx.Client.SendArray([]resp.RESPValue{})
}

func discardCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

func watchCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

func unwatchCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}
