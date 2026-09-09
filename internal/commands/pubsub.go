// 发布订阅命令处理器 (pubsub.c equivalent)
package commands

import (
	"github.com/km-dev/km-go-redis/internal/resp"
)

func subscribeCommand(ctx *CommandContext) {
	for i := 1; i < len(ctx.Args); i++ {
		channel := ctx.Args[i]
		ctx.Client.GetSubscriptions()[channel] = true
		ctx.Server.Subscribe(ctx.Client, channel)
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("subscribe"),
			resp.BulkStringReply(channel),
			resp.IntegerReply(int64(len(ctx.Client.GetSubscriptions()))),
		})
	}
}

func unsubscribeCommand(ctx *CommandContext) {
	if len(ctx.Args) == 1 {
		// Unsubscribe all
		for ch := range ctx.Client.GetSubscriptions() {
			ctx.Server.Unsubscribe(ctx.Client, ch)
		}
		ctx.Client.GetSubscriptions() // clear
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("unsubscribe"),
			resp.NullBulk,
			resp.IntegerReply(0),
		})
		return
	}
	for i := 1; i < len(ctx.Args); i++ {
		channel := ctx.Args[i]
		delete(ctx.Client.GetSubscriptions(), channel)
		ctx.Server.Unsubscribe(ctx.Client, channel)
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("unsubscribe"),
			resp.BulkStringReply(channel),
			resp.IntegerReply(int64(len(ctx.Client.GetSubscriptions()))),
		})
	}
}

func psubscribeCommand(ctx *CommandContext) {
	for i := 1; i < len(ctx.Args); i++ {
		pattern := ctx.Args[i]
		ctx.Client.GetPatternSubs()[pattern] = true
		ctx.Server.PSubscribe(ctx.Client, pattern)
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("psubscribe"),
			resp.BulkStringReply(pattern),
			resp.IntegerReply(int64(len(ctx.Client.GetPatternSubs()))),
		})
	}
}

func punsubscribeCommand(ctx *CommandContext) {
	if len(ctx.Args) == 1 {
		for p := range ctx.Client.GetPatternSubs() {
			ctx.Server.PUnsubscribe(ctx.Client, p)
		}
		// clear
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("punsubscribe"),
			resp.NullBulk,
			resp.IntegerReply(0),
		})
		return
	}
	for i := 1; i < len(ctx.Args); i++ {
		pattern := ctx.Args[i]
		delete(ctx.Client.GetPatternSubs(), pattern)
		ctx.Server.PUnsubscribe(ctx.Client, pattern)
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("punsubscribe"),
			resp.BulkStringReply(pattern),
			resp.IntegerReply(int64(len(ctx.Client.GetPatternSubs()))),
		})
	}
}

func publishCommand(ctx *CommandContext) {
	channel := ctx.Args[1]
	message := ctx.Args[2]
	receivers := ctx.Server.BroadcastPubSub(channel, message)
	ctx.Client.SendInteger(int64(receivers))
}

func pubsubCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'pubsub' command")
		return
	}
	subcmd := ctx.Args[1]
	switch subcmd {
	case "CHANNELS":
		ctx.Client.SendArray([]resp.RESPValue{})
	case "NUMSUB":
		ctx.Client.SendArray([]resp.RESPValue{})
	case "NUMPAT":
		ctx.Client.SendInteger(0)
	default:
		ctx.Client.SendError("ERR unknown subcommand")
	}
}
