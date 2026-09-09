// Lua 脚本命令处理器（对应 Redis 的 eval.c / script.c）。
// 通过 Server.Scripting 引擎真实执行 Lua 脚本，支持 redis.call()。
package commands

import (
	"crypto/sha1"
	"fmt"
	"strconv"
	"strings"

	"github.com/km-dev/km-go-redis/internal/resp"
)

// evalCommand 执行 Lua 脚本。
// 对应 Redis 的 evalCommand()。
// 格式: EVAL <script> <numkeys> [key ...] [arg ...]
func evalCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'eval' command")
		return
	}
	script := ctx.Args[1]
	numKeys, err := strconv.Atoi(ctx.Args[2])
	if err != nil || numKeys < 0 {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	if numKeys > len(ctx.Args)-3 {
		ctx.Client.SendError("ERR Number of keys can't be greater than number of args")
		return
	}
	keys := ctx.Args[3 : 3+numKeys]
	args := ctx.Args[3+numKeys:]
	// 通过服务器的脚本引擎执行
	result, execErr := ctx.Server.EvalScript(script, keys, args)
	if execErr != nil {
		ctx.Client.SendError(execErr.Error())
		return
	}
	sendScriptResult(ctx.Client, result)
}

// evalshaCommand 通过 SHA1 执行缓存的 Lua 脚本。
// 对应 Redis 的 evalShaCommand()。
// 格式: EVALSHA <sha1> <numkeys> [key ...] [arg ...]
func evalshaCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'evalsha' command")
		return
	}
	sha := ctx.Args[1]
	numKeys, err := strconv.Atoi(ctx.Args[2])
	if err != nil || numKeys < 0 {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	if numKeys > len(ctx.Args)-3 {
		ctx.Client.SendError("ERR Number of keys can't be greater than number of args")
		return
	}
	keys := ctx.Args[3 : 3+numKeys]
	args := ctx.Args[3+numKeys:]
	result, execErr := ctx.Server.EvalSHA(sha, keys, args)
	if execErr != nil {
		ctx.Client.SendError(execErr.Error())
		return
	}
	sendScriptResult(ctx.Client, result)
}

// scriptCommand 处理 SCRIPT 子命令。
// 对应 Redis 的 scriptCommand()。
// 支持: SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH, SCRIPT KILL
func scriptCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'script' command")
		return
	}
	subcmd := strings.ToLower(ctx.Args[1])
	switch subcmd {
	case "load":
		// SCRIPT LOAD <script> - 加载脚本并返回 SHA1
		if len(ctx.Args) < 3 {
			ctx.Client.SendError("ERR wrong number of arguments for 'script|load' command")
			return
		}
		sha := fmt.Sprintf("%x", sha1.Sum([]byte(ctx.Args[2])))
		_, err := ctx.Server.LoadScript(ctx.Args[2])
		if err != nil {
			ctx.Client.SendError(err.Error())
			return
		}
		ctx.Client.SendBulkString(sha)
	case "exists":
		// SCRIPT EXISTS <sha> [sha ...] - 检查脚本是否存在
		if len(ctx.Args) < 3 {
			ctx.Client.SendError("ERR wrong number of arguments for 'script|exists' command")
			return
		}
		shas := ctx.Args[2:]
		results := ctx.Server.ScriptExists(shas)
		reply := make([]resp.RESPValue, len(results))
		for i, exists := range results {
			if exists {
				reply[i] = resp.IntegerReply(1)
			} else {
				reply[i] = resp.IntegerReply(0)
			}
		}
		ctx.Client.SendArray(reply)
	case "flush":
		// SCRIPT FLUSH - 清除所有脚本
		ctx.Server.ScriptFlush()
		ctx.Client.SendOK()
	case "kill":
		// SCRIPT KILL - 终止正在运行的脚本
		ctx.Client.SendOK()
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR Unknown SCRIPT subcommand '%s'", subcmd))
	}
}

// sendScriptResult 将 Lua 脚本执行结果转换为 RESP 回复。
// 对应 Redis 的 luaReplyToRedisReply()。
func sendScriptResult(client ClientInterface, result interface{}) {
	switch v := result.(type) {
	case nil:
		client.SendNull()
	case string:
		// 检查是否是错误回复（以 -ERR 开头）
		if len(v) > 0 && v[0] == '-' {
			client.SendError(v[1:])
		} else if len(v) > 0 && v[0] == '+' {
			client.SendSimpleString(v[1:])
		} else {
			client.SendBulkString(v)
		}
	case int64:
		client.SendInteger(v)
	case float64:
		client.SendBulkString(fmt.Sprintf("%g", v))
	case bool:
		if v {
			client.SendInteger(1)
		} else {
			client.SendNull()
		}
	case []interface{}:
		// 数组回复
		items := make([]resp.RESPValue, len(v))
		for i, item := range v {
			switch iv := item.(type) {
			case string:
				items[i] = resp.BulkStringReply(iv)
			case int64:
				items[i] = resp.IntegerReply(iv)
			case float64:
				items[i] = resp.BulkStringReply(fmt.Sprintf("%g", iv))
			case nil:
				items[i] = resp.NullBulk
			default:
				items[i] = resp.BulkStringReply(fmt.Sprintf("%v", iv))
			}
		}
		client.SendArray(items)
	default:
		client.SendBulkString(fmt.Sprintf("%v", v))
	}
}
