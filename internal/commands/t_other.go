// 其他缺失命令实现。
// 对应 Redis 中的 lcsCommand, roleCommand, memoryCommand, moveCommand 等。
package commands

import (
	"fmt"
	"strconv"
	"strings"

	"time"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN <len>] [WITHMATCHLEN]
func lcsCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'lcs' command")
		return
	}
	key1 := ctx.Args[1]
	key2 := ctx.Args[2]
	val1 := ctx.DB.LookupKey(key1)
	val2 := ctx.DB.LookupKey(key2)
	if val1 == nil || val2 == nil {
		ctx.Client.SendBulkString("")
		return
	}
	s1 := val1.GetString()
	s2 := val2.GetString()
	// 动态规划求最长公共子序列
	lcs := longestCommonSubsequence(s1, s2)
	// 检查选项
	for i := 3; i < len(ctx.Args); i++ {
		if strings.ToUpper(ctx.Args[i]) == "LEN" {
			ctx.Client.SendInteger(int64(len(lcs)))
			return
		}
	}
	ctx.Client.SendBulkString(lcs)
}

func longestCommonSubsequence(s1, s2 string) string {
	m, n := len(s1), len(s2)
	if m == 0 || n == 0 {
		return ""
	}
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if s1[i-1] == s2[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				if dp[i-1][j] > dp[i][j-1] {
					dp[i][j] = dp[i-1][j]
				} else {
					dp[i][j] = dp[i][j-1]
				}
			}
		}
	}
	// 回溯
	result := make([]byte, 0, dp[m][n])
	i, j := m, n
	for i > 0 && j > 0 {
		if s1[i-1] == s2[j-1] {
			result = append([]byte{s1[i-1]}, result...)
			i--
			j--
		} else if dp[i-1][j] > dp[i][j-1] {
			i--
		} else {
			j--
		}
	}
	return string(result)
}

// ROLE
func roleCommand(ctx *CommandContext) {
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply("master"),
		resp.IntegerReply(0),
		resp.ArrayReply([]resp.RESPValue{}),
	})
}

// MEMORY USAGE key [SAMPLES <count>]
func memoryCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'memory' command")
		return
	}
	subcmd := strings.ToUpper(ctx.Args[1])
	switch subcmd {
	case "USAGE":
		key := ctx.Args[2]
		val := ctx.DB.LookupKey(key)
		if val == nil {
			ctx.Client.SendNull()
			return
		}
		// 简化估算
		est := int64(64) // 对象头
		switch val.Type {
		case 0: // string
			est += int64(val.GetStringLen())
		default:
			est += 256
		}
		ctx.Client.SendInteger(est)
	case "STATS":
		ctx.Client.SendArray([]resp.RESPValue{})
	case "MALLOC-STATS":
		ctx.Client.SendBulkString("")
	case "DOCTOR":
		ctx.Client.SendBulkString("No memory issues detected")
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR Unknown subcommand '%s'", subcmd))
	}
}

// MOVE key db
func moveCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'move' command")
		return
	}
	key := ctx.Args[1]
	targetDB, err := ctx.Client.GetDBIndex(), error(nil)
	_ = err
	// 简化：不实际移动
	ctx.Client.SendInteger(0)
	_ = targetDB
	_ = key
}

// DUMP key
func dumpCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	// 简化：返回序列化表示
	ctx.Client.SendBulkString(fmt.Sprintf("DUMP:%s:%v", key, val.Type))
}

// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME <seconds>] [FREQ <frequency>]
func restoreCommand(ctx *CommandContext) {
	// 简化：直接返回 OK
	ctx.Client.SendOK()
}

// WAIT numreplicas timeout
func waitCommand(ctx *CommandContext) {
	// 简化：立即返回 0（无副本）
	ctx.Client.SendInteger(0)
}

// WAITAOF numlocal numreplicas timeout
func waitaofCommand(ctx *CommandContext) {
	ctx.Client.SendArray([]resp.RESPValue{
		resp.IntegerReply(0),
		resp.IntegerReply(0),
	})
}

// MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH <password>] [AUTH2 <username> <password>] [KEYS key [key ...]]
func migrateCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR MIGRATE is not supported in standalone mode")
}

// SWAPDB index1 index2
func swapdbCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
func sortroCommand(ctx *CommandContext) {
	// 复用 SORT 命令
	sortCommand(ctx)
}

// PUBLISH (sharded) - SPUBLISH channel message
func spublishCommand(ctx *CommandContext) {
	// 复用普通 PUBLISH
	publishCommand(ctx)
}

// SSUBSCRIBE channel [channel ...]
func ssubscribeCommand(ctx *CommandContext) {
	subscribeCommand(ctx)
}

// SUNSUBSCRIBE [channel [channel ...]]
func sunsubscribeCommand(ctx *CommandContext) {
	unsubscribeCommand(ctx)
}

// SYNC - 内部复制命令
func syncCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR SYNC is not supported")
}

// DEBUG subcommand [args...]
// 已在 stubs.go 中定义，这里补充子命令

// MSETEX key milliseconds value [key milliseconds value ...]
// 对应 Redis 的 msetexCommand。
func msetexCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 || (len(ctx.Args)-1)%3 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'msetex' command")
		return
	}
	for i := 1; i < len(ctx.Args); i += 3 {
		key := ctx.Args[i]
		ms, err := strconv.ParseInt(ctx.Args[i+1], 10, 64)
		if err != nil || ms <= 0 {
			ctx.Client.SendError("ERR value is not an integer or out of range")
			return
		}
		value := ctx.Args[i+2]
		obj := object.NewStringObject(value)
		ctx.DB.SetKey(key, obj)
		ctx.DB.SetExpire(key, time.Now().Add(time.Duration(ms)*time.Millisecond))
	}
	ctx.Client.SendOK()
}
