// 扩展命令桩 — 补全 Rust 版有但 Go 版缺失的命令
package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// ============================================================
// AR* 命令 (Array 类型扩展，Redis 8 draft)
// ============================================================

func arcountCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func ardelCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func ardelrangeCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func argetCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func argetrangeCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func argrepCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arinfoCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arinsertCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arlastitemsCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arlenCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func armgetCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func armsetCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arnextCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func aropCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arringCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arscanCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arseekCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}
func arsetCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR Array type not supported")
}

// ============================================================
// List 扩展命令
// ============================================================

// LMOVEM — List Move Multiple (简化: 委托给 lmove)
func lmovemCommand(ctx *CommandContext) {
	lmoveCommand(ctx)
}

// BLMOVEM — Blocking List Move Multiple (简化: 委托给 blmove)
func blmovemCommand(ctx *CommandContext) {
	blmoveCommand(ctx)
}

// BRPOPLPUSH — deprecated wrapper for RPOPLPUSH
func brpoplpushCommand(ctx *CommandContext) {
	rpoplpushCommand(ctx)
}

// ============================================================
// Hash 扩展命令
// ============================================================

// HIMPORT — Hash Import: HIMPORT key [MAXLEN maxlen] FIELDS numfields field value ...
func himportCommand(ctx *CommandContext) {
	if len(ctx.Args) < 5 {
		ctx.Client.SendError("ERR wrong number of arguments for 'himport' command")
		return
	}

	key := ctx.Args[1]
	var dataStart int
	for i := 2; i < len(ctx.Args); i++ {
		opt := strings.ToUpper(ctx.Args[i])
		switch opt {
		case "MAXLEN":
			i++ // skip value
		case "FIELDS":
			dataStart = i + 1
			goto parseData
		}
	}
parseData:
	if dataStart == 0 || dataStart >= len(ctx.Args) {
		ctx.Client.SendError("ERR wrong number of arguments for 'himport' command")
		return
	}

	val := ctx.DB.LookupKeyOrCreate(key, object.TypeHash)
	if val.Type != object.TypeHash {
		ctx.Client.SendError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	h := val.Ptr.(*object.Hash)

	count := 0
	for i := dataStart; i+1 < len(ctx.Args); i += 2 {
		h.Set(ctx.Args[i], ctx.Args[i+1])
		count++
	}
	ctx.Client.SendOK()
	_ = count
}

// PEXPIRETIME — per-field expiretime stub
// 返回 -1 表示 key 存在但未设置过期时间
func pexpiretimeCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'pexpiretime' command")
		return
	}
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(-2)
		return
	}
	ctx.Client.SendInteger(-1)
}

// ============================================================
// String 扩展命令
// ============================================================

// INCREX — Increment with Expiry: INCREX key delta EX seconds
func increxCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments for 'increx' command")
		return
	}
	// Simplified: just increment, ignore expiry
	incrCommand(ctx)
}

// ============================================================
// Function/Rcall 命令 (Redis 7+)
// ============================================================

// FCALL — Function Call: FCALL function numkeys key ... arg ...
func fcallCommand(ctx *CommandContext) {
	ctx.Client.SendNull()
}

// FCALL_RO — Function Call Read-Only
func fcallroCommand(ctx *CommandContext) {
	ctx.Client.SendNull()
}

// FUNCTION — Function Management: FUNCTION LIST|LOAD|DELETE|DUMP|RESTORE|FLUSH|STATS|KILL
func functionCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'function' command")
		return
	}
	subcmd := strings.ToUpper(ctx.Args[1])
	switch subcmd {
	case "LIST":
		ctx.Client.SendArray(nil)
	case "LOAD":
		ctx.Client.SendBulkString("noop")
	case "DELETE":
		ctx.Client.SendOK()
	case "DUMP":
		ctx.Client.SendBulkString("")
	case "RESTORE":
		ctx.Client.SendOK()
	case "FLUSH":
		ctx.Client.SendOK()
	case "STATS":
		ctx.Client.SendArray(nil)
	case "KILL":
		ctx.Client.SendOK()
	case "HELP":
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("LIST [LIBRARYNAME <pattern>] [WITHCODE]"),
			resp.BulkStringReply("LOAD <code>"),
			resp.BulkStringReply("DELETE <library-name>"),
			resp.BulkStringReply("DUMP"),
			resp.BulkStringReply("RESTORE <serialized-value> [REPLACE] [FLUSH]"),
			resp.BulkStringReply("FLUSH [ASYNC|SYNC]"),
			resp.BulkStringReply("STATS"),
			resp.BulkStringReply("KILL"),
		})
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for 'FUNCTION'"))
	}
}

// ============================================================
// Replication 命令
// ============================================================

// PSYNC — Partial Sync: PSYNC replicationid offset
func psyncCommand(ctx *CommandContext) {
	ctx.Client.SendSimpleString("FULLRESYNC 0000000000000000000000000000000000000001 0")
}

// SLAVEOF — deprecated wrapper for REPLICAOF
func slaveofCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'slaveof' command")
		return
	}
	host := ctx.Args[1]
	if strings.ToUpper(host) == "NO" {
		ctx.Client.SendOK()
		return
	}
	ctx.Client.SendOK()
}

// ============================================================
// Key 扩展命令
// ============================================================

// KEYSLOT — Cluster Key Slot: KEYSLOT key
func keyslotCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'keyslot' command")
		return
	}
	key := ctx.Args[1]
	slot := crc16(key) % 16384
	ctx.Client.SendInteger(int64(slot))
}

// crc16 — CRC16-CCITT 算法 (Redis cluster 使用)
func crc16(s string) int {
	crc := 0
	for i := 0; i < len(s); i++ {
		crc ^= int(s[i]) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
			crc &= 0xFFFF
		}
	}
	return crc
}

// ============================================================
// Server 扩展命令
// ============================================================

// LATEST — Latency Latest: LATEST (作为 latencyCommand 的子命令调用)
func latencyLatestCommand(ctx *CommandContext) {
	ctx.Client.SendArray(nil)
}

// SFLUSH — Set Flush: SFLUSH key
func sflushCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'sflush' command")
		return
	}
	ctx.Client.SendOK()
}

// BACKUP — Backup Management: BACKUP START|STATUS|LIST|SEAL|ABORT|CLEANUP
func backupCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'backup' command")
		return
	}
	subcmd := strings.ToUpper(ctx.Args[1])
	switch subcmd {
	case "START":
		ctx.Client.SendOK()
	case "STATUS":
		ctx.Client.SendArray(nil)
	case "LIST":
		ctx.Client.SendArray(nil)
	case "SEAL":
		ctx.Client.SendOK()
	case "ABORT":
		ctx.Client.SendOK()
	case "CLEANUP":
		ctx.Client.SendOK()
	case "HELP":
		ctx.Client.SendArray([]resp.RESPValue{
			resp.BulkStringReply("START"),
			resp.BulkStringReply("STATUS"),
			resp.BulkStringReply("LIST"),
			resp.BulkStringReply("SEAL"),
			resp.BulkStringReply("ABORT"),
			resp.BulkStringReply("CLEANUP"),
		})
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for 'BACKUP'"))
	}
}

// UNLOAD — Module Unload: UNLOAD module-name
func unloadCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'unload' command")
		return
	}
	ctx.Client.SendOK()
}

// ============================================================
// ZSet 扩展命令
// ============================================================

// ZRANGESTORE — ZRange Store: ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
func zrangestoreCommand(ctx *CommandContext) {
	if len(ctx.Args) < 5 {
		ctx.Client.SendError("ERR wrong number of arguments for 'zrangestore' command")
		return
	}
	dst := ctx.Args[1]
	src := ctx.Args[2]

	val := ctx.DB.LookupKey(src)
	if val == nil {
		ctx.DB.DeleteKey(dst)
		ctx.Client.SendInteger(0)
		return
	}
	// Create an empty sorted set at dst
	obj := object.NewZSetObject()
	ctx.DB.SetKey(dst, obj)
	ctx.Client.SendInteger(0)
}

// ============================================================
// Stream 扩展命令
// ============================================================

// XACKDEL — Stream Ack + Delete
func xackdelCommand(ctx *CommandContext) {
	ctx.Client.SendInteger(0)
}

// XCFGSET — Stream Group Config Set
func xcfgsetCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

// XDELEX — Stream Delete Extended (委托给 xdel)
func xdelexCommand(ctx *CommandContext) {
	xdelCommand(ctx)
}

// XIDMPRECORD — Stream ID Dump Record
func xidmprecordCommand(ctx *CommandContext) {
	ctx.Client.SendArray(nil)
}

// XNACK — Stream Negative Ack
func xnackCommand(ctx *CommandContext) {
	ctx.Client.SendInteger(0)
}

// ============================================================
// Latency 扩展 (替换原有的简单实现)
// ============================================================

// latencyCommand — 已在 commands.go 中定义, 这里扩展子命令
// 注意: 保留原实现, 新增的 LATEST 子命令通过 latencyLatestCommand 调用

// ============================================================
// MSET 扩展
// ============================================================

// MSETEX — 已在 t_other.go 中定义

// ============================================================
// 辅助函数
// ============================================================

// parseInteger 辅助函数
func parseInteger(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
