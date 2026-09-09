// Package commands 实现 Redis 命令表 and all command handlers.
// This is the Go equivalent of Redis's commands.c and t_*.c files.
package commands

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/km-dev/km-go-redis/internal/db"
	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// Command 表示 Redis 命令定义.
// This matches Redis's redisCommand struct.
type Command struct {
	Name     string
	Handler  func(ctx *CommandContext)
	Arity    int    // -n means at least n args, positive means exactly n
	Flags    int
	FirstKey int
	LastKey  int
	KeyStep  int
}

// CommandContext 提供命令执行上下文 for a command.
type CommandContext struct {
	Client  ClientInterface
	Args    []string
	Command *Command
	DB      *db.DB
	DBIndex int
	Server  ServerInterface
}

// --- CommandContext Pool (P1) ---
// Pool for reusing CommandContext objects to reduce GC pressure on hot path.

var ctxPool = sync.Pool{
	New: func() interface{} {
		return &CommandContext{}
	},
}

// GetCommandContext returns a pooled CommandContext. Caller must call PutCommandContext when done.
func GetCommandContext() *CommandContext {
	return ctxPool.Get().(*CommandContext)
}

// PutCommandContext resets and returns a CommandContext to the pool.
func PutCommandContext(ctx *CommandContext) {
	if ctx == nil {
		return
	}
	// Reset all fields to zero values to avoid retaining references.
	*ctx = CommandContext{}
	ctxPool.Put(ctx)
}

// --- Fast command lookup key ---
// cmdKey is a fixed-size representation of a lowercased command name (max 15 bytes).
// Used for O(1) map lookup without string allocation on the hot path.
type cmdKey [16]byte // [0]=len, [1..15]=lowercase name bytes

func makeCmdKey(name string) cmdKey {
	var k cmdKey
	n := len(name)
	if n > 15 {
		n = 15
	}
	k[0] = byte(n)
	for i := 0; i < n; i++ {
		c := name[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		k[i+1] = c
	}
	return k
}

// ClientInterface abstracts client operations for command handlers.
type ClientInterface interface {
	SendReply(v resp.RESPValue) error
	SendBulkString(s string) error
	SendInteger(n int64) error
	SendSimpleString(s string) error
	SendError(msg string) error
	SendOK() error
	SendNull() error
	SendArray(items []resp.RESPValue) error
	SendNil() error
	ReplyTypeMismatch() error
	ReplyWrongArgCount(cmd string) error
	GetArgv() []string
	SetArgv([]string)
	GetDB() *db.DB
	SetDB(d *db.DB)
	GetDBIndex() int
	SetDBIndex(i int)
	GetAuthenticated() bool
	SetAuthenticated(b bool)
	IsResp3() bool
	GetClientName() string
	GetSubscriptions() map[string]bool
	GetPatternSubs() map[string]bool
}

// ServerInterface abstracts server operations for command handlers.
type ServerInterface interface {
	BroadcastPubSub(channel string, message string) int
	Subscribe(client ClientInterface, channel string)
	Unsubscribe(client ClientInterface, channel string)
	PSubscribe(client ClientInterface, pattern string)
	PUnsubscribe(client ClientInterface, pattern string)
	// Lua 脚本支持
	EvalScript(code string, keys []string, args []string) (interface{}, error)
	EvalSHA(sha string, keys []string, args []string) (interface{}, error)
	LoadScript(code string) (string, error)
	ScriptExists(shas []string) []bool
	ScriptFlush()
}

// Command flags (matching Redis's CMD_FLAG_* constants).
const (
	CmdWrite     = 1 << 0
	CmdReadonly  = 1 << 1
	CmdDenyOOM   = 1 << 2
	CmdAdmin     = 1 << 3
	CmdPubSub    = 1 << 4
	CmdNoScript  = 1 << 5
	CmdRandom    = 1 << 6
	CmdSortForScript = 1 << 7
	CmdLoading  = 1 << 8
	CmdStale    = 1 << 9
	CmdSkipMonitor  = 1 << 10
	CmdAsking   = 1 << 11
	CmdFast     = 1 << 12
	CmdNoAuth   = 1 << 13
	CmdMayReplicate = 1 << 14
)

// CommandTable 保存所有已注册命令.
type CommandTable struct {
	commands map[string]*Command
	fast     map[cmdKey]*Command // fast lookup by [16]byte key (P0 optimization)
}

// NewCommandTable creates a new command table with all Redis 8.10 commands.
func NewCommandTable() *CommandTable {
	ct := &CommandTable{
		commands: make(map[string]*Command),
		fast:     make(map[cmdKey]*Command),
	}
	ct.registerAll()
	return ct
}

// Lookup 按名称查找命令 (case-insensitive).
func (ct *CommandTable) Lookup(name string) *Command {
	name = strings.ToLower(name)
	return ct.commands[name]
}

// LookupLower 按已转小写的名称查找命令 (P0 fast path).
// Caller must ensure name is already lowercase. Uses [16]byte key for
// zero-allocation map lookup.
func (ct *CommandTable) LookupLower(name string) *Command {
	return ct.fast[makeCmdKey(name)]
}

// Register 向表中添加命令.
func (ct *CommandTable) Register(cmd *Command) {
	lower := strings.ToLower(cmd.Name)
	ct.commands[lower] = cmd
	ct.fast[makeCmdKey(cmd.Name)] = cmd
}

// Count returns the number of registered commands.
func (ct *CommandTable) Count() int {
	return len(ct.commands)
}

// GetCommands returns all command names.
func (ct *CommandTable) GetCommands() []string {
	names := make([]string, 0, len(ct.commands))
	for name := range ct.commands {
		names = append(names, name)
	}
	return names
}

// registerAll registers all Redis 8.10 commands.
func (ct *CommandTable) registerAll() {
	// --- Server commands ---
	ct.Register(&Command{Name: "ping", Handler: pingCommand, Arity: -1, Flags: CmdStale | CmdFast})
	ct.Register(&Command{Name: "echo", Handler: echoCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "select", Handler: selectCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "auth", Handler: authCommand, Arity: -2, Flags: CmdNoAuth | CmdNoScript | CmdFast})
	ct.Register(&Command{Name: "hello", Handler: helloCommand, Arity: -1, Flags: CmdNoAuth | CmdNoScript | CmdFast})
	ct.Register(&Command{Name: "quit", Handler: quitCommand, Arity: 1, Flags: CmdNoAuth | CmdFast})
	ct.Register(&Command{Name: "command", Handler: commandCommand, Arity: -1, Flags: CmdRandom | CmdLoading})
	ct.Register(&Command{Name: "dbsize", Handler: dbsizeCommand, Arity: 1, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "flushdb", Handler: flushdbCommand, Arity: -1, Flags: CmdWrite})
	ct.Register(&Command{Name: "flushall", Handler: flushallCommand, Arity: -1, Flags: CmdWrite})
	ct.Register(&Command{Name: "info", Handler: infoCommand, Arity: -1, Flags: CmdRandom | CmdLoading})
	ct.Register(&Command{Name: "client", Handler: clientCommand, Arity: -2, Flags: CmdAdmin | CmdRandom})
	ct.Register(&Command{Name: "config", Handler: configCommand, Arity: -2, Flags: CmdAdmin | CmdLoading})
	ct.Register(&Command{Name: "time", Handler: timeCommand, Arity: 1, Flags: CmdRandom | CmdFast})
	ct.Register(&Command{Name: "dbfilename", Handler: dbfilenameCommand, Arity: 1, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "lastsave", Handler: lastsaveCommand, Arity: 1, Flags: CmdRandom | CmdFast})
	ct.Register(&Command{Name: "save", Handler: saveCommand, Arity: 1, Flags: CmdAdmin | CmdWrite})
	ct.Register(&Command{Name: "bgsave", Handler: bgsaveCommand, Arity: -1, Flags: CmdAdmin | CmdWrite})
	ct.Register(&Command{Name: "bgrewriteaof", Handler: bgrewriteaofCommand, Arity: 1, Flags: CmdAdmin | CmdWrite})
	ct.Register(&Command{Name: "shutdown", Handler: shutdownCommand, Arity: -1, Flags: CmdAdmin | CmdLoading | CmdStale})
	ct.Register(&Command{Name: "debug", Handler: debugCommand, Arity: -2, Flags: CmdAdmin | CmdRandom})
	// 其他缺失命令
	ct.Register(&Command{Name: "lcs", Handler: lcsCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "role", Handler: roleCommand, Arity: 1, Flags: CmdAdmin | CmdRandom})
	ct.Register(&Command{Name: "memory", Handler: memoryCommand, Arity: -2, Flags: CmdAdmin | CmdRandom})
	ct.Register(&Command{Name: "move", Handler: moveCommand, Arity: 3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "dump", Handler: dumpCommand, Arity: 2, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "restore", Handler: restoreCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "wait", Handler: waitCommand, Arity: 3, Flags: CmdAdmin})
	ct.Register(&Command{Name: "waitaof", Handler: waitaofCommand, Arity: 4, Flags: CmdAdmin})
	ct.Register(&Command{Name: "swapdb", Handler: swapdbCommand, Arity: 3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "sort_ro", Handler: sortroCommand, Arity: -2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "spublish", Handler: spublishCommand, Arity: 3, Flags: CmdPubSub | CmdLoading | CmdFast})
	ct.Register(&Command{Name: "ssubscribe", Handler: ssubscribeCommand, Arity: -2, Flags: CmdPubSub | CmdNoScript | CmdLoading})
	ct.Register(&Command{Name: "sunsubscribe", Handler: sunsubscribeCommand, Arity: -1, Flags: CmdPubSub | CmdNoScript | CmdLoading})
	ct.Register(&Command{Name: "sync", Handler: syncCommand, Arity: 1, Flags: CmdAdmin})
	ct.Register(&Command{Name: "msetex", Handler: msetexCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "monitor", Handler: monitorCommand, Arity: 1, Flags: CmdAdmin | CmdNoScript})
	ct.Register(&Command{Name: "slowlog", Handler: slowlogCommand, Arity: -2, Flags: CmdAdmin | CmdRandom})
	ct.Register(&Command{Name: "acl", Handler: aclCommand, Arity: -2, Flags: CmdAdmin | CmdNoAuth})
	ct.Register(&Command{Name: "latency", Handler: latencyCommand, Arity: -2, Flags: CmdAdmin | CmdRandom})
	ct.Register(&Command{Name: "module", Handler: moduleCommand, Arity: -2, Flags: CmdAdmin})
	ct.Register(&Command{Name: "lolwut", Handler: lolwutCommand, Arity: -1, Flags: CmdReadonly})

	// --- String commands (t_string.c) ---
	ct.Register(&Command{Name: "get", Handler: getCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "set", Handler: setCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "append", Handler: appendCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "strlen", Handler: strlenCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "incr", Handler: incrCommand, Arity: 2, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "incrby", Handler: incrbyCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "incrbyfloat", Handler: incrbyfloatCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "decr", Handler: decrCommand, Arity: 2, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "decrby", Handler: decrbyCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "mget", Handler: mgetCommand, Arity: -2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "mset", Handler: msetCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "msetnx", Handler: msetnxCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "setnx", Handler: setnxCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "setex", Handler: setexCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "psetex", Handler: psetexCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "getset", Handler: getsetCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "getrange", Handler: getrangeCommand, Arity: 4, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "setrange", Handler: setrangeCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "getdel", Handler: getdelCommand, Arity: 2, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "getex", Handler: getexCommand, Arity: -2, Flags: CmdWrite | CmdFast})

	// --- Key commands (db.c) ---
	ct.Register(&Command{Name: "del", Handler: delCommand, Arity: -2, Flags: CmdWrite})
	ct.Register(&Command{Name: "unlink", Handler: unlinkCommand, Arity: -2, Flags: CmdWrite})
	ct.Register(&Command{Name: "exists", Handler: existsCommand, Arity: -2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "type", Handler: typeCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "expire", Handler: expireCommand, Arity: 3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "expireat", Handler: expireatCommand, Arity: 3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "pexpire", Handler: pexpireCommand, Arity: 3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "pexpireat", Handler: pexpireatCommand, Arity: 3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "ttl", Handler: ttlCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "pttl", Handler: pttlCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "persist", Handler: persistCommand, Arity: 2, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "rename", Handler: renameCommand, Arity: 3, Flags: CmdWrite})
	ct.Register(&Command{Name: "renamenx", Handler: renamenxCommand, Arity: 3, Flags: CmdWrite})
	ct.Register(&Command{Name: "keys", Handler: keysCommand, Arity: 2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "scan", Handler: scanCommand, Arity: -2, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "randomkey", Handler: randomkeyCommand, Arity: 1, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "sort", Handler: sortCommand, Arity: -2, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "object", Handler: objectCommand, Arity: -2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "touch", Handler: touchCommand, Arity: -2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "copy", Handler: copyCommand, Arity: -3, Flags: CmdWrite})

	// --- List commands (t_list.c) ---
	ct.Register(&Command{Name: "lpush", Handler: lpushCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "rpush", Handler: rpushCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "lpop", Handler: lpopCommand, Arity: -2, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "rpop", Handler: rpopCommand, Arity: -2, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "lrange", Handler: lrangeCommand, Arity: 4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "llen", Handler: llenCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "lindex", Handler: lindexCommand, Arity: 3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "lset", Handler: lsetCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "lrem", Handler: lremCommand, Arity: 4, Flags: CmdWrite})
	ct.Register(&Command{Name: "ltrim", Handler: ltrimCommand, Arity: 4, Flags: CmdWrite})
	ct.Register(&Command{Name: "lpos", Handler: lposCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "linsert", Handler: linsertCommand, Arity: 5, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "lpushx", Handler: lpushxCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "rpushx", Handler: rpushxCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "blpop", Handler: blpopCommand, Arity: -3, Flags: CmdWrite | CmdNoScript})
	ct.Register(&Command{Name: "brpop", Handler: brpopCommand, Arity: -3, Flags: CmdWrite | CmdNoScript})
	ct.Register(&Command{Name: "rpoplpush", Handler: rpoplpushCommand, Arity: 3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "lmove", Handler: lmoveCommand, Arity: 5, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "blmove", Handler: blmoveCommand, Arity: 6, Flags: CmdWrite | CmdDenyOOM | CmdNoScript})
	ct.Register(&Command{Name: "lmpop", Handler: lmpopCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "blmpop", Handler: blmpopCommand, Arity: -5, Flags: CmdWrite | CmdNoScript})

	// --- Hash commands (t_hash.c) ---
	ct.Register(&Command{Name: "hset", Handler: hsetCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "hget", Handler: hgetCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hmset", Handler: hmsetCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "hmget", Handler: hmgetCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hgetall", Handler: hgetallCommand, Arity: 2, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "hdel", Handler: hdelCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hlen", Handler: hlenCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hexists", Handler: hexistsCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hincrby", Handler: hincrbyCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "hincrbyfloat", Handler: hincrbyfloatCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "hkeys", Handler: hkeysCommand, Arity: 2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "hvals", Handler: hvalsCommand, Arity: 2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "hsetnx", Handler: hsetnxCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "hstrlen", Handler: hstrlenCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hexpire", Handler: hexpireCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hexpireat", Handler: hexpireatCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hpexpire", Handler: hpexpireCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hpexpireat", Handler: hpexpireatCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hpersist", Handler: hpersistCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "httl", Handler: httlCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hpttl", Handler: hpttlCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hexpiretime", Handler: hexpiretimeCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hpexpiretime", Handler: hpexpiretimeCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "hgetex", Handler: hgetexCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hgetdel", Handler: hgetdelCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "hsetex", Handler: hsetexCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "hscan", Handler: hscanCommand, Arity: -3, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "hrandfield", Handler: hrandfieldCommand, Arity: -2, Flags: CmdReadonly | CmdRandom})

	// --- Set commands (t_set.c) ---
	ct.Register(&Command{Name: "sadd", Handler: saddCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "srem", Handler: sremCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "smembers", Handler: smembersCommand, Arity: 2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "sismember", Handler: sismemberCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "scard", Handler: scardCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "spop", Handler: spopCommand, Arity: -2, Flags: CmdWrite | CmdRandom | CmdFast})
	ct.Register(&Command{Name: "srandmember", Handler: srandmemberCommand, Arity: -2, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "sunion", Handler: sunionCommand, Arity: -2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "sunionstore", Handler: sunionstoreCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "sinter", Handler: sinterCommand, Arity: -2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "sinterstore", Handler: sinterstoreCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "sdiff", Handler: sdiffCommand, Arity: -2, Flags: CmdReadonly | CmdSortForScript})
	ct.Register(&Command{Name: "sdiffstore", Handler: sdiffstoreCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "smove", Handler: smoveCommand, Arity: 4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "sscan", Handler: sscanCommand, Arity: -3, Flags: CmdReadonly | CmdRandom})
	// Set 新增命令（Redis 6.2+）
	ct.Register(&Command{Name: "smismember", Handler: smismemberCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "sintercard", Handler: sintercardCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "sdiffcard", Handler: sdiffcardCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "sunioncard", Handler: sunioncardCommand, Arity: -3, Flags: CmdReadonly | CmdFast})

	// --- Sorted set commands (t_zset.c) ---
	ct.Register(&Command{Name: "zadd", Handler: zaddCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "zincrby", Handler: zincrbyCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "zrem", Handler: zremCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "zrange", Handler: zrangeCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zrevrange", Handler: zrevrangeCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zrangebyscore", Handler: zrangebyscoreCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zrevrangebyscore", Handler: zrevrangebyscoreCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zrangebylex", Handler: zrangebylexCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zcard", Handler: zcardCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zscore", Handler: zscoreCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zrank", Handler: zrankCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zrevrank", Handler: zrevrankCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zcount", Handler: zcountCommand, Arity: 4, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zremrangebyrank", Handler: zremrangebyrankCommand, Arity: 4, Flags: CmdWrite})
	ct.Register(&Command{Name: "zremrangebyscore", Handler: zremrangebyscoreCommand, Arity: 4, Flags: CmdWrite})
	ct.Register(&Command{Name: "zunionstore", Handler: zunionstoreCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM})
	// ZSet 新增命令（Redis 6.2+）
	ct.Register(&Command{Name: "zdiff", Handler: zdiffCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zdiffstore", Handler: zdiffstoreCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "zinter", Handler: zinterCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zintercard", Handler: zintercardCommand, Arity: -3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zunion", Handler: zunionCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zmpop", Handler: zmpopCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "bzmpop", Handler: bzmpopCommand, Arity: -5, Flags: CmdWrite | CmdNoScript})
	ct.Register(&Command{Name: "bzpopmax", Handler: bzpopmaxCommand, Arity: -3, Flags: CmdWrite | CmdNoScript})
	ct.Register(&Command{Name: "bzpopmin", Handler: bzpopminCommand, Arity: -3, Flags: CmdWrite | CmdNoScript})
	ct.Register(&Command{Name: "zremrangebylex", Handler: zremrangebylexCommand, Arity: 4, Flags: CmdWrite})
	ct.Register(&Command{Name: "zrevrangebylex", Handler: zrevrangebylexCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "zinterstore", Handler: zinterstoreCommand, Arity: -4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "zlexcount", Handler: zlexcountCommand, Arity: 4, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "zpopmin", Handler: zpopminCommand, Arity: -2, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "zpopmax", Handler: zpopmaxCommand, Arity: -2, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "zscan", Handler: zscanCommand, Arity: -3, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "zrandmember", Handler: zrandmemberCommand, Arity: -2, Flags: CmdReadonly | CmdRandom})
	ct.Register(&Command{Name: "zmscore", Handler: zmscoreCommand, Arity: -3, Flags: CmdReadonly | CmdFast})

	// --- Pub/Sub commands (pubsub.c) ---
	ct.Register(&Command{Name: "subscribe", Handler: subscribeCommand, Arity: -2, Flags: CmdPubSub | CmdNoScript | CmdLoading})
	ct.Register(&Command{Name: "unsubscribe", Handler: unsubscribeCommand, Arity: -1, Flags: CmdPubSub | CmdNoScript | CmdLoading})
	ct.Register(&Command{Name: "psubscribe", Handler: psubscribeCommand, Arity: -2, Flags: CmdPubSub | CmdNoScript | CmdLoading})
	ct.Register(&Command{Name: "punsubscribe", Handler: punsubscribeCommand, Arity: -1, Flags: CmdPubSub | CmdNoScript | CmdLoading})
	ct.Register(&Command{Name: "publish", Handler: publishCommand, Arity: 3, Flags: CmdPubSub | CmdLoading | CmdFast})
	ct.Register(&Command{Name: "pubsub", Handler: pubsubCommand, Arity: -2, Flags: CmdPubSub | CmdRandom})

	// --- Transaction commands ---
	ct.Register(&Command{Name: "multi", Handler: multiCommand, Arity: 1, Flags: CmdNoScript | CmdFast})
	ct.Register(&Command{Name: "exec", Handler: execCommand, Arity: 1, Flags: CmdNoScript | CmdSkipMonitor})
	ct.Register(&Command{Name: "discard", Handler: discardCommand, Arity: 1, Flags: CmdNoScript | CmdFast})
	ct.Register(&Command{Name: "watch", Handler: watchCommand, Arity: -2, Flags: CmdNoScript | CmdFast})
	ct.Register(&Command{Name: "unwatch", Handler: unwatchCommand, Arity: 1, Flags: CmdNoScript | CmdFast})

	// --- Scripting ---
	ct.Register(&Command{Name: "eval", Handler: evalCommand, Arity: -3, Flags: CmdNoScript | CmdMayReplicate})
	ct.Register(&Command{Name: "evalsha", Handler: evalshaCommand, Arity: -3, Flags: CmdNoScript | CmdMayReplicate})
	ct.Register(&Command{Name: "script", Handler: scriptCommand, Arity: -2, Flags: CmdNoScript})

	// --- HyperLogLog ---
	ct.Register(&Command{Name: "pfadd", Handler: pfaddCommand, Arity: -2, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "pfcount", Handler: pfcountCommand, Arity: -2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "pfmerge", Handler: pfmergeCommand, Arity: -2, Flags: CmdWrite | CmdDenyOOM})

	// --- Stream commands (t_stream.c) ---
	ct.Register(&Command{Name: "xadd", Handler: xaddCommand, Arity: -5, Flags: CmdWrite | CmdDenyOOM | CmdFast})
	ct.Register(&Command{Name: "xrange", Handler: xrangeCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "xrevrange", Handler: xrevrangeCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "xlen", Handler: xlenCommand, Arity: 2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "xread", Handler: xreadCommand, Arity: -4, Flags: CmdReadonly | CmdNoScript})
	ct.Register(&Command{Name: "xdel", Handler: xdelCommand, Arity: -3, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "xtrim", Handler: xtrimCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "xinfo", Handler: xinfoCommand, Arity: -2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "xgroup", Handler: xgroupCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "xreadgroup", Handler: xreadgroupCommand, Arity: -7, Flags: CmdWrite | CmdNoScript})
	ct.Register(&Command{Name: "xack", Handler: xackCommand, Arity: -4, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "xclaim", Handler: xclaimCommand, Arity: -6, Flags: CmdWrite | CmdFast})
	ct.Register(&Command{Name: "xpending", Handler: xpendingCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "xsetid", Handler: xsetidCommand, Arity: 3, Flags: CmdWrite})
	ct.Register(&Command{Name: "xautoclaim", Handler: xautoclaimCommand, Arity: -6, Flags: CmdWrite | CmdFast})

	// --- Geo commands ---
	ct.Register(&Command{Name: "geoadd", Handler: geoaddCommand, Arity: -5, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "geopos", Handler: geoposCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "geodist", Handler: geodistCommand, Arity: -4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "geohash", Handler: geohashCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "georadius", Handler: georadiusCommand, Arity: -6, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "georadiusbymember", Handler: georadiusbymemberCommand, Arity: -5, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "geosearch", Handler: geosearchCommand, Arity: -7, Flags: CmdReadonly})
	ct.Register(&Command{Name: "geosearchstore", Handler: geosearchstoreCommand, Arity: -8, Flags: CmdWrite | CmdDenyOOM})

	// --- Bitmap commands (bitops.c) ---
	ct.Register(&Command{Name: "setbit", Handler: setbitCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "getbit", Handler: getbitCommand, Arity: 3, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "bitcount", Handler: bitcountCommand, Arity: -2, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "bitpos", Handler: bitposCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "bitop", Handler: bitopCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "bitfield", Handler: bitfieldCommand, Arity: -2, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "bitfield_ro", Handler: bitfieldroCommand, Arity: -2, Flags: CmdReadonly | CmdFast})

	// --- Cluster commands ---
	ct.Register(&Command{Name: "cluster", Handler: clusterCommand, Arity: -2, Flags: CmdAdmin})
	ct.Register(&Command{Name: "asking", Handler: askingCommand, Arity: 1, Flags: CmdFast})
	ct.Register(&Command{Name: "readonly", Handler: readonlyCommand, Arity: 1, Flags: CmdReadonly | CmdFast})
	ct.Register(&Command{Name: "readwrite", Handler: readwriteCommand, Arity: 1, Flags: CmdReadonly | CmdFast})

	// --- Sentinel commands ---
	ct.Register(&Command{Name: "sentinel", Handler: sentinelCommand, Arity: -2, Flags: CmdAdmin})
	ct.Register(&Command{Name: "failover", Handler: failoverCommand, Arity: -1, Flags: CmdAdmin})
	ct.Register(&Command{Name: "replicaof", Handler: replicaofCommand, Arity: 3, Flags: CmdAdmin | CmdNoAuth})
	ct.Register(&Command{Name: "replconf", Handler: replconfCommand, Arity: -1, Flags: CmdAdmin | CmdNoAuth})

	// ============================================================
	// 扩展命令 (补全 Rust 版缺失)
	// ============================================================

	// AR* 命令 (Array 类型扩展)
	ct.Register(&Command{Name: "arcount", Handler: arcountCommand, Arity: 3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "ardel", Handler: ardelCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "ardelrange", Handler: ardelrangeCommand, Arity: 5, Flags: CmdWrite})
	ct.Register(&Command{Name: "arget", Handler: argetCommand, Arity: 4, Flags: CmdReadonly})
	ct.Register(&Command{Name: "argetrange", Handler: argetrangeCommand, Arity: 5, Flags: CmdReadonly})
	ct.Register(&Command{Name: "argrep", Handler: argrepCommand, Arity: 5, Flags: CmdWrite})
	ct.Register(&Command{Name: "arinfo", Handler: arinfoCommand, Arity: 2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "arinsert", Handler: arinsertCommand, Arity: -5, Flags: CmdWrite})
	ct.Register(&Command{Name: "arlastitems", Handler: arlastitemsCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "arlen", Handler: arlenCommand, Arity: 2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "armget", Handler: armgetCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "armset", Handler: armsetCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "arnext", Handler: arnextCommand, Arity: 2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "arop", Handler: aropCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "arring", Handler: arringCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "arscan", Handler: arscanCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "arseek", Handler: arseekCommand, Arity: 3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "arset", Handler: arsetCommand, Arity: -3, Flags: CmdWrite})

	// List 扩展
	ct.Register(&Command{Name: "lmovem", Handler: lmovemCommand, Arity: 6, Flags: CmdWrite | CmdDenyOOM})
	ct.Register(&Command{Name: "blmovem", Handler: blmovemCommand, Arity: 6, Flags: CmdWrite | CmdDenyOOM | CmdNoScript})
	ct.Register(&Command{Name: "brpoplpush", Handler: brpoplpushCommand, Arity: 4, Flags: CmdWrite | CmdDenyOOM | CmdNoScript})

	// Hash 扩展
	ct.Register(&Command{Name: "himport", Handler: himportCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "pexpiretime", Handler: pexpiretimeCommand, Arity: 2, Flags: CmdReadonly | CmdFast})

	// String 扩展
	ct.Register(&Command{Name: "increx", Handler: increxCommand, Arity: -3, Flags: CmdWrite | CmdDenyOOM})

	// Function/Rcall (Redis 7+)
	ct.Register(&Command{Name: "fcall", Handler: fcallCommand, Arity: -3, Flags: CmdWrite | CmdMayReplicate})
	ct.Register(&Command{Name: "fcall_ro", Handler: fcallroCommand, Arity: -3, Flags: CmdReadonly})
	ct.Register(&Command{Name: "function", Handler: functionCommand, Arity: -2, Flags: CmdAdmin | CmdNoScript})

	// Replication
	ct.Register(&Command{Name: "psync", Handler: psyncCommand, Arity: 3, Flags: CmdAdmin})
	ct.Register(&Command{Name: "slaveof", Handler: slaveofCommand, Arity: 3, Flags: CmdAdmin | CmdNoAuth})

	// Key 扩展
	ct.Register(&Command{Name: "keyslot", Handler: keyslotCommand, Arity: 2, Flags: CmdReadonly | CmdFast})

	// Server 扩展
	ct.Register(&Command{Name: "sflush", Handler: sflushCommand, Arity: 2, Flags: CmdWrite})
	ct.Register(&Command{Name: "backup", Handler: backupCommand, Arity: -2, Flags: CmdAdmin})
	ct.Register(&Command{Name: "unload", Handler: unloadCommand, Arity: 2, Flags: CmdAdmin})

	// ZSet 扩展
	ct.Register(&Command{Name: "zrangestore", Handler: zrangestoreCommand, Arity: -5, Flags: CmdWrite})

	// Stream 扩展
	ct.Register(&Command{Name: "xackdel", Handler: xackdelCommand, Arity: -4, Flags: CmdWrite})
	ct.Register(&Command{Name: "xcfgset", Handler: xcfgsetCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "xdelex", Handler: xdelexCommand, Arity: -3, Flags: CmdWrite})
	ct.Register(&Command{Name: "xidmprecord", Handler: xidmprecordCommand, Arity: 2, Flags: CmdReadonly})
	ct.Register(&Command{Name: "xnack", Handler: xnackCommand, Arity: -4, Flags: CmdWrite})
}

// --- Command Handlers ---

// PING
func pingCommand(ctx *CommandContext) {
	if len(ctx.Args) > 1 {
		ctx.Client.SendBulkString(ctx.Args[1])
	} else {
		ctx.Client.SendSimpleString("PONG")
	}
}

// ECHO
func echoCommand(ctx *CommandContext) {
	ctx.Client.SendBulkString(ctx.Args[1])
}

// SELECT
func selectCommand(ctx *CommandContext) {
	idx, err := strconv.Atoi(ctx.Args[1])
	if err != nil || idx < 0 || idx >= 16 {
		ctx.Client.SendError("ERR invalid DB index")
		return
	}
	ctx.Client.SetDBIndex(idx)
	// The actual DB selection is handled by the server
	ctx.Client.SendOK()
}

// AUTH
func authCommand(ctx *CommandContext) {
	// Simplified auth - in production, check against config.requirepass
	ctx.Client.SetAuthenticated(true)
	ctx.Client.SendOK()
}

// HELLO - RESP3 handshake (redis-cli v8 sends this on connect)
func helloCommand(ctx *CommandContext) {
	// Always return RESP2-style array (redis-cli accepts this)
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply("server"), resp.BulkStringReply("redis"),
		resp.BulkStringReply("version"), resp.BulkStringReply("8.10.0"),
		resp.BulkStringReply("proto"), resp.IntegerReply(2),
		resp.BulkStringReply("id"), resp.IntegerReply(1),
		resp.BulkStringReply("name"), resp.BulkStringReply(""),
		resp.BulkStringReply("mode"), resp.BulkStringReply("standalone"),
		resp.BulkStringReply("role"), resp.BulkStringReply("master"),
		resp.BulkStringReply("modules"), resp.ArrayReply([]resp.RESPValue{}),
	})
}

// QUIT
func quitCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

// COMMAND - returns command info
func commandCommand(ctx *CommandContext) {
	// redis-benchmark sends COMMAND DOCS and COMMAND INFO
	// Return a simple array response
	if len(ctx.Args) > 1 {
		subcmd := strings.ToLower(ctx.Args[1])
		switch subcmd {
		case "count":
			ctx.Client.SendInteger(200) // approximate command count
			return
		case "docs", "info":
			// Return empty array for each requested command
			result := make([]resp.RESPValue, len(ctx.Args)-2)
			for i := range result {
				result[i] = resp.NullArray
			}
			ctx.Client.SendArray(result)
			return
		}
	}
	ctx.Client.SendArray([]resp.RESPValue{})
}

// DBSIZE
func dbsizeCommand(ctx *CommandContext) {
	ctx.Client.SendInteger(ctx.DB.DBSize())
}

// FLUSHDB
func flushdbCommand(ctx *CommandContext) {
	ctx.DB.FlushDB()
	ctx.Client.SendOK()
}

// FLUSHALL
func flushallCommand(ctx *CommandContext) {
	ctx.DB.FlushDB()
	ctx.Client.SendOK()
}

// INFO
func infoCommand(ctx *CommandContext) {
	section := "default"
	if len(ctx.Args) > 1 {
		section = strings.ToLower(ctx.Args[1])
	}
	var info strings.Builder
	switch section {
	case "server", "default":
		info.WriteString("# Server\r\n")
		info.WriteString("redis_version:8.10.0\r\n")
		info.WriteString("redis_mode:standalone\r\n")
		info.WriteString("os:Go\r\n")
		info.WriteString("tcp_port:6379\r\n")
		info.WriteString("uptime_in_seconds:0\r\n")
		info.WriteString("uptime_in_days:0\r\n")
	case "clients":
		info.WriteString("# Clients\r\n")
		info.WriteString("connected_clients:1\r\n")
	case "memory":
		info.WriteString("# Memory\r\n")
		info.WriteString("used_memory:0\r\n")
	case "stats":
		info.WriteString("# Stats\r\n")
		info.WriteString("total_connections_received:0\r\n")
		info.WriteString("total_commands_processed:0\r\n")
	case "replication":
		info.WriteString("# Replication\r\n")
		info.WriteString("role:master\r\n")
	case "all":
		info.WriteString("# Server\r\n")
		info.WriteString("redis_version:8.10.0\r\n")
	}
	ctx.Client.SendBulkString(info.String())
}

// CLIENT
func clientCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'client' command")
		return
	}
	subcmd := strings.ToLower(ctx.Args[1])
	switch subcmd {
	case "list":
		ctx.Client.SendBulkString("id=1 addr=127.0.0.1:0 fd=0 name= db=0 flags=N qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client")
	case "setname":
		if len(ctx.Args) < 3 {
			ctx.Client.SendError("ERR wrong number of arguments for 'client|setname' command")
			return
		}
		ctx.Client.SendOK()
	case "getname":
		name := ctx.Client.GetClientName()
		if name == "" {
			ctx.Client.SendNull()
		} else {
			ctx.Client.SendBulkString(name)
		}
	case "id":
		ctx.Client.SendInteger(1)
	case "info":
		ctx.Client.SendBulkString(ctx.Client.GetClientName())
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR unknown subcommand '%s'", subcmd))
	}
}

// CONFIG
func configCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'config' command")
		return
	}
	subcmd := strings.ToLower(ctx.Args[1])
	switch subcmd {
	case "get":
		if len(ctx.Args) < 3 {
			ctx.Client.SendArray([]resp.RESPValue{})
			return
		}
		pattern := strings.ToLower(ctx.Args[2])
		result := make([]resp.RESPValue, 0)
		// Return common config params matching the pattern
		configs := map[string]string{
			"databases":       "16",
			"maxmemory":       "0",
			"maxmemory-policy": "noeviction",
			"timeout":         "0",
			"tcp-keepalive":   "300",
			"port":            "6379",
			"bind":            "*",
			"save":            "3600 1 300 100 60 10000",
			"loglevel":        "notice",
			"requirepass":     "",
			"appendonly":      "no",
			"dbfilename":      "dump.rdb",
			"dir":             "./",
			"maxclients":      "10000",
		}
		for k, v := range configs {
			if pattern == "*" || pattern == k || strings.Contains(k, pattern) {
				result = append(result, resp.BulkStringReply(k), resp.BulkStringReply(v))
			}
		}
		ctx.Client.SendArray(result)
	case "set":
		ctx.Client.SendOK()
	case "resetstat":
		ctx.Client.SendOK()
	case "rewrite":
		ctx.Client.SendOK()
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR unknown subcommand '%s'", subcmd))
	}
}

// TIME
func timeCommand(ctx *CommandContext) {
	now := time.Now()
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply(strconv.FormatInt(now.Unix(), 10)),
		resp.BulkStringReply(strconv.FormatInt(int64(now.Nanosecond()/1000), 10)),
	})
}

func dbfilenameCommand(ctx *CommandContext) {
	ctx.Client.SendBulkString("dump.rdb")
}

func lastsaveCommand(ctx *CommandContext) {
	ctx.Client.SendInteger(0)
}

func saveCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

func bgsaveCommand(ctx *CommandContext) {
	ctx.Client.SendSimpleString("Background saving started")
}

func bgrewriteaofCommand(ctx *CommandContext) {
	ctx.Client.SendSimpleString("Background append only file rewriting started")
}

func shutdownCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

func debugCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR DEBUG is not allowed in cluster mode")
}

func monitorCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}

func slowlogCommand(ctx *CommandContext) {
	ctx.Client.SendArray(nil)
}

func aclCommand(ctx *CommandContext) {
	ctx.Client.SendSimpleString("OK")
}

func latencyCommand(ctx *CommandContext) {
	ctx.Client.SendArray(nil)
}

func moduleCommand(ctx *CommandContext) {
	ctx.Client.SendSimpleString("OK")
}

func lolwutCommand(ctx *CommandContext) {
	ctx.Client.SendBulkString("Redis ver. 8.10.0 (Go port)")
}

// --- String Commands (t_string.c equivalent) ---

// GET
func getCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendBulkString(val.GetString())
}

// SET - supports EX, PX, NX, XX, KEEPTTL, GET options
func setCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	value := ctx.Args[2]

	// Parse options
	var exSeconds, pxMillis int64
	var nx, xx, get bool
	var keepttl bool

	for i := 3; i < len(ctx.Args); i++ {
		opt := strings.ToUpper(ctx.Args[i])
		switch opt {
		case "EX":
			i++
			if i >= len(ctx.Args) {
				ctx.Client.SendError("ERR syntax error")
				return
			}
			n, err := strconv.ParseInt(ctx.Args[i], 10, 64)
			if err != nil || n <= 0 {
				ctx.Client.SendError("ERR invalid expire time in 'set' command")
				return
			}
			exSeconds = n
		case "PX":
			i++
			if i >= len(ctx.Args) {
				ctx.Client.SendError("ERR syntax error")
				return
			}
			n, err := strconv.ParseInt(ctx.Args[i], 10, 64)
			if err != nil || n <= 0 {
				ctx.Client.SendError("ERR invalid expire time in 'set' command")
				return
			}
			pxMillis = n
		case "EXAT":
			i++
			if i >= len(ctx.Args) {
				ctx.Client.SendError("ERR syntax error")
				return
			}
			n, err := strconv.ParseInt(ctx.Args[i], 10, 64)
			if err != nil || n <= 0 {
				ctx.Client.SendError("ERR invalid expire time in 'set' command")
				return
			}
			exSeconds = n - time.Now().Unix()
		case "PXAT":
			i++
			if i >= len(ctx.Args) {
				ctx.Client.SendError("ERR syntax error")
				return
			}
			n, err := strconv.ParseInt(ctx.Args[i], 10, 64)
			if err != nil || n <= 0 {
				ctx.Client.SendError("ERR invalid expire time in 'set' command")
				return
			}
			pxMillis = n - time.Now().UnixMilli()
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepttl = true
		case "GET":
			get = true
		}
	}

	// Check NX/XX conditions
	exists := ctx.DB.Exists(key)
	if nx && exists {
		ctx.Client.SendNull()
		return
	}
	if xx && !exists {
		ctx.Client.SendNull()
		return
	}

	// If GET, return old value
	if get {
		old := ctx.DB.LookupKey(key)
		if old != nil && old.Type == object.TypeString {
			ctx.Client.SendBulkString(old.GetString())
		} else if old != nil {
			ctx.Client.ReplyTypeMismatch()
			return
		} else {
			ctx.Client.SendNull()
		}
	}

	// Set the value
	obj := object.NewStringObject(value)
	ctx.DB.SetKey(key, obj)

	// Set expiration
	if !keepttl {
		if exSeconds > 0 {
			ctx.DB.SetExpire(key, time.Now().Add(time.Duration(exSeconds)*time.Second))
		} else if pxMillis > 0 {
			ctx.DB.SetExpire(key, time.Now().Add(time.Duration(pxMillis)*time.Millisecond))
		}
	}

	if !get {
		ctx.Client.SendOK()
	}
}

// APPEND
func appendCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	value := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		obj := object.NewStringObject(value)
		ctx.DB.SetKey(key, obj)
		ctx.Client.SendInteger(int64(len(value)))
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	val.Append(value)
	ctx.Client.SendInteger(int64(val.GetStringLen()))
}

// STRLEN
func strlenCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(int64(val.GetStringLen()))
}

// INCR
func incrCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		obj := object.NewStringObjectFromInt(1)
		ctx.DB.SetKey(key, obj)
		ctx.Client.SendInteger(1)
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	newVal, err := val.IncrementInt(1)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.DB.SetKey(key, newVal)
	ctx.Client.SendInteger(newVal.Ptr.(int64))
}

// INCRBY
func incrbyCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	delta, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		obj := object.NewStringObjectFromInt(delta)
		ctx.DB.SetKey(key, obj)
		ctx.Client.SendInteger(delta)
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	newVal, err := val.IncrementInt(delta)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.DB.SetKey(key, newVal)
	ctx.Client.SendInteger(newVal.Ptr.(int64))
}

// INCRBYFLOAT
func incrbyfloatCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	delta, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not a valid float")
		return
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		obj := object.NewStringObject(fmt.Sprintf("%g", delta))
		ctx.DB.SetKey(key, obj)
		ctx.Client.SendBulkString(fmt.Sprintf("%g", delta))
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	newVal, err := val.IncrementFloat(delta)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.DB.SetKey(key, newVal)
	ctx.Client.SendBulkString(newVal.GetString())
}

// DECR
func decrCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		obj := object.NewStringObjectFromInt(-1)
		ctx.DB.SetKey(key, obj)
		ctx.Client.SendInteger(-1)
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	newVal, err := val.IncrementInt(-1)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.DB.SetKey(key, newVal)
	ctx.Client.SendInteger(newVal.Ptr.(int64))
}

// DECRBY
func decrbyCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	delta, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		obj := object.NewStringObjectFromInt(-delta)
		ctx.DB.SetKey(key, obj)
		ctx.Client.SendInteger(-delta)
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	newVal, err := val.IncrementInt(-delta)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.DB.SetKey(key, newVal)
	ctx.Client.SendInteger(newVal.Ptr.(int64))
}

// MGET
func mgetCommand(ctx *CommandContext) {
	result := make([]resp.RESPValue, len(ctx.Args)-1)
	for i := 1; i < len(ctx.Args); i++ {
		val := ctx.DB.LookupKey(ctx.Args[i])
		if val == nil || val.Type != object.TypeString {
			result[i-1] = resp.NullBulk
		} else {
			result[i-1] = resp.BulkStringReply(val.GetString())
		}
	}
	ctx.Client.SendArray(result)
}

// MSET
func msetCommand(ctx *CommandContext) {
	if (len(ctx.Args)-1)%2 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'mset' command")
		return
	}
	for i := 1; i < len(ctx.Args); i += 2 {
		key := ctx.Args[i]
		value := ctx.Args[i+1]
		obj := object.NewStringObject(value)
		ctx.DB.SetKey(key, obj)
	}
	ctx.Client.SendOK()
}

// MSETNX
func msetnxCommand(ctx *CommandContext) {
	if (len(ctx.Args)-1)%2 != 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'msetnx' command")
		return
	}
	// Check if any key exists
	for i := 1; i < len(ctx.Args); i += 2 {
		if ctx.DB.Exists(ctx.Args[i]) {
			ctx.Client.SendInteger(0)
			return
		}
	}
	for i := 1; i < len(ctx.Args); i += 2 {
		obj := object.NewStringObject(ctx.Args[i+1])
		ctx.DB.SetKey(ctx.Args[i], obj)
	}
	ctx.Client.SendInteger(1)
}

// SETNX
func setnxCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	value := ctx.Args[2]
	if ctx.DB.Exists(key) {
		ctx.Client.SendInteger(0)
		return
	}
	obj := object.NewStringObject(value)
	ctx.DB.SetKey(key, obj)
	ctx.Client.SendInteger(1)
}

// SETEX
func setexCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	sec, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil || sec <= 0 {
		ctx.Client.SendError("ERR invalid expire time in 'setex' command")
		return
	}
	value := ctx.Args[3]
	obj := object.NewStringObject(value)
	ctx.DB.SetKey(key, obj)
	ctx.DB.SetExpire(key, time.Now().Add(time.Duration(sec)*time.Second))
	ctx.Client.SendOK()
}

// PSETEX
func psetexCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ms, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil || ms <= 0 {
		ctx.Client.SendError("ERR invalid expire time in 'psetex' command")
		return
	}
	value := ctx.Args[3]
	obj := object.NewStringObject(value)
	ctx.DB.SetKey(key, obj)
	ctx.DB.SetExpire(key, time.Now().Add(time.Duration(ms)*time.Millisecond))
	ctx.Client.SendOK()
}

// GETSET
func getsetCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	value := ctx.Args[2]
	old := ctx.DB.LookupKey(key)
	if old != nil && old.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	if old == nil {
		ctx.Client.SendNull()
	} else {
		ctx.Client.SendBulkString(old.GetString())
	}
	obj := object.NewStringObject(value)
	ctx.DB.SetKey(key, obj)
}

// GETRANGE
func getrangeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	start, _ := strconv.Atoi(ctx.Args[2])
	end, _ := strconv.Atoi(ctx.Args[3])
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendBulkString("")
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	s := val.GetString()
	if start < 0 {
		start = len(s) + start
	}
	if end < 0 {
		end = len(s) + end
	}
	if start < 0 {
		start = 0
	}
	if end >= len(s) {
		end = len(s) - 1
	}
	if start > end || start >= len(s) {
		ctx.Client.SendBulkString("")
		return
	}
	ctx.Client.SendBulkString(s[start : end+1])
}

// SETRANGE
func setrangeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	offset, _ := strconv.Atoi(ctx.Args[2])
	value := ctx.Args[3]
	val := ctx.DB.LookupKey(key)
	var s string
	if val != nil {
		if val.Type != object.TypeString {
			ctx.Client.ReplyTypeMismatch()
			return
		}
		s = val.GetString()
	}
	if offset < 0 {
		ctx.Client.SendError("ERR offset is out of range")
		return
	}
	// Pad with null bytes if needed
	if offset > len(s) {
		s += strings.Repeat("\x00", offset-len(s))
	}
	newStr := s[:offset] + value
	if offset+len(value) < len(s) {
		newStr += s[offset+len(value):]
	}
	obj := object.NewStringObject(newStr)
	ctx.DB.SetKey(key, obj)
	ctx.Client.SendInteger(int64(len(newStr)))
}

// GETDEL
func getdelCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	result := val.GetString()
	ctx.DB.DeleteKey(key)
	ctx.Client.SendBulkString(result)
}

// GETEX - GET with expiration options
func getexCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendNull()
		return
	}
	if val.Type != object.TypeString {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	// Parse expiration options
	for i := 2; i < len(ctx.Args); i++ {
		opt := strings.ToUpper(ctx.Args[i])
		switch opt {
		case "EX":
			i++
			sec, _ := strconv.ParseInt(ctx.Args[i], 10, 64)
			ctx.DB.SetExpire(key, time.Now().Add(time.Duration(sec)*time.Second))
		case "PX":
			i++
			ms, _ := strconv.ParseInt(ctx.Args[i], 10, 64)
			ctx.DB.SetExpire(key, time.Now().Add(time.Duration(ms)*time.Millisecond))
		case "PERSIST":
			ctx.DB.Persist(key)
		}
	}
	ctx.Client.SendBulkString(val.GetString())
}

// --- Key Commands (db.c equivalent) ---

// DEL
func delCommand(ctx *CommandContext) {
	deleted := 0
	for i := 1; i < len(ctx.Args); i++ {
		if ctx.DB.DeleteKey(ctx.Args[i]) {
			deleted++
		}
	}
	ctx.Client.SendInteger(int64(deleted))
}

// UNLINK (async DEL)
func unlinkCommand(ctx *CommandContext) {
	delCommand(ctx)
}

// EXISTS
func existsCommand(ctx *CommandContext) {
	count := 0
	for i := 1; i < len(ctx.Args); i++ {
		if ctx.DB.Exists(ctx.Args[i]) {
			count++
		}
	}
	ctx.Client.SendInteger(int64(count))
}

// TYPE
func typeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ctx.Client.SendSimpleString(ctx.DB.Type(key))
}

// EXPIRE
func expireCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	sec, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	if !ctx.DB.Exists(key) {
		ctx.Client.SendInteger(0)
		return
	}
	ctx.DB.SetExpire(key, time.Now().Add(time.Duration(sec)*time.Second))
	ctx.Client.SendInteger(1)
}

// EXPIREAT
func expireatCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ts, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	if !ctx.DB.Exists(key) {
		ctx.Client.SendInteger(0)
		return
	}
	ctx.DB.SetExpire(key, time.Unix(ts, 0))
	ctx.Client.SendInteger(1)
}

// PEXPIRE
func pexpireCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ms, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	if !ctx.DB.Exists(key) {
		ctx.Client.SendInteger(0)
		return
	}
	ctx.DB.SetExpire(key, time.Now().Add(time.Duration(ms)*time.Millisecond))
	ctx.Client.SendInteger(1)
}

// PEXPIREAT
func pexpireatCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ms, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		ctx.Client.SendError("ERR value is not an integer or out of range")
		return
	}
	if !ctx.DB.Exists(key) {
		ctx.Client.SendInteger(0)
		return
	}
	ctx.DB.SetExpire(key, time.UnixMilli(ms))
	ctx.Client.SendInteger(1)
}

// TTL
func ttlCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ttl := ctx.DB.TTL(key)
	ctx.Client.SendInteger(ttl)
}

// PTTL
func pttlCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	ttl := ctx.DB.PTTL(key)
	ctx.Client.SendInteger(ttl)
}

// PERSIST
func persistCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	if ctx.DB.Persist(key) {
		ctx.Client.SendInteger(1)
	} else {
		ctx.Client.SendInteger(0)
	}
}

// RENAME
func renameCommand(ctx *CommandContext) {
	err := ctx.DB.Rename(ctx.Args[1], ctx.Args[2])
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	ctx.Client.SendOK()
}

// RENAMENX
func renamenxCommand(ctx *CommandContext) {
	ok, err := ctx.DB.RenameNX(ctx.Args[1], ctx.Args[2])
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	if ok {
		ctx.Client.SendInteger(1)
	} else {
		ctx.Client.SendInteger(0)
	}
}

// KEYS
func keysCommand(ctx *CommandContext) {
	pattern := ctx.Args[1]
	keys := ctx.DB.GetKeys(pattern)
	result := make([]resp.RESPValue, len(keys))
	for i, k := range keys {
		result[i] = resp.BulkStringReply(k)
	}
	ctx.Client.SendArray(result)
}

// SCAN
func scanCommand(ctx *CommandContext) {
	cursor, _ := strconv.ParseUint(ctx.Args[1], 10, 64)
	pattern := ""
	count := 10
	for i := 2; i < len(ctx.Args); i += 2 {
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
	newCursor, keys := ctx.DB.Scan(cursor, pattern, count)
	result := make([]resp.RESPValue, 2)
	result[0] = resp.BulkStringReply(strconv.FormatUint(newCursor, 10))
	keyList := make([]resp.RESPValue, len(keys))
	for i, k := range keys {
		keyList[i] = resp.BulkStringReply(k)
	}
	result[1] = resp.ArrayReply(keyList)
	ctx.Client.SendArray(result)
}

// RANDOMKEY
func randomkeyCommand(ctx *CommandContext) {
	key := ctx.DB.RandomKey()
	if key == "" {
		ctx.Client.SendNull()
		return
	}
	ctx.Client.SendBulkString(key)
}

// SORT (simplified)
func sortCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray(nil)
		return
	}
	// Simplified: just return the raw list/set members
	switch val.Type {
	case object.TypeList:
		ql := val.Ptr.(*object.QuickList)
		result := make([]resp.RESPValue, ql.Len())
		for i := 0; i < ql.Len(); i++ {
			v, _ := ql.Index(i)
			result[i] = resp.BulkStringReply(fmt.Sprintf("%v", v))
		}
		ctx.Client.SendArray(result)
	default:
		ctx.Client.SendArray(nil)
	}
}

func objectCommand(ctx *CommandContext) {
	ctx.Client.SendNull()
}

func touchCommand(ctx *CommandContext) {
	count := 0
	for i := 1; i < len(ctx.Args); i++ {
		if ctx.DB.Exists(ctx.Args[i]) {
			count++
		}
	}
	ctx.Client.SendInteger(int64(count))
}

func copyCommand(ctx *CommandContext) {
	ctx.Client.SendInteger(0)
}

// ... (List, Hash, Set, ZSet, PubSub, Stream, etc. commands are in separate files)
