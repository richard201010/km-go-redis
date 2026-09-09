// 命令单元测试 — 覆盖所有新增命令 + 核心命令
package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/km-dev/km-go-redis/internal/db"
	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// ============================================================
// 测试基础设施
// ============================================================

// mockClient 模拟 RESP 客户端用于单元测试
type mockClient struct {
	replies    []string
	replyTypes []string // "+", "-", ":", "$", "*"
	database   *db.Database
	db         *db.DB
	dbIndex    int
	argv       []string
}

func newMockClient() *mockClient {
	database := db.New(16)
	d, _ := database.GetDB(0)
	return &mockClient{
		replies:    make([]string, 0),
		replyTypes: make([]string, 0),
		database:   database,
		db:         d,
		dbIndex:    0,
		argv:       make([]string, 0),
	}
}

func (m *mockClient) SendReply(v resp.RESPValue) error {
	return nil
}
func (m *mockClient) SendBulkString(s string) error {
	m.replies = append(m.replies, s)
	m.replyTypes = append(m.replyTypes, "$")
	return nil
}
func (m *mockClient) SendInteger(n int64) error {
	m.replies = append(m.replies, fmt.Sprintf("%d", n))
	m.replyTypes = append(m.replyTypes, ":")
	return nil
}
func (m *mockClient) SendSimpleString(s string) error {
	m.replies = append(m.replies, s)
	m.replyTypes = append(m.replyTypes, "+")
	return nil
}
func (m *mockClient) SendError(msg string) error {
	m.replies = append(m.replies, msg)
	m.replyTypes = append(m.replyTypes, "-")
	return nil
}
func (m *mockClient) SendOK() error {
	m.replies = append(m.replies, "OK")
	m.replyTypes = append(m.replyTypes, "+")
	return nil
}
func (m *mockClient) SendNull() error {
	m.replies = append(m.replies, "(nil)")
	m.replyTypes = append(m.replyTypes, "$")
	return nil
}
func (m *mockClient) SendArray(items []resp.RESPValue) error {
	if items == nil {
		m.replies = append(m.replies, "(nil)")
		m.replyTypes = append(m.replyTypes, "*")
	} else {
		m.replies = append(m.replies, fmt.Sprintf("%d", len(items)))
		m.replyTypes = append(m.replyTypes, "*")
	}
	return nil
}
func (m *mockClient) SendNil() error {
	m.replies = append(m.replies, "(nil)")
	m.replyTypes = append(m.replyTypes, "$")
	return nil
}
func (m *mockClient) ReplyTypeMismatch() error {
	m.replies = append(m.replies, "WRONGTYPE")
	m.replyTypes = append(m.replyTypes, "-")
	return nil
}
func (m *mockClient) ReplyWrongArgCount(cmd string) error {
	m.replies = append(m.replies, fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd))
	m.replyTypes = append(m.replyTypes, "-")
	return nil
}
func (m *mockClient) GetArgv() []string  { return m.argv }
func (m *mockClient) SetArgv(a []string) { m.argv = a }
func (m *mockClient) GetDB() *db.DB     { return m.db }
func (m *mockClient) SetDB(d *db.DB)    { m.db = d }
func (m *mockClient) GetDBIndex() int   { return m.dbIndex }
func (m *mockClient) SetDBIndex(i int)  { m.dbIndex = i }
func (m *mockClient) GetAuthenticated() bool  { return true }
func (m *mockClient) SetAuthenticated(b bool) {}
func (m *mockClient) IsResp3() bool           { return false }
func (m *mockClient) GetClientName() string   { return "test" }
func (m *mockClient) GetSubscriptions() map[string]bool  { return nil }
func (m *mockClient) GetPatternSubs() map[string]bool    { return nil }

// mockServer 模拟服务器
type mockServer struct{}

func (s *mockServer) BroadcastPubSub(channel string, message string) int { return 0 }
func (s *mockServer) Subscribe(client ClientInterface, channel string)   {}
func (s *mockServer) Unsubscribe(client ClientInterface, channel string) {}
func (s *mockServer) PSubscribe(client ClientInterface, pattern string)  {}
func (s *mockServer) PUnsubscribe(client ClientInterface, pattern string) {}
func (s *mockServer) EvalScript(code string, keys []string, args []string) (interface{}, error) {
	return nil, fmt.Errorf("not supported")
}
func (s *mockServer) EvalSHA(sha string, keys []string, args []string) (interface{}, error) {
	return nil, fmt.Errorf("not supported")
}
func (s *mockServer) LoadScript(code string) (string, error) {
	return "", fmt.Errorf("not supported")
}
func (s *mockServer) ScriptExists(shas []string) []bool { return nil }
func (s *mockServer) ScriptFlush()                     {}

func newTestContext(client *mockClient, args ...string) *CommandContext {
	return &CommandContext{
		Client:  client,
		Args:    args,
		Command: &Command{Name: args[0]},
		DB:      client.db,
		DBIndex: 0,
		Server:  &mockServer{},
	}
}

func lastReply(mc *mockClient) string {
	if len(mc.replies) == 0 {
		return ""
	}
	return mc.replies[len(mc.replies)-1]
}

func lastReplyType(mc *mockClient) string {
	if len(mc.replyTypes) == 0 {
		return ""
	}
	return mc.replyTypes[len(mc.replyTypes)-1]
}

// ============================================================
// AR* 命令测试
// ============================================================

func TestARCommands(t *testing.T) {
	commands := []struct {
		name string
		fn   func(*CommandContext)
	}{
		{"arcount", arcountCommand},
		{"ardel", ardelCommand},
		{"ardelrange", ardelrangeCommand},
		{"arget", argetCommand},
		{"argetrange", argetrangeCommand},
		{"argrep", argrepCommand},
		{"arinfo", arinfoCommand},
		{"arinsert", arinsertCommand},
		{"arlastitems", arlastitemsCommand},
		{"arlen", arlenCommand},
		{"armget", armgetCommand},
		{"armset", armsetCommand},
		{"arnext", arnextCommand},
		{"arop", aropCommand},
		{"arring", arringCommand},
		{"arscan", arscanCommand},
		{"arseek", arseekCommand},
		{"arset", arsetCommand},
	}

	for _, cmd := range commands {
		t.Run(cmd.name, func(t *testing.T) {
			mc := newMockClient()
			ctx := newTestContext(mc, cmd.name, "arg1")
			cmd.fn(ctx)
			if lastReplyType(mc) != "-" {
				t.Errorf("%s: expected error reply, got %s", cmd.name, lastReplyType(mc))
			}
			if !strings.Contains(lastReply(mc), "Array type not supported") {
				t.Errorf("%s: expected 'Array type not supported', got %s", cmd.name, lastReply(mc))
			}
		})
	}
}

// ============================================================
// List 扩展命令测试
// ============================================================

func TestLmovemCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "lmovem", "source", "destination", "LEFT", "RIGHT", "0", "1")
	lmovemCommand(ctx)
	// 应委托给 lmoveCommand
}

func TestBlmovemCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "blmovem", "source", "destination", "LEFT", "RIGHT", "0", "1")
	blmovemCommand(ctx)
}

func TestBrpoplpushCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "brpoplpush", "source", "destination", "0")
	brpoplpushCommand(ctx)
}

// ============================================================
// Hash 扩展命令测试
// ============================================================

func TestHimportCommand(t *testing.T) {
	t.Run("normal import", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "himport", "myhash", "FIELDS", "2", "f1", "v1", "f2", "v2")
		himportCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
			t.Errorf("expected OK, got %s", lastReply(mc))
		}
	})

	t.Run("import with MAXLEN", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "himport", "myhash", "MAXLEN", "100", "FIELDS", "1", "f1", "v1")
		himportCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
			t.Errorf("expected OK, got %s", lastReply(mc))
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "himport", "myhash")
		himportCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

func TestPexpiretimeCommand(t *testing.T) {
	t.Run("key exists", func(t *testing.T) {
		mc := newMockClient()
		mc.db.SetKey("k1", object.NewStringObject("v1"))
		ctx := newTestContext(mc, "pexpiretime", "k1")
		pexpiretimeCommand(ctx)
		if lastReplyType(mc) != ":" || lastReply(mc) != "-1" {
			t.Errorf("expected -1, got %s", lastReply(mc))
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "pexpiretime", "nokey")
		pexpiretimeCommand(ctx)
		if lastReplyType(mc) != ":" || lastReply(mc) != "-2" {
			t.Errorf("expected -2, got %s", lastReply(mc))
		}
	})
}

// ============================================================
// String 扩展命令测试
// ============================================================

func TestIncrexCommand(t *testing.T) {
	t.Run("normal increment", func(t *testing.T) {
		mc := newMockClient()
		mc.db.SetKey("counter", object.NewStringObject("10"))
		ctx := newTestContext(mc, "increx", "counter", "5", "EX", "60")
		increxCommand(ctx)
		if lastReplyType(mc) != ":" {
			t.Errorf("expected integer reply, got %s", lastReplyType(mc))
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "increx", "counter")
		increxCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

// ============================================================
// Function 命令测试
// ============================================================

func TestFunctionCommand(t *testing.T) {
	tests := []struct {
		subcmd string
		args   []string
		expect string
	}{
		{"LIST", []string{"function", "LIST"}, "(nil)"},
		{"LOAD", []string{"function", "LOAD", "code"}, "noop"},
		{"DELETE", []string{"function", "DELETE", "lib"}, "OK"},
		{"DUMP", []string{"function", "DUMP"}, ""},
		{"RESTORE", []string{"function", "RESTORE", "val"}, "OK"},
		{"FLUSH", []string{"function", "FLUSH"}, "OK"},
		{"STATS", []string{"function", "STATS"}, "(nil)"},
		{"KILL", []string{"function", "KILL"}, "OK"},
	}

	for _, tt := range tests {
		t.Run(tt.subcmd, func(t *testing.T) {
			mc := newMockClient()
			ctx := newTestContext(mc, tt.args...)
			functionCommand(ctx)
			if lastReply(mc) != tt.expect {
				t.Errorf("function %s: expected %q, got %q", tt.subcmd, tt.expect, lastReply(mc))
			}
		})
	}

	t.Run("HELP", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "function", "HELP")
		functionCommand(ctx)
		if lastReplyType(mc) != "*" {
			t.Errorf("expected array, got %s", lastReplyType(mc))
		}
	})

	t.Run("unknown subcommand", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "function", "UNKNOWN")
		functionCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})

	t.Run("missing subcommand", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "function")
		functionCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

func TestFcallCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "fcall", "myfunc", "1", "k1")
	fcallCommand(ctx)
	if lastReplyType(mc) != "$" || lastReply(mc) != "(nil)" {
		t.Errorf("expected nil, got %s", lastReply(mc))
	}
}

func TestFcallroCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "fcall_ro", "myfunc", "1", "k1")
	fcallroCommand(ctx)
	if lastReplyType(mc) != "$" || lastReply(mc) != "(nil)" {
		t.Errorf("expected nil, got %s", lastReply(mc))
	}
}

// ============================================================
// Replication 命令测试
// ============================================================

func TestPsyncCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "psync", "?", "-1")
	psyncCommand(ctx)
	if lastReplyType(mc) != "+" {
		t.Errorf("expected simple string, got %s", lastReplyType(mc))
	}
	if !strings.Contains(lastReply(mc), "FULLRESYNC") {
		t.Errorf("expected FULLRESYNC, got %s", lastReply(mc))
	}
}

func TestSlaveofCommand(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "slaveof", "127.0.0.1", "6379")
		slaveofCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
			t.Errorf("expected OK, got %s", lastReply(mc))
		}
	})

	t.Run("no one", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "slaveof", "NO", "ONE")
		slaveofCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
			t.Errorf("expected OK, got %s", lastReply(mc))
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "slaveof")
		slaveofCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

// ============================================================
// Key 扩展命令测试
// ============================================================

func TestKeyslotCommand(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "keyslot", "mykey")
		keyslotCommand(ctx)
		if lastReplyType(mc) != ":" {
			t.Errorf("expected integer, got %s", lastReplyType(mc))
		}
	})

	t.Run("empty key", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "keyslot")
		keyslotCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})

	t.Run("CRC16 consistency", func(t *testing.T) {
		slot1 := crc16("foo") % 16384
		slot2 := crc16("foo") % 16384
		if slot1 != slot2 {
			t.Errorf("CRC16 not consistent: %d != %d", slot1, slot2)
		}
		if slot1 < 0 || slot1 >= 16384 {
			t.Errorf("slot out of range: %d", slot1)
		}
	})
}

// ============================================================
// Server 扩展命令测试
// ============================================================

func TestSflushCommand(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "sflush", "myset")
		sflushCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
			t.Errorf("expected OK, got %s", lastReply(mc))
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "sflush")
		sflushCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

func TestBackupCommand(t *testing.T) {
	tests := []struct {
		subcmd string
		args   []string
		expect string
	}{
		{"START", []string{"backup", "START"}, "OK"},
		{"STATUS", []string{"backup", "STATUS"}, "(nil)"},
		{"LIST", []string{"backup", "LIST"}, "(nil)"},
		{"SEAL", []string{"backup", "SEAL"}, "OK"},
		{"ABORT", []string{"backup", "ABORT"}, "OK"},
		{"CLEANUP", []string{"backup", "CLEANUP"}, "OK"},
	}

	for _, tt := range tests {
		t.Run(tt.subcmd, func(t *testing.T) {
			mc := newMockClient()
			ctx := newTestContext(mc, tt.args...)
			backupCommand(ctx)
			if lastReply(mc) != tt.expect {
				t.Errorf("backup %s: expected %q, got %q", tt.subcmd, tt.expect, lastReply(mc))
			}
		})
	}

	t.Run("HELP", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "backup", "HELP")
		backupCommand(ctx)
		if lastReplyType(mc) != "*" {
			t.Errorf("expected array, got %s", lastReplyType(mc))
		}
	})

	t.Run("unknown subcommand", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "backup", "UNKNOWN")
		backupCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})

	t.Run("missing subcommand", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "backup")
		backupCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

func TestUnloadCommand(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "unload", "mymodule")
		unloadCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
			t.Errorf("expected OK, got %s", lastReply(mc))
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "unload")
		unloadCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

// ============================================================
// ZSet 扩展命令测试
// ============================================================

func TestZrangestoreCommand(t *testing.T) {
	t.Run("source exists", func(t *testing.T) {
		mc := newMockClient()
		zs := object.NewZSet()
		zs.Add("member1", 1.0)
		zs.Add("member2", 2.0)
		obj := &object.Object{Type: object.TypeZSet, Ptr: zs}
		mc.db.SetKey("src", obj)

		ctx := newTestContext(mc, "zrangestore", "dst", "src", "0", "1")
		zrangestoreCommand(ctx)
		if lastReplyType(mc) != ":" || lastReply(mc) != "0" {
			t.Errorf("expected 0, got %s", lastReply(mc))
		}
	})

	t.Run("source not exists", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "zrangestore", "dst", "nosrc", "0", "1")
		zrangestoreCommand(ctx)
		if lastReplyType(mc) != ":" || lastReply(mc) != "0" {
			t.Errorf("expected 0, got %s", lastReply(mc))
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "zrangestore", "dst", "src")
		zrangestoreCommand(ctx)
		if lastReplyType(mc) != "-" {
			t.Errorf("expected error, got %s", lastReplyType(mc))
		}
	})
}

// ============================================================
// Stream 扩展命令测试
// ============================================================

func TestStreamExtensionCommands(t *testing.T) {
	tests := []struct {
		name       string
		fn         func(*CommandContext)
		args       []string
		expect     string
		expectType string
	}{
		{"xackdel", xackdelCommand, []string{"xackdel", "stream", "group", "1-1"}, "0", ":"},
		{"xcfgset", xcfgsetCommand, []string{"xcfgset", "stream", "group", "SET", "entries-read", "0"}, "OK", "+"},
		{"xidmprecord", xidmprecordCommand, []string{"xidmprecord", "stream"}, "(nil)", "*"},
		{"xnack", xnackCommand, []string{"xnack", "stream", "group", "1-1"}, "0", ":"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := newMockClient()
			ctx := newTestContext(mc, tt.args...)
			tt.fn(ctx)
			if lastReplyType(mc) != tt.expectType {
				t.Errorf("%s: expected type %s, got %s", tt.name, tt.expectType, lastReplyType(mc))
			}
			if lastReply(mc) != tt.expect {
				t.Errorf("%s: expected %q, got %q", tt.name, tt.expect, lastReply(mc))
			}
		})
	}

	t.Run("xdelex delegates to xdel", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "xdelex", "mystream", "1-1")
		xdelexCommand(ctx)
	})
}

// ============================================================
// 核心命令测试 (补充原有缺失)
// ============================================================

func TestPingCommand(t *testing.T) {
	t.Run("no args", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "PING")
		pingCommand(ctx)
		if lastReplyType(mc) != "+" || lastReply(mc) != "PONG" {
			t.Errorf("expected PONG, got %s", lastReply(mc))
		}
	})

	t.Run("with message", func(t *testing.T) {
		mc := newMockClient()
		ctx := newTestContext(mc, "PING", "hello")
		pingCommand(ctx)
		if lastReplyType(mc) != "$" || lastReply(mc) != "hello" {
			t.Errorf("expected hello, got %s", lastReply(mc))
		}
	})
}

func TestEchoCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "ECHO", "hello world")
	echoCommand(ctx)
	if lastReplyType(mc) != "$" || lastReply(mc) != "hello world" {
		t.Errorf("expected 'hello world', got %s", lastReply(mc))
	}
}

func TestSelectCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "SELECT", "3")
	selectCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("expected OK, got %s", lastReply(mc))
	}
	if mc.GetDBIndex() != 3 {
		t.Errorf("expected db index 3, got %d", mc.GetDBIndex())
	}
}

func TestSetGetCommands(t *testing.T) {
	mc := newMockClient()

	// SET
	ctx := newTestContext(mc, "SET", "mykey", "myvalue")
	setCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("SET: expected OK, got %s", lastReply(mc))
	}

	// GET
	ctx2 := newTestContext(mc, "GET", "mykey")
	getCommand(ctx2)
	if lastReplyType(mc) != "$" || lastReply(mc) != "myvalue" {
		t.Errorf("GET: expected 'myvalue', got %s", lastReply(mc))
	}

	// GET nonexistent
	ctx3 := newTestContext(mc, "GET", "nokey")
	getCommand(ctx3)
	if lastReplyType(mc) != "$" || lastReply(mc) != "(nil)" {
		t.Errorf("GET nonexistent: expected nil, got %s", lastReply(mc))
	}
}

func TestDelCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("v1"))
	mc.db.SetKey("k2", object.NewStringObject("v2"))

	ctx := newTestContext(mc, "DEL", "k1", "k2", "k3")
	delCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("expected 2, got %s", lastReply(mc))
	}
}

func TestExistsCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("v1"))

	ctx := newTestContext(mc, "EXISTS", "k1", "k2")
	existsCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("expected 1, got %s", lastReply(mc))
	}
}

func TestIncrDecrCommands(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("counter", object.NewStringObject("10"))

	// INCR
	ctx := newTestContext(mc, "INCR", "counter")
	incrCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "11" {
		t.Errorf("INCR: expected 11, got %s", lastReply(mc))
	}

	// DECR
	ctx2 := newTestContext(mc, "DECR", "counter")
	decrCommand(ctx2)
	if lastReplyType(mc) != ":" || lastReply(mc) != "10" {
		t.Errorf("DECR: expected 10, got %s", lastReply(mc))
	}
}

func TestAppendCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("hello"))

	ctx := newTestContext(mc, "APPEND", "k1", " world")
	appendCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "11" {
		t.Errorf("expected 11, got %s", lastReply(mc))
	}
}

func TestStrlenCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("hello"))

	ctx := newTestContext(mc, "STRLEN", "k1")
	strlenCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "5" {
		t.Errorf("expected 5, got %s", lastReply(mc))
	}
}

func TestMgetMsetCommands(t *testing.T) {
	mc := newMockClient()

	// MSET
	ctx := newTestContext(mc, "MSET", "k1", "v1", "k2", "v2")
	msetCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("MSET: expected OK, got %s", lastReply(mc))
	}

	// MGET
	ctx2 := newTestContext(mc, "MGET", "k1", "k2", "k3")
	mgetCommand(ctx2)
	if lastReplyType(mc) != "*" {
		t.Errorf("MGET: expected array, got %s", lastReplyType(mc))
	}
}

func TestHsetHgetCommands(t *testing.T) {
	mc := newMockClient()

	// HSET
	ctx := newTestContext(mc, "HSET", "myhash", "f1", "v1", "f2", "v2")
	hsetCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("HSET: expected 2, got %s", lastReply(mc))
	}

	// HGET
	ctx2 := newTestContext(mc, "HGET", "myhash", "f1")
	hgetCommand(ctx2)
	if lastReplyType(mc) != "$" || lastReply(mc) != "v1" {
		t.Errorf("HGET: expected 'v1', got %s", lastReply(mc))
	}
}

func TestSaddSmembersCommands(t *testing.T) {
	mc := newMockClient()

	// SADD
	ctx := newTestContext(mc, "SADD", "myset", "a", "b", "c")
	saddCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "3" {
		t.Errorf("SADD: expected 3, got %s", lastReply(mc))
	}

	// SCARD
	ctx2 := newTestContext(mc, "SCARD", "myset")
	scardCommand(ctx2)
	if lastReplyType(mc) != ":" || lastReply(mc) != "3" {
		t.Errorf("SCARD: expected 3, got %s", lastReply(mc))
	}
}

func TestZaddZrangeCommands(t *testing.T) {
	mc := newMockClient()

	// ZADD
	ctx := newTestContext(mc, "ZADD", "myzset", "1.0", "a", "2.0", "b", "3.0", "c")
	zaddCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "3" {
		t.Errorf("ZADD: expected 3, got %s", lastReply(mc))
	}

	// ZRANGE
	ctx2 := newTestContext(mc, "ZRANGE", "myzset", "0", "-1")
	zrangeCommand(ctx2)
	if lastReplyType(mc) != "*" {
		t.Errorf("ZRANGE: expected array, got %s", lastReplyType(mc))
	}
}

func TestPublishCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "PUBLISH", "channel", "hello")
	publishCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "0" {
		t.Errorf("PUBLISH: expected 0, got %s", lastReply(mc))
	}
}

func TestMultiExecCommands(t *testing.T) {
	mc := newMockClient()

	// MULTI
	ctx := newTestContext(mc, "MULTI")
	multiCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("MULTI: expected OK, got %s", lastReply(mc))
	}

	// EXEC
	ctx2 := newTestContext(mc, "EXEC")
	execCommand(ctx2)
	if lastReplyType(mc) != "*" {
		t.Errorf("EXEC: expected array, got %s", lastReplyType(mc))
	}
}

func TestExpireTtlCommands(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("v1"))

	// EXPIRE
	ctx := newTestContext(mc, "EXPIRE", "k1", "100")
	expireCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("EXPIRE: expected 1, got %s", lastReply(mc))
	}

	// TTL
	ctx2 := newTestContext(mc, "TTL", "k1")
	ttlCommand(ctx2)
	if lastReplyType(mc) != ":" {
		t.Errorf("TTL: expected integer, got %s", lastReplyType(mc))
	}
}

func TestKeysCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("a", object.NewStringObject("1"))
	mc.db.SetKey("b", object.NewStringObject("2"))
	mc.db.SetKey("c", object.NewStringObject("3"))

	ctx := newTestContext(mc, "KEYS", "*")
	keysCommand(ctx)
	if lastReplyType(mc) != "*" {
		t.Errorf("KEYS: expected array, got %s", lastReplyType(mc))
	}
}

func TestScanCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("a", object.NewStringObject("1"))
	mc.db.SetKey("b", object.NewStringObject("2"))

	ctx := newTestContext(mc, "SCAN", "0")
	scanCommand(ctx)
	if lastReplyType(mc) != "*" {
		t.Errorf("SCAN: expected array, got %s", lastReplyType(mc))
	}
}

func TestTypeCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("v1"))

	ctx := newTestContext(mc, "TYPE", "k1")
	typeCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "string" {
		t.Errorf("TYPE: expected 'string', got %s", lastReply(mc))
	}
}

func TestTimeCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "TIME")
	timeCommand(ctx)
	if lastReplyType(mc) != "*" {
		t.Errorf("TIME: expected array, got %s", lastReplyType(mc))
	}
}

func TestDbsizeCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("a", object.NewStringObject("1"))
	mc.db.SetKey("b", object.NewStringObject("2"))

	ctx := newTestContext(mc, "DBSIZE")
	dbsizeCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("DBSIZE: expected 2, got %s", lastReply(mc))
	}
}

func TestFlushdbFlushallCommands(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("a", object.NewStringObject("1"))

	// FLUSHDB
	ctx := newTestContext(mc, "FLUSHDB")
	flushdbCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("FLUSHDB: expected OK, got %s", lastReply(mc))
	}

	// Verify empty
	ctx2 := newTestContext(mc, "DBSIZE")
	dbsizeCommand(ctx2)
	if lastReplyType(mc) != ":" || lastReply(mc) != "0" {
		t.Errorf("DBSIZE after FLUSHDB: expected 0, got %s", lastReply(mc))
	}
}

func TestRenameCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("old", object.NewStringObject("value"))

	ctx := newTestContext(mc, "RENAME", "old", "new")
	renameCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("RENAME: expected OK, got %s", lastReply(mc))
	}

	// Verify
	ctx2 := newTestContext(mc, "GET", "new")
	getCommand(ctx2)
	if lastReplyType(mc) != "$" || lastReply(mc) != "value" {
		t.Errorf("GET after RENAME: expected 'value', got %s", lastReply(mc))
	}
}

func TestMoveCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("v1"))

	ctx := newTestContext(mc, "MOVE", "k1", "1")
	moveCommand(ctx)
	// Move is a stub, returns 0
	if lastReplyType(mc) != ":" {
		t.Errorf("MOVE: expected integer, got %s", lastReplyType(mc))
	}
}

func TestSortCommand(t *testing.T) {
	mc := newMockClient()
	ql := object.NewQuickList()
	ql.PushLeft("3")
	ql.PushLeft("1")
	ql.PushLeft("2")
	obj := &object.Object{Type: object.TypeList, Ptr: ql}
	mc.db.SetKey("mylist", obj)

	ctx := newTestContext(mc, "SORT", "mylist")
	sortCommand(ctx)
	if lastReplyType(mc) != "*" {
		t.Errorf("SORT: expected array, got %s", lastReplyType(mc))
	}
}

func TestLolwutCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "LOLWUT")
	lolwutCommand(ctx)
	if lastReplyType(mc) != "$" {
		t.Errorf("LOLWUT: expected bulk string, got %s", lastReplyType(mc))
	}
}

func TestTouchCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("k1", object.NewStringObject("v1"))

	ctx := newTestContext(mc, "TOUCH", "k1", "k2")
	touchCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("TOUCH: expected 1, got %s", lastReply(mc))
	}
}

func TestWaitCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "WAIT", "1", "1000")
	waitCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "0" {
		t.Errorf("WAIT: expected 0, got %s", lastReply(mc))
	}
}

func TestWaitaofCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "WAITAOF", "1", "1", "1000")
	waitaofCommand(ctx)
	if lastReplyType(mc) != "*" {
		t.Errorf("WAITAOF: expected array, got %s", lastReplyType(mc))
	}
}

func TestCommandCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "COMMAND", "COUNT")
	commandCommand(ctx)
	if lastReplyType(mc) != ":" {
		t.Errorf("COMMAND COUNT: expected integer, got %s", lastReplyType(mc))
	}
}

func TestInfoCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "INFO")
	infoCommand(ctx)
	if lastReplyType(mc) != "$" {
		t.Errorf("INFO: expected bulk string, got %s", lastReplyType(mc))
	}
}

func TestSlowlogCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "SLOWLOG", "LEN")
	slowlogCommand(ctx)
	// Slowlog is a stub
}

func TestMemoryCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "MEMORY", "DOCTOR")
	memoryCommand(ctx)
	// Memory is a stub
}

func TestLastsaveCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "LASTSAVE")
	lastsaveCommand(ctx)
	if lastReplyType(mc) != ":" {
		t.Errorf("LASTSAVE: expected integer, got %s", lastReplyType(mc))
	}
}

func TestBgrewriteaofCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "BGREWRITEAOF")
	bgrewriteaofCommand(ctx)
	if lastReplyType(mc) != "+" {
		t.Errorf("BGREWRITEAOF: expected simple string, got %s", lastReplyType(mc))
	}
}

func TestBgsaveCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "BGSAVE")
	bgsaveCommand(ctx)
	if lastReplyType(mc) != "+" {
		t.Errorf("BGSAVE: expected simple string, got %s", lastReplyType(mc))
	}
}

func TestSaveCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "SAVE")
	saveCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("SAVE: expected OK, got %s", lastReply(mc))
	}
}

func TestClusterCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "CLUSTER", "INFO")
	clusterCommand(ctx)
	if lastReplyType(mc) != "$" {
		t.Errorf("CLUSTER INFO: expected bulk string, got %s", lastReplyType(mc))
	}
}

func TestRoleCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "ROLE")
	roleCommand(ctx)
	if lastReplyType(mc) != "*" {
		t.Errorf("ROLE: expected array, got %s", lastReplyType(mc))
	}
}

func TestAskingCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "ASKING")
	askingCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("ASKING: expected OK, got %s", lastReply(mc))
	}
}

func TestReadonlyReadwriteCommands(t *testing.T) {
	mc := newMockClient()

	// READONLY
	ctx := newTestContext(mc, "READONLY")
	readonlyCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("READONLY: expected OK, got %s", lastReply(mc))
	}

	// READWRITE
	ctx2 := newTestContext(mc, "READWRITE")
	readwriteCommand(ctx2)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("READWRITE: expected OK, got %s", lastReply(mc))
	}
}

func TestSwapdbCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "SWAPDB", "0", "1")
	swapdbCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("SWAPDB: expected OK, got %s", lastReply(mc))
	}
}

func TestReplicaofCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "REPLICAOF", "NO", "ONE")
	replicaofCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("REPLICAOF: expected OK, got %s", lastReply(mc))
	}
}

func TestReplconfCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "REPLCONF", "ACK", "0")
	replconfCommand(ctx)
	if lastReplyType(mc) != "+" || lastReply(mc) != "OK" {
		t.Errorf("REPLCONF: expected OK, got %s", lastReply(mc))
	}
}

func TestFailoverCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "FAILOVER")
	failoverCommand(ctx)
	if lastReplyType(mc) != "-" {
		t.Errorf("FAILOVER: expected error, got %s", lastReplyType(mc))
	}
}

func TestHsetnxCommand(t *testing.T) {
	mc := newMockClient()

	// HSETNX (new field)
	ctx := newTestContext(mc, "HSETNX", "myhash", "f1", "v1")
	hsetnxCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("HSETNX: expected 1, got %s", lastReply(mc))
	}

	// HSETNX (existing field)
	ctx2 := newTestContext(mc, "HSETNX", "myhash", "f1", "v2")
	hsetnxCommand(ctx2)
	if lastReplyType(mc) != ":" || lastReply(mc) != "0" {
		t.Errorf("HSETNX existing: expected 0, got %s", lastReply(mc))
	}
}

func TestHstrlenCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myhash", &object.Object{
		Type: object.TypeHash,
		Ptr: func() *object.Hash {
			h := object.NewHash()
			h.Set("f1", "hello")
			return h
		}(),
	})

	ctx := newTestContext(mc, "HSTRLEN", "myhash", "f1")
	hstrlenCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "5" {
		t.Errorf("HSTRLEN: expected 5, got %s", lastReply(mc))
	}
}

func TestHincrbyCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myhash", &object.Object{
		Type: object.TypeHash,
		Ptr: func() *object.Hash {
			h := object.NewHash()
			h.Set("counter", "10")
			return h
		}(),
	})

	ctx := newTestContext(mc, "HINCRBY", "myhash", "counter", "5")
	hincrbyCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "15" {
		t.Errorf("HINCRBY: expected 15, got %s", lastReply(mc))
	}
}

func TestHlenCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myhash", &object.Object{
		Type: object.TypeHash,
		Ptr: func() *object.Hash {
			h := object.NewHash()
			h.Set("f1", "v1")
			h.Set("f2", "v2")
			return h
		}(),
	})

	ctx := newTestContext(mc, "HLEN", "myhash")
	hlenCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("HLEN: expected 2, got %s", lastReply(mc))
	}
}

func TestHdelCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myhash", &object.Object{
		Type: object.TypeHash,
		Ptr: func() *object.Hash {
			h := object.NewHash()
			h.Set("f1", "v1")
			h.Set("f2", "v2")
			return h
		}(),
	})

	ctx := newTestContext(mc, "HDEL", "myhash", "f1")
	hdelCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("HDEL: expected 1, got %s", lastReply(mc))
	}
}

func TestHexistsCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myhash", &object.Object{
		Type: object.TypeHash,
		Ptr: func() *object.Hash {
			h := object.NewHash()
			h.Set("f1", "v1")
			return h
		}(),
	})

	ctx := newTestContext(mc, "HEXISTS", "myhash", "f1")
	hexistsCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("HEXISTS: expected 1, got %s", lastReply(mc))
	}
}

func TestSremCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myset", &object.Object{
		Type: object.TypeSet,
		Ptr: func() *object.Set {
			s := object.NewSet()
			s.Add("a")
			s.Add("b")
			return s
		}(),
	})

	ctx := newTestContext(mc, "SREM", "myset", "a")
	sremCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("SREM: expected 1, got %s", lastReply(mc))
	}
}

func TestSismemberCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myset", &object.Object{
		Type: object.TypeSet,
		Ptr: func() *object.Set {
			s := object.NewSet()
			s.Add("a")
			return s
		}(),
	})

	ctx := newTestContext(mc, "SISMEMBER", "myset", "a")
	sismemberCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("SISMEMBER: expected 1, got %s", lastReply(mc))
	}
}

func TestZcardCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.0)
			zs.Add("b", 2.0)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZCARD", "myzset")
	zcardCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("ZCARD: expected 2, got %s", lastReply(mc))
	}
}

func TestZcountCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.0)
			zs.Add("b", 2.0)
			zs.Add("c", 3.0)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZCOUNT", "myzset", "1", "2")
	zcountCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("ZCOUNT: expected 2, got %s", lastReply(mc))
	}
}

func TestZrankCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.0)
			zs.Add("b", 2.0)
			zs.Add("c", 3.0)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZRANK", "myzset", "b")
	zrankCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("ZRANK: expected 1, got %s", lastReply(mc))
	}
}

func TestZrevrankCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.0)
			zs.Add("b", 2.0)
			zs.Add("c", 3.0)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZREVRANK", "myzset", "b")
	zrevrankCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("ZREVRANK: expected 1, got %s", lastReply(mc))
	}
}

func TestZscoreCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.5)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZSCORE", "myzset", "a")
	zscoreCommand(ctx)
	if lastReplyType(mc) != "$" {
		t.Errorf("ZSCORE: expected bulk string, got %s", lastReplyType(mc))
	}
}

func TestZincrbyCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.0)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZINCRBY", "myzset", "0.5", "a")
	zincrbyCommand(ctx)
	if lastReplyType(mc) != "$" {
		t.Errorf("ZINCRBY: expected bulk string, got %s", lastReplyType(mc))
	}
}

func TestZremCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("myzset", &object.Object{
		Type: object.TypeZSet,
		Ptr: func() *object.ZSet {
			zs := object.NewZSet()
			zs.Add("a", 1.0)
			zs.Add("b", 2.0)
			return zs
		}(),
	})

	ctx := newTestContext(mc, "ZREM", "myzset", "a")
	zremCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("ZREM: expected 1, got %s", lastReply(mc))
	}
}

func TestGeoaddCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "GEOADD", "geo", "13.361389", "38.115556", "Palermo")
	geoaddCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("GEOADD: expected 1, got %s", lastReply(mc))
	}
}

func TestLpushCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "LPUSH", "mylist", "a", "b", "c")
	lpushCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "3" {
		t.Errorf("LPUSH: expected 3, got %s", lastReply(mc))
	}
}

func TestLlenCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("mylist", &object.Object{
		Type: object.TypeList,
		Ptr: func() *object.QuickList {
			ql := object.NewQuickList()
			ql.PushLeft("a")
			ql.PushLeft("b")
			return ql
		}(),
	})

	ctx := newTestContext(mc, "LLEN", "mylist")
	llenCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "2" {
		t.Errorf("LLEN: expected 2, got %s", lastReply(mc))
	}
}

func TestLpopCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("mylist", &object.Object{
		Type: object.TypeList,
		Ptr: func() *object.QuickList {
			ql := object.NewQuickList()
			ql.PushLeft("a")
			return ql
		}(),
	})

	ctx := newTestContext(mc, "LPOP", "mylist")
	lpopCommand(ctx)
	if lastReplyType(mc) != "$" {
		t.Errorf("LPOP: expected bulk string, got %s", lastReplyType(mc))
	}
}

func TestLindexCommand(t *testing.T) {
	mc := newMockClient()
	mc.db.SetKey("mylist", &object.Object{
		Type: object.TypeList,
		Ptr: func() *object.QuickList {
			ql := object.NewQuickList()
			ql.PushLeft("a")
			ql.PushLeft("b")
			ql.PushLeft("c")
			return ql
		}(),
	})

	ctx := newTestContext(mc, "LINDEX", "mylist", "1")
	lindexCommand(ctx)
	if lastReplyType(mc) != "$" || lastReply(mc) != "b" {
		t.Errorf("LINDEX: expected 'b', got %s", lastReply(mc))
	}
}

func TestPfaddCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "PFADD", "hll", "a", "b", "c")
	pfaddCommand(ctx)
	if lastReplyType(mc) != ":" || lastReply(mc) != "1" {
		t.Errorf("PFADD: expected 1, got %s", lastReply(mc))
	}
}

func TestPfcountCommand(t *testing.T) {
	mc := newMockClient()
	ctx := newTestContext(mc, "PFCOUNT", "hll")
	pfcountCommand(ctx)
	if lastReplyType(mc) != ":" {
		t.Errorf("PFCOUNT: expected integer, got %s", lastReplyType(mc))
	}
}

// ============================================================
// CRC16 测试
// ============================================================

func TestCRC16(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"123456789", 0x31C3},
	}

	for _, tt := range tests {
		got := crc16(tt.input)
		if got != tt.want {
			t.Errorf("crc16(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
