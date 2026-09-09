// Package networking 实现 Redis 客户端管理 and connection handling.
// This is the Go equivalent of Redis's networking.c.
package networking

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/km-dev/km-go-redis/internal/commands"
	"github.com/km-dev/km-go-redis/internal/db"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// Client flags (matching Redis's CLIENT_* constants).
const (
	ClientSlave         = 1 << 0
	ClientMaster        = 1 << 1
	ClientMonitor       = 1 << 2
	ClientMulti         = 1 << 3
	ClientBlocked       = 1 << 4
	ClientDirtyCAS      = 1 << 5
	ClientCloseASAP     = 1 << 6
	ClientUnBlocked     = 1 << 7
	ClientScript        = 1 << 8
	ClientAsking        = 1 << 9
	ClientCloseAfterReply = 1 << 10
	ClientUnixSocket    = 1 << 11
	ClientDirtyExec     = 1 << 12
	ClientMasterForce   = 1 << 13
	ClientForceAOF      = 1 << 14
	ClientForceRepl     = 1 << 15
	ClientPrePSync      = 1 << 16
	ClientReadOnly      = 1 << 17
	ClientPubSub        = 1 << 18
	ClientPreventAOF    = 1 << 19
	ClientPreventRepl   = 1 << 20
	ClientPreventPROP   = 1 << 21
	ClientNoEvict       = 1 << 22
)

// Client 表示已连接的 Redis 客户端.
// This is the Go equivalent of Redis's client struct.
type Client struct {
	ID       uint64
	Conn     net.Conn
	Writer   *resp.Writer
	Reader   *resp.Reader
	Database *db.DB
	DBIndex  int

	// Command state
	Argv       []string // Current command arguments
	Argc       int      // Number of arguments
	Cmd        *commands.Command // Current command
	LastCmd    *commands.Command // Last executed command
	RespVersion int     // 2 or 3

	// Flags
	Flags    int
	Authenticated bool

	// Transaction (MULTI/EXEC)
	MultiState *MultiState

	// Pub/Sub
	Subscriptions map[string]bool
	PatternSubs   map[string]bool

	// Blocking
	BlockedKeys  []string
	BlockedType  int
	BlockTimeout time.Time

	// Reply buffer
	Reply       []resp.RESPValue
	ReplyBytes  int

	// Statistics
	QueryBufLen  int
	DBufLen      int
	ReplyLen     int
	LastInteraction time.Time
	CreateTime   time.Time
	QueryBufPeak int64
	NetInputBytes  int64
	NetOutputBytes int64

	// Memory
	MemUsageBucket *MemUsageBucket

	// Name
	Name string

	mu sync.Mutex
}

// MultiState holds MULTI/EXEC transaction state.
type MultiState struct {
	Commands []PendingCommand
	Active   bool
}

// PendingCommand stores a command queued in a transaction.
type PendingCommand struct {
	Cmd  *commands.Command
	Args []string
}

// MemUsageBucket tracks client memory usage.
type MemUsageBucket struct {
	Index      int
	MemUsageSum int64
}

var clientIDCounter uint64

// NewClient 创建新客户端 for the given connection.
func NewClient(conn net.Conn, currentDB *db.DB, dbIndex int) *Client {
	id := atomic.AddUint64(&clientIDCounter, 1)
	c := &Client{
		ID:           id,
		Conn:         conn,
		Writer:       resp.NewWriter(conn),
		Reader:       resp.NewReader(conn),
		Database:     currentDB,
		DBIndex:      dbIndex,
		RespVersion:  2,
		Authenticated: false,
		CreateTime:   time.Now(),
		LastInteraction: time.Now(),
		Subscriptions: make(map[string]bool),
		PatternSubs:  make(map[string]bool),
	}
	return c
}

// Close 关闭客户端连接.
func (c *Client) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

// SendReply 发送 RESP 值 to the client.
func (c *Client) SendReply(v resp.RESPValue) error {
	c.mu.Lock()
	if err := c.Writer.WriteValue(v); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendBulkString 发送批量字符串 reply (optimized, no alloc).
func (c *Client) SendBulkString(s string) error {
	c.mu.Lock()
	if err := c.Writer.WriteBulkStr(s); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendInteger 发送整数 reply (optimized, cached for 0-9999).
func (c *Client) SendInteger(n int64) error {
	c.mu.Lock()
	if err := c.Writer.WriteInt(n); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendSimpleString 发送简单字符串 reply.
func (c *Client) SendSimpleString(s string) error {
	c.mu.Lock()
	if err := c.Writer.WriteValue(resp.RESPValue{Type: '+', Str: s}); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendError 发送错误 reply.
func (c *Client) SendError(msg string) error {
	c.mu.Lock()
	if err := c.Writer.WriteError(msg); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendOK sends +OK\r\n (zero alloc).
func (c *Client) SendOK() error {
	c.mu.Lock()
	if err := c.Writer.WriteOK(); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendNull sends $-1\r\n (zero alloc).
func (c *Client) SendNull() error {
	c.mu.Lock()
	if err := c.Writer.WriteNull(); err != nil {
		c.mu.Unlock()
		return err
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendArray 发送数组回复.
func (c *Client) SendArray(items []resp.RESPValue) error {
	c.mu.Lock()
	if err := c.Writer.WriteArrayHeader(len(items)); err != nil {
		c.mu.Unlock()
		return err
	}
	for i := range items {
		if err := c.Writer.WriteValue(items[i]); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	err := c.Writer.Flush()
	c.mu.Unlock()
	return err
}

// SendNil sends *-1\r\n (zero alloc).
func (c *Client) SendNil() error {
	c.mu.Lock()
	err := c.Writer.WriteNullArray()
	c.mu.Unlock()
	return err
}

// ReplyTypeMismatch sends a WRONGTYPE error.
func (c *Client) ReplyTypeMismatch() error {
	return c.SendError("WRONGTYPE Operation against a key holding the wrong kind of value")
}

// ReplyWrongArgCount sends wrong number of arguments error.
func (c *Client) ReplyWrongArgCount(cmd string) error {
	return c.SendError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd))
}

// String returns a description of the client (for CLIENT LIST).
func (c *Client) String() string {
	addr := ""
	if c.Conn != nil {
		addr = c.Conn.RemoteAddr().String()
	}
	return fmt.Sprintf("id=%d addr=%s db=%d flags=%s qbuf=%d obl=%d oll=%d omem=%d events=r cmd=%s",
		c.ID, addr, c.DBIndex, c.flagString(), 0, 0, 0, 0, c.lastCommandName())
}

func (c *Client) flagString() string {
	var flags []string
	if c.Flags&ClientSlave != 0 {
		flags = append(flags, "S")
	}
	if c.Flags&ClientMaster != 0 {
		flags = append(flags, "M")
	}
	if c.Flags&ClientMulti != 0 {
		flags = append(flags, "x")
	}
	if c.Flags&ClientBlocked != 0 {
		flags = append(flags, "b")
	}
	if c.Flags&ClientPubSub != 0 {
		flags = append(flags, "P")
	}
	if c.Authenticated {
		flags = append(flags, "N")
	}
	if len(flags) == 0 {
		return "N"
	}
	return strings.Join(flags, "")
}

func (c *Client) lastCommandName() string {
	if c.LastCmd != nil {
		return c.LastCmd.Name
	}
	return ""
}

// --- ClientInterface implementation ---

// GetArgv returns the current command arguments.
func (c *Client) GetArgv() []string { return c.Argv }

// SetArgv sets the current command arguments.
func (c *Client) SetArgv(argv []string) { c.Argv = argv }

// GetDB returns the client's current database.
func (c *Client) GetDB() *db.DB { return c.Database }

// SetDB sets the client's current database.
func (c *Client) SetDB(d *db.DB) { c.Database = d }

// GetDBIndex returns the client's current database index.
func (c *Client) GetDBIndex() int { return c.DBIndex }

// SetDBIndex sets the client's current database index.
func (c *Client) SetDBIndex(i int) { c.DBIndex = i }

// GetAuthenticated returns whether the client is authenticated.
func (c *Client) GetAuthenticated() bool { return c.Authenticated }

// SetAuthenticated sets the client's authentication state.
func (c *Client) SetAuthenticated(b bool) { c.Authenticated = b }

// IsResp3 returns true if the client is using RESP3 protocol.
func (c *Client) IsResp3() bool { return c.RespVersion == 3 }

// GetClientName returns the client's name.
func (c *Client) GetClientName() string { return c.Name }

// GetSubscriptions returns the client's channel subscriptions.
func (c *Client) GetSubscriptions() map[string]bool { return c.Subscriptions }

// GetPatternSubs returns the client's pattern subscriptions.
func (c *Client) GetPatternSubs() map[string]bool { return c.PatternSubs }

// ClientList 保存所有已连接客户端.
type ClientList struct {
	clients sync.Map // id -> *Client
}

func NewClientList() *ClientList {
	return &ClientList{}
}

func (cl *ClientList) Add(c *Client) {
	cl.clients.Store(c.ID, c)
}

func (cl *ClientList) Remove(c *Client) {
	cl.clients.Delete(c.ID)
}

func (cl *ClientList) Get(id uint64) *Client {
	v, ok := cl.clients.Load(id)
	if !ok {
		return nil
	}
	return v.(*Client)
}

func (cl *ClientList) Count() int {
	count := 0
	cl.clients.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (cl *ClientList) ForEach(fn func(*Client) bool) {
	cl.clients.Range(func(_, v interface{}) bool {
		return fn(v.(*Client))
	})
}
