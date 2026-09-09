// Package server 实现 Redis 主服务器 event loop and connection handling.
// This is the Go equivalent of Redis's server.c — accepting connections, dispatching
// commands, managing pub/sub, active expiration, and graceful shutdown.
package server

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/km-dev/km-go-redis/internal/commands"
	"github.com/km-dev/km-go-redis/internal/config"
	"github.com/km-dev/km-go-redis/internal/db"
	"github.com/km-dev/km-go-redis/internal/networking"
	"github.com/km-dev/km-go-redis/internal/resp"
	"github.com/km-dev/km-go-redis/internal/scripting"
)

// Server 是主 Redis 服务器实例.
// Matches Redis's redisServer struct.
type Server struct {
	Config    *config.Config
	Databases *db.Database
	CmdTable  *commands.CommandTable
	Clients   *networking.ClientList

	Listener net.Listener

	// Pub/Sub state
	pubsubChannels map[string]map[uint64]*networking.Client // channel -> set of clients
	pubsubPatterns []*pubsubPattern                          // compiled pattern subscriptions
	pubsubMu       sync.RWMutex

	// Lifecycle
	ShutdownFlag int32 // atomic: 1 = shutting down
	StartTime    time.Time

	// Lua 脚本引擎
	Scripting *scripting.Scripting

	// Stats
	StatTotalConnections   int64
	StatTotalCommands      int64
	StatRejectedConnections int64
	StatKeyspaceHits       int64
	StatKeyspaceMisses     int64
	StatNetInputBytes      int64
	StatNetOutputBytes     int64

	// Active expiration ticker
	expireTicker *time.Ticker
	expireDone   chan struct{}
}

// pubsubPattern holds a client's compiled pattern subscription.
type pubsubPattern struct {
	Client  *networking.Client
	Pattern string
}

// NewServer 创建并初始化新的 Redis 服务器.
func NewServer(cfg *config.Config) *Server {
	// GC tuning: reduce GC frequency for better throughput
	// Redis doesn't have GC, so we trade memory for speed
	runtime.GOMAXPROCS(runtime.NumCPU())
	// Increase GC threshold to reduce pauses
	debug.SetGCPercent(200) // Default is100, higher = less frequent GC

	databases := db.New(cfg.Databases)

	// 为 Lua 脚本设置命令执行器，使 redis.call() 可真实执行命令
	srv := &Server{
		Config:         cfg,
		Databases:      databases,
		CmdTable:       commands.NewCommandTable(),
		Clients:        networking.NewClientList(),
		StartTime:      time.Now(),
		pubsubChannels: make(map[string]map[uint64]*networking.Client),
		expireDone:     make(chan struct{}),
		Scripting:      scripting.NewScripting(true),
	}
	srv.Scripting.SetExecutor(func(cmd string, args []string) (string, bool) {
		return srv.executeScriptCommand(cmd, args)
	})
	return srv
}

// Start 开始监听客户端连接 and runs background tasks.
// This is the Go equivalent of Redis's aeMain / initServer.
// scriptResponseCollector 是一个内存中的响应收集器，供 Lua 脚本中 redis.call() 使用。
// 对应 Redis 中脚本执行时的 fake client。
type scriptResponseCollector struct {
	replies []string
	lastErr string
}

func (c *scriptResponseCollector) SendReply(v resp.RESPValue) error {
	switch v.Type {
	case '+':
		c.replies = append(c.replies, v.Str)
	case '-':
		c.lastErr = v.Str
		c.replies = append(c.replies, "-"+v.Str)
	case ':':
		c.replies = append(c.replies, fmt.Sprintf("%d", v.Num))
	case '$':
		if v.Bulk == nil {
			c.replies = append(c.replies, "")
		} else {
			c.replies = append(c.replies, string(v.Bulk))
		}
	case '*':
		if v.Array == nil {
			c.replies = append(c.replies, "")
		}
	}
	return nil
}
func (c *scriptResponseCollector) SendBulkString(s string) error {
	c.replies = append(c.replies, s)
	return nil
}
func (c *scriptResponseCollector) SendInteger(n int64) error {
	c.replies = append(c.replies, fmt.Sprintf("%d", n))
	return nil
}
func (c *scriptResponseCollector) SendSimpleString(s string) error {
	c.replies = append(c.replies, s)
	return nil
}
func (c *scriptResponseCollector) SendError(msg string) error {
	c.lastErr = msg
	c.replies = append(c.replies, "-"+msg)
	return nil
}
func (c *scriptResponseCollector) SendOK() error {
	c.replies = append(c.replies, "OK")
	return nil
}
func (c *scriptResponseCollector) SendNull() error {
	c.replies = append(c.replies, "")
	return nil
}
func (c *scriptResponseCollector) SendArray(items []resp.RESPValue) error {
	for _, item := range items {
		c.SendReply(item)
	}
	return nil
}
func (c *scriptResponseCollector) SendNil() error {
	c.replies = append(c.replies, "")
	return nil
}
func (c *scriptResponseCollector) ReplyTypeMismatch() error {
	return c.SendError("WRONGTYPE Operation against a key holding the wrong kind of value")
}
func (c *scriptResponseCollector) ReplyWrongArgCount(cmd string) error {
	return c.SendError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd))
}
func (c *scriptResponseCollector) GetArgv() []string                { return nil }
func (c *scriptResponseCollector) SetArgv([]string)                 {}
func (c *scriptResponseCollector) GetDB() *db.DB                    { return nil }
func (c *scriptResponseCollector) SetDB(d *db.DB)                   {}
func (c *scriptResponseCollector) GetDBIndex() int                  { return 0 }
func (c *scriptResponseCollector) SetDBIndex(i int)                 {}
func (c *scriptResponseCollector) GetAuthenticated() bool           { return true }
func (c *scriptResponseCollector) SetAuthenticated(b bool)          {}
func (c *scriptResponseCollector) IsResp3() bool                    { return false }
func (c *scriptResponseCollector) GetClientName() string            { return "lua-script" }
func (c *scriptResponseCollector) GetSubscriptions() map[string]bool { return nil }
func (c *scriptResponseCollector) GetPatternSubs() map[string]bool  { return nil }

// executeScriptCommand 供 Lua 脚本中 redis.call() 真实执行命令。
// 对应 Redis 的 redisGenericCommand() 在脚本上下文中的调用。
// 创建一个内存中的响应收集器，通过正常命令分发路径执行，捕获结果。
func (s *Server) executeScriptCommand(cmd string, args []string) (string, bool) {
	fullArgs := append([]string{cmd}, args...)
	cmdName := strings.ToLower(cmd)
	command := s.CmdTable.Lookup(cmdName)
	if command == nil {
		return "ERR unknown command '" + cmd + "'", true
	}
	if command.Arity > 0 && len(fullArgs) != command.Arity {
		return fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName), true
	}
	if command.Arity < 0 && len(fullArgs) < -command.Arity {
		return fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName), true
	}
	// 创建内存响应收集器
	collector := &scriptResponseCollector{}
	defaultDB, _ := s.Databases.GetDB(0)
	ctx := &commands.CommandContext{
		Client:  collector,
		Args:    fullArgs,
		Command: command,
		DB:      defaultDB,
		DBIndex: 0,
		Server:  s,
	}
	command.Handler(ctx)
	// 返回最后一个非空回复
	if collector.lastErr != "" {
		return collector.lastErr, true
	}
	if len(collector.replies) > 0 {
		return collector.replies[len(collector.replies)-1], false
	}
	return "OK", false
}

func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.effectiveBind(), s.Config.Port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.Listener = ln

	log.Printf("Redis server started on %s (Go port, version 8.10.0)", addr)
	log.Printf("Ready to accept connections")

	// Start active expiration cycle (runs every 100ms, like Redis)
	s.expireTicker = time.NewTicker(100 * time.Millisecond)
	go s.activeExpireLoop()

	// Accept loop — one goroutine per connection (equivalent to ae event loop)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if atomic.LoadInt32(&s.ShutdownFlag) == 1 {
				return nil // graceful shutdown
			}
			log.Printf("Accept error: %v", err)
			continue
		}
		go s.acceptHandler(conn)
	}
}

// effectiveBind returns the first bind address, or "0.0.0.0" if wildcard.
func (s *Server) effectiveBind() string {
	if len(s.Config.Bind) == 0 || s.Config.Bind[0] == "*" {
		return "0.0.0.0"
	}
	return s.Config.Bind[0]
}

// acceptHandler creates a client for the connection and processes commands.
func (s *Server) acceptHandler(conn net.Conn) {
	atomic.AddInt64(&s.StatTotalConnections, 1)

	client := networking.NewClient(conn, s.Databases.Dbs[0], 0)
	s.Clients.Add(client)
	defer func() {
		s.Clients.Remove(client)
		s.removeClientSubscriptions(client)
		client.Close()
	}()

	// If no password required, mark authenticated immediately
	if s.Config.RequirePass == "" {
		client.Authenticated = true
	}

	s.processClient(client)
}

// processClient 读取并分发 RESP 命令 from the client.
// This is the equivalent of Redis's readQueryFromClient / processInputBuffer.
func (s *Server) processClient(client *networking.Client) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Client %d panicked: %v", client.ID, r)
		}
	}()

	for {
		// Set a read deadline so we don't block forever on dead connections
		if s.Config.Timeout > 0 {
			client.Conn.SetReadDeadline(time.Now().Add(time.Duration(s.Config.Timeout) * time.Second))
		}

		args, err := client.Reader.ReadCommand()
		if err != nil {
			// Connection closed or protocol error
			return
		}

		if len(args) == 0 {
			continue
		}

		atomic.AddInt64(&s.StatTotalCommands, 1)
		client.LastInteraction = time.Now()

		if !s.processCommand(client, args) {
			return // client disconnected or server shutting down
		}
	}
}

// respToArgs converts a RESP array (or inline) value into a string slice.
func (s *Server) respToArgs(val resp.RESPValue) []string {
	switch val.Type {
	case resp.TypeArray:
		args := make([]string, len(val.Array))
		for i, item := range val.Array {
			if item.Type == resp.TypeBulkString && item.Bulk != nil {
				args[i] = string(item.Bulk)
			} else if item.Type == resp.TypeSimpleString {
				args[i] = item.Str
			}
		}
		return args
	case resp.TypeBulkString:
		if val.Bulk != nil {
			return strings.Fields(string(val.Bulk))
		}
	case resp.TypeSimpleString:
		return strings.Fields(val.Str)
	}
	return nil
}

// processCommand 实现 Redis's processCommand flow:
// AUTH check → command lookup → MULTI/EXEC queueing → execute.
// Returns false if the client should be disconnected.
func (s *Server) processCommand(client *networking.Client, args []string) bool {
	cmdName := strings.ToLower(args[0])

	// AUTH is always allowed (even when not authenticated)
	if cmdName != "auth" && cmdName != "hello" && cmdName != "quit" {
		if !client.Authenticated {
			client.SendError("NOAUTH Authentication required.")
			return true
		}
	}

	// QUIT is always allowed
	if cmdName == "quit" {
		client.SendOK()
		return false
	}

	// Look up command in table
	cmd := s.CmdTable.Lookup(cmdName)
	if cmd == nil {
		client.SendError(fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], strings.Join(args[1:], " ")))
		return true
	}

	// Arity check
	if cmd.Arity > 0 && len(args) != cmd.Arity {
		client.ReplyWrongArgCount(cmdName)
		return true
	}
	if cmd.Arity < 0 && len(args) < -cmd.Arity {
		client.ReplyWrongArgCount(cmdName)
		return true
	}

	// If client is in MULTI mode, queue the command (except MULTI/EXEC/DISCARD/ WATCH/UNWATCH)
	if client.MultiState != nil && client.MultiState.Active {
		if cmdName != "multi" && cmdName != "exec" && cmdName != "discard" &&
			cmdName != "watch" && cmdName != "unwatch" {
			client.MultiState.Commands = append(client.MultiState.Commands, networking.PendingCommand{
				Cmd:  cmd,
				Args: args,
			})
			client.SendSimpleString("QUEUED")
			return true
		}
	}

	// Handle SELECT to update the client's database pointer
	if cmdName == "select" {
		s.handleSelect(client, args, cmd)
		return true
	}

	// Handle SHUTDOWN
	if cmdName == "shutdown" {
		s.handleShutdown(client)
		return false
	}

	// Execute the command
	ctx := &commands.CommandContext{
		Client:  client,
		Args:    args,
		Command: cmd,
		DB:      client.Database,
		DBIndex: client.DBIndex,
		Server:  s,
	}

	cmd.Handler(ctx)
	client.LastCmd = cmd

	// If command was MULTI, mark the client as in transaction
	if cmdName == "multi" {
		client.Flags |= networking.ClientMulti
		client.MultiState = &networking.MultiState{Active: true}
	}

	// If command was EXEC/DISCARD, clear transaction state
	if cmdName == "exec" || cmdName == "discard" {
		client.Flags &^= networking.ClientMulti
		client.MultiState = nil
	}

	return true
}

// handleSelect 切换客户端's database.
func (s *Server) handleSelect(client *networking.Client, args []string, cmd *commands.Command) {
	ctx := &commands.CommandContext{
		Client:  client,
		Args:    args,
		Command: cmd,
		DB:      client.Database,
		DBIndex: client.DBIndex,
		Server:  s,
	}
	cmd.Handler(ctx)

	// After handler, update the actual DB pointer if the index changed
	if client.DBIndex >= 0 && client.DBIndex < s.Databases.Count {
		newDB, err := s.Databases.GetDB(client.DBIndex)
		if err == nil {
			client.Database = newDB
		}
	}
}

// handleShutdown performs graceful shutdown.
func (s *Server) handleShutdown(client *networking.Client) {
	client.SendOK()
	atomic.StoreInt32(&s.ShutdownFlag, 1)
	if s.Listener != nil {
		s.Listener.Close()
	}
	if s.expireTicker != nil {
		s.expireTicker.Stop()
		close(s.expireDone)
	}
	// Close all client connections
	s.Clients.ForEach(func(c *networking.Client) bool {
		c.Close()
		return true
	})
	log.Printf("Server shutdown initiated")
}

// Shutdown performs a graceful server shutdown.
func (s *Server) Shutdown() {
	s.handleShutdown(nil)
}

// --- Pub/Sub (ServerInterface implementation) ---

// BroadcastPubSub sends a message to all subscribers of a channel.
// Returns the number of clients that received the message.
func (s *Server) BroadcastPubSub(channel string, message string) int {
	s.pubsubMu.RLock()
	defer s.pubsubMu.RUnlock()

	receivers := 0

	// Send to direct subscribers
	if subs, ok := s.pubsubChannels[channel]; ok {
		for _, client := range subs {
			// Message format: ["message", channel, message]
			err := client.SendArray([]resp.RESPValue{
				resp.BulkStringReply("message"),
				resp.BulkStringReply(channel),
				resp.BulkStringReply(message),
			})
			if err == nil {
				receivers++
			}
		}
	}

	// Send to pattern subscribers
	for _, ps := range s.pubsubPatterns {
		if matchPattern(ps.Pattern, channel) {
			err := ps.Client.SendArray([]resp.RESPValue{
				resp.BulkStringReply("pmessage"),
				resp.BulkStringReply(ps.Pattern),
				resp.BulkStringReply(channel),
				resp.BulkStringReply(message),
			})
			if err == nil {
				receivers++
			}
		}
	}

	return receivers
}

// Subscribe subscribes a client to a channel.
func (s *Server) Subscribe(client commands.ClientInterface, channel string) {
	c := s.unwrapClient(client)
	if c == nil {
		return
	}

	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	if s.pubsubChannels[channel] == nil {
		s.pubsubChannels[channel] = make(map[uint64]*networking.Client)
	}
	s.pubsubChannels[channel][c.ID] = c
	c.Flags |= networking.ClientPubSub

	// Send confirmation
	c.SendArray([]resp.RESPValue{
		resp.BulkStringReply("subscribe"),
		resp.BulkStringReply(channel),
		resp.IntegerReply(int64(len(c.Subscriptions))),
	})
	c.Subscriptions[channel] = true
}

// Unsubscribe unsubscribes a client from a channel.
func (s *Server) Unsubscribe(client commands.ClientInterface, channel string) {
	c := s.unwrapClient(client)
	if c == nil {
		return
	}

	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	if channel != "" {
		if subs, ok := s.pubsubChannels[channel]; ok {
			delete(subs, c.ID)
			if len(subs) == 0 {
				delete(s.pubsubChannels, channel)
			}
		}
		delete(c.Subscriptions, channel)
	} else {
		// Unsubscribe from all
		for ch := range c.Subscriptions {
			if subs, ok := s.pubsubChannels[ch]; ok {
				delete(subs, c.ID)
				if len(subs) == 0 {
					delete(s.pubsubChannels, ch)
				}
			}
		}
		c.Subscriptions = make(map[string]bool)
	}

	if len(c.Subscriptions) == 0 && len(c.PatternSubs) == 0 {
		c.Flags &^= networking.ClientPubSub
	}

	c.SendArray([]resp.RESPValue{
		resp.BulkStringReply("unsubscribe"),
		resp.BulkStringReply(channel),
		resp.IntegerReply(int64(len(c.Subscriptions))),
	})
}

// PSubscribe subscribes a client to a pattern.
func (s *Server) PSubscribe(client commands.ClientInterface, pattern string) {
	c := s.unwrapClient(client)
	if c == nil {
		return
	}

	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	s.pubsubPatterns = append(s.pubsubPatterns, &pubsubPattern{Client: c, Pattern: pattern})
	c.PatternSubs[pattern] = true
	c.Flags |= networking.ClientPubSub

	c.SendArray([]resp.RESPValue{
		resp.BulkStringReply("psubscribe"),
		resp.BulkStringReply(pattern),
		resp.IntegerReply(int64(len(c.PatternSubs))),
	})
}

// PUnsubscribe unsubscribes a client from a pattern.
func (s *Server) PUnsubscribe(client commands.ClientInterface, pattern string) {
	c := s.unwrapClient(client)
	if c == nil {
		return
	}

	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	if pattern != "" {
		// Remove specific pattern
		for i := len(s.pubsubPatterns) - 1; i >= 0; i-- {
			if s.pubsubPatterns[i].Client.ID == c.ID && s.pubsubPatterns[i].Pattern == pattern {
				s.pubsubPatterns = append(s.pubsubPatterns[:i], s.pubsubPatterns[i+1:]...)
			}
		}
		delete(c.PatternSubs, pattern)
	} else {
		// Unsubscribe from all patterns
		for i := len(s.pubsubPatterns) - 1; i >= 0; i-- {
			if s.pubsubPatterns[i].Client.ID == c.ID {
				s.pubsubPatterns = append(s.pubsubPatterns[:i], s.pubsubPatterns[i+1:]...)
			}
		}
		c.PatternSubs = make(map[string]bool)
	}

	if len(c.Subscriptions) == 0 && len(c.PatternSubs) == 0 {
		c.Flags &^= networking.ClientPubSub
	}

	c.SendArray([]resp.RESPValue{
		resp.BulkStringReply("punsubscribe"),
		resp.BulkStringReply(pattern),
		resp.IntegerReply(int64(len(c.PatternSubs))),
	})
}

// removeClientSubscriptions removes all subscriptions for a disconnected client.
func (s *Server) removeClientSubscriptions(client *networking.Client) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	for ch := range client.Subscriptions {
		if subs, ok := s.pubsubChannels[ch]; ok {
			delete(subs, client.ID)
			if len(subs) == 0 {
				delete(s.pubsubChannels, ch)
			}
		}
	}

	for i := len(s.pubsubPatterns) - 1; i >= 0; i-- {
		if s.pubsubPatterns[i].Client.ID == client.ID {
			s.pubsubPatterns = append(s.pubsubPatterns[:i], s.pubsubPatterns[i+1:]...)
		}
	}
}

// unwrapClient extracts the concrete *networking.Client from a ClientInterface.
func (s *Server) unwrapClient(ci commands.ClientInterface) *networking.Client {
	if c, ok := ci.(*networking.Client); ok {
		return c
	}
	return nil
}

// --- Active Expiration ---

// activeExpireLoop periodically runs the active expiration cycle on all databases.
func (s *Server) activeExpireLoop() {
	for {
		select {
		case <-s.expireTicker.C:
			for i := 0; i < s.Databases.Count; i++ {
				s.Databases.Dbs[i].ActiveExpireCycle(20)
			}
		case <-s.expireDone:
			return
		}
	}
}

// --- Info helpers ---

// GetUptimeSeconds returns the server uptime in seconds.
func (s *Server) GetUptimeSeconds() int64 {
	return int64(time.Since(s.StartTime).Seconds())
}

// GetConnectedClients returns the number of connected clients.
func (s *Server) GetConnectedClients() int {
	return s.Clients.Count()
}

// GetTotalCommands returns the total commands processed.
func (s *Server) GetTotalCommands() int64 {
	return atomic.LoadInt64(&s.StatTotalCommands)
}

// GetClientList returns a CLIENT LIST formatted string.
func (s *Server) GetClientList() string {
	var lines []string
	s.Clients.ForEach(func(c *networking.Client) bool {
		lines = append(lines, c.String())
		return true
	})
	return strings.Join(lines, "\n")
}

// matchPattern matches a channel against a glob pattern.
func matchPattern(pattern, channel string) bool {
	pi, ci := 0, 0
	pLen, cLen := len(pattern), len(channel)
	for pi < pLen || ci < cLen {
		if pi < pLen {
			switch pattern[pi] {
			case '*':
				pi++
				if pi == pLen {
					return true
				}
				for i := ci; i <= cLen; i++ {
					if matchPattern(pattern[pi:], channel[i:]) {
						return true
					}
				}
				return false
			case '?':
				if ci >= cLen {
					return false
				}
				pi++
				ci++
				continue
			case '\\':
				pi++
				if pi >= pLen {
					return false
				}
				fallthrough
			default:
				if ci >= cLen || pattern[pi] != channel[ci] {
					return false
				}
				pi++
				ci++
				continue
			}
		}
		return false
	}
	return pi == pLen && ci == cLen
}

// FlushAllDBs flushes all databases.
func (s *Server) FlushAllDBs() {
	s.Databases.FlushAll()
}

// --- Lua 脚本方法（实现 ServerInterface） ---

// EvalScript 执行 Lua 脚本（供命令处理器调用）。
func (s *Server) EvalScript(code string, keys []string, args []string) (interface{}, error) {
	return s.Scripting.EvalScript(code, keys, args)
}

// EvalSHA 通过 SHA1 执行缓存脚本。
func (s *Server) EvalSHA(sha string, keys []string, args []string) (interface{}, error) {
	return s.Scripting.EvalSHA(sha, keys, args)
}

// LoadScript 加载并缓存 Lua 脚本。
func (s *Server) LoadScript(code string) (string, error) {
	return s.Scripting.LoadScript(code)
}

// ScriptExists 检查脚本是否存在。
func (s *Server) ScriptExists(shas []string) []bool {
	return s.Scripting.ScriptExists(shas)
}

// ScriptFlush 清除所有缓存脚本。
func (s *Server) ScriptFlush() {
	s.Scripting.ScriptFlush()
}
