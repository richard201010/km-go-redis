// Package integration 提供集成测试 for km-go-redis.
// 这些测试验证行为一致性 with Redis 8.10 by testing
// against the running server on the configured port.
package integration

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

const testPort = 7379

// TestClient wraps a RESP connection for testing.
type TestClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func NewTestClient(port int) (*TestClient, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &TestClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

func (tc *TestClient) Close() {
	tc.conn.Close()
}

// SendCommand sends a RESP command and returns the response.
func (tc *TestClient) SendCommand(args ...string) (string, error) {
	// Build RESP array
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := tc.conn.Write([]byte(sb.String()))
	if err != nil {
		return "", err
	}
	return tc.readResponse()
}

func (tc *TestClient) readResponse() (string, error) {
	line, err := tc.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}
	prefix := line[0]
	data := line[1:]
	switch prefix {
	case '+':
		return data, nil
	case '-':
		return "", fmt.Errorf("%s", data)
	case ':':
		return data, nil
	case '$':
		// Bulk string
		n := 0
		fmt.Sscanf(data, "%d", &n)
		if n == -1 {
			return "(nil)", nil
		}
		buf := make([]byte, n+2)
		_, err := tc.reader.Read(buf)
		if err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*':
		// Array
		count := 0
		fmt.Sscanf(data, "%d", &count)
		if count == -1 {
			return "(nil)", nil
		}
		items := make([]string, count)
		for i := 0; i < count; i++ {
			resp, err := tc.readResponse()
			if err != nil {
				return "", err
			}
			items[i] = resp
		}
		return strings.Join(items, "\n"), nil
	default:
		return line, nil
	}
}

// --- String Command Tests ---

func TestStringCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	// Clean up
	c.SendCommand("FLUSHDB")
	// SET/GET
	resp, err := c.SendCommand("SET", "testkey", "testvalue")
	if err != nil || resp != "OK" {
		t.Errorf("SET failed: %v %v", resp, err)
	}
	resp, err = c.SendCommand("GET", "testkey")
	if err != nil || resp != "testvalue" {
		t.Errorf("GET failed: %v %v", resp, err)
	}
	// SET NX
	resp, _ = c.SendCommand("SET", "testkey", "newval", "NX")
	if resp != "(nil)" {
		t.Errorf("SET NX should fail on existing key: %v", resp)
	}
	c.SendCommand("DEL", "nxkey")
	resp, _ = c.SendCommand("SET", "nxkey", "val", "NX")
	if resp != "OK" {
		t.Errorf("SET NX should succeed on new key: %v", resp)
	}
	// INCR
	c.SendCommand("SET", "counter", "0")
	resp, _ = c.SendCommand("INCR", "counter")
	if resp != "1" {
		t.Errorf("INCR failed: %v", resp)
	}
	resp, _ = c.SendCommand("INCRBY", "counter", "10")
	if resp != "11" {
		t.Errorf("INCRBY failed: %v", resp)
	}
	// MSET/MGET
	c.SendCommand("MSET", "k1", "v1", "k2", "v2")
	resp, _ = c.SendCommand("MGET", "k1", "k2", "k3")
	if resp != "v1\nv2\n(nil)" {
		t.Errorf("MGET failed: %v", resp)
	}
	// APPEND
	c.SendCommand("SET", "appendkey", "hello")
	resp, _ = c.SendCommand("APPEND", "appendkey", " world")
	if resp != "11" {
		t.Errorf("APPEND failed: %v", resp)
	}
	// STRLEN
	resp, _ = c.SendCommand("STRLEN", "appendkey")
	if resp != "11" {
		t.Errorf("STRLEN failed: %v", resp)
	}
	// GETRANGE
	resp, _ = c.SendCommand("GETRANGE", "appendkey", "0", "4")
	if resp != "hello" {
		t.Errorf("GETRANGE failed: %v", resp)
	}
	// SETEX
	c.SendCommand("SETEX", "ttlkey", "100", "ttlval")
	resp, _ = c.SendCommand("TTL", "ttlkey")
	if resp != "99" && resp != "100" {
		// Allow for timing differences
		t.Logf("TTL: %v (may vary due to timing)", resp)
	}
	// Cleanup
	c.SendCommand("FLUSHDB")
}

// --- List Command Tests ---

func TestListCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	// RPUSH
	resp, _ := c.SendCommand("RPUSH", "mylist", "a", "b", "c")
	if resp != "3" {
		t.Errorf("RPUSH failed: %v", resp)
	}
	// LLEN
	resp, _ = c.SendCommand("LLEN", "mylist")
	if resp != "3" {
		t.Errorf("LLEN failed: %v", resp)
	}
	// LPUSH
	resp, _ = c.SendCommand("LPUSH", "mylist", "z")
	if resp != "4" {
		t.Errorf("LPUSH failed: %v", resp)
	}
	// LPOP
	resp, _ = c.SendCommand("LPOP", "mylist")
	if resp != "z" {
		t.Errorf("LPOP failed: %v", resp)
	}
	// RPOP
	resp, _ = c.SendCommand("RPOP", "mylist")
	if resp != "c" {
		t.Errorf("RPOP failed: %v", resp)
	}
	// LRANGE
	resp, _ = c.SendCommand("LRANGE", "mylist", "0", "-1")
	if resp != "a\nb" {
		t.Errorf("LRANGE failed: %v", resp)
	}
	// LINDEX
	resp, _ = c.SendCommand("LINDEX", "mylist", "0")
	if resp != "a" {
		t.Errorf("LINDEX failed: %v", resp)
	}
	// LSET
	c.SendCommand("LSET", "mylist", "0", "X")
	resp, _ = c.SendCommand("LINDEX", "mylist", "0")
	if resp != "X" {
		t.Errorf("LSET failed: %v", resp)
	}
	// LREM
	c.SendCommand("RPUSH", "remlist", "a", "b", "a", "c", "a")
	resp, _ = c.SendCommand("LREM", "remlist", "2", "a")
	if resp != "2" {
		t.Errorf("LREM failed: %v", resp)
	}
	c.SendCommand("FLUSHDB")
}

// --- Hash Command Tests ---

func TestHashCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	// HSET
	resp, _ := c.SendCommand("HSET", "myhash", "name", "Alice", "age", "30")
	if resp != "2" {
		t.Errorf("HSET failed: %v", resp)
	}
	// HGET
	resp, _ = c.SendCommand("HGET", "myhash", "name")
	if resp != "Alice" {
		t.Errorf("HGET failed: %v", resp)
	}
	// HLEN
	resp, _ = c.SendCommand("HLEN", "myhash")
	if resp != "2" {
		t.Errorf("HLEN failed: %v", resp)
	}
	// HEXISTS
	resp, _ = c.SendCommand("HEXISTS", "myhash", "name")
	if resp != "1" {
		t.Errorf("HEXISTS failed: %v", resp)
	}
	resp, _ = c.SendCommand("HEXISTS", "myhash", "email")
	if resp != "0" {
		t.Errorf("HEXISTS should return 0: %v", resp)
	}
	// HINCRBY
	resp, _ = c.SendCommand("HINCRBY", "myhash", "age", "5")
	if resp != "35" {
		t.Errorf("HINCRBY failed: %v", resp)
	}
	// HSETNX
	resp, _ = c.SendCommand("HSETNX", "myhash", "name", "Bob")
	if resp != "0" {
		t.Errorf("HSETNX should fail on existing field: %v", resp)
	}
	// HDEL
	resp, _ = c.SendCommand("HDEL", "myhash", "name")
	if resp != "1" {
		t.Errorf("HDEL failed: %v", resp)
	}
	c.SendCommand("FLUSHDB")
}

// --- Set Command Tests ---

func TestSetCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	// SADD
	resp, _ := c.SendCommand("SADD", "myset", "a", "b", "c")
	if resp != "3" {
		t.Errorf("SADD failed: %v", resp)
	}
	// SCARD
	resp, _ = c.SendCommand("SCARD", "myset")
	if resp != "3" {
		t.Errorf("SCARD failed: %v", resp)
	}
	// SISMEMBER
	resp, _ = c.SendCommand("SISMEMBER", "myset", "a")
	if resp != "1" {
		t.Errorf("SISMEMBER failed: %v", resp)
	}
	resp, _ = c.SendCommand("SISMEMBER", "myset", "z")
	if resp != "0" {
		t.Errorf("SISMEMBER should return 0: %v", resp)
	}
	// SREM
	resp, _ = c.SendCommand("SREM", "myset", "a")
	if resp != "1" {
		t.Errorf("SREM failed: %v", resp)
	}
	c.SendCommand("FLUSHDB")
}

// --- ZSet Command Tests ---

func TestZSetCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	// ZADD
	resp, _ := c.SendCommand("ZADD", "leaderboard", "100", "alice", "200", "bob", "150", "charlie")
	if resp != "3" {
		t.Errorf("ZADD failed: %v", resp)
	}
	// ZCARD
	resp, _ = c.SendCommand("ZCARD", "leaderboard")
	if resp != "3" {
		t.Errorf("ZCARD failed: %v", resp)
	}
	// ZSCORE
	resp, _ = c.SendCommand("ZSCORE", "leaderboard", "alice")
	if resp != "100" {
		t.Errorf("ZSCORE failed: %v", resp)
	}
	// ZRANK
	resp, _ = c.SendCommand("ZRANK", "leaderboard", "alice")
	if resp != "0" {
		t.Errorf("ZRANK failed: %v", resp)
	}
	// ZINCRBY
	resp, _ = c.SendCommand("ZINCRBY", "leaderboard", "50", "alice")
	if resp != "150" {
		t.Errorf("ZINCRBY failed: %v", resp)
	}
	// ZCOUNT
	resp, _ = c.SendCommand("ZCOUNT", "leaderboard", "100", "200")
	if resp != "3" {
		t.Errorf("ZCOUNT failed: %v", resp)
	}
	c.SendCommand("FLUSHDB")
}

// --- Key Command Tests ---

func TestKeyCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	// SET and check
	c.SendCommand("SET", "key1", "val1")
	resp, _ := c.SendCommand("EXISTS", "key1")
	if resp != "1" {
		t.Errorf("EXISTS failed: %v", resp)
	}
	// TYPE
	resp, _ = c.SendCommand("TYPE", "key1")
	if resp != "string" {
		t.Errorf("TYPE failed: %v", resp)
	}
	// DEL
	resp, _ = c.SendCommand("DEL", "key1")
	if resp != "1" {
		t.Errorf("DEL failed: %v", resp)
	}
	// EXPIRE/PERSIST
	c.SendCommand("SET", "expkey", "val")
	c.SendCommand("EXPIRE", "expkey", "100")
	resp, _ = c.SendCommand("PERSIST", "expkey")
	if resp != "1" {
		t.Errorf("PERSIST failed: %v", resp)
	}
	resp, _ = c.SendCommand("TTL", "expkey")
	if resp != "-1" {
		t.Errorf("TTL after PERSIST failed: %v", resp)
	}
	// RENAME
	c.SendCommand("SET", "oldname", "val")
	resp, _ = c.SendCommand("RENAME", "oldname", "newname")
	if resp != "OK" {
		t.Errorf("RENAME failed: %v", resp)
	}
	resp, _ = c.SendCommand("GET", "newname")
	if resp != "val" {
		t.Errorf("GET after RENAME failed: %v", resp)
	}
	c.SendCommand("FLUSHDB")
}

// --- Server Command Tests ---

func TestServerCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	// PING
	resp, _ := c.SendCommand("PING")
	if resp != "PONG" {
		t.Errorf("PING failed: %v", resp)
	}
	// ECHO
	resp, _ = c.SendCommand("ECHO", "hello")
	if resp != "hello" {
		t.Errorf("ECHO failed: %v", resp)
	}
	// SELECT
	resp, _ = c.SendCommand("SELECT", "0")
	if resp != "OK" {
		t.Errorf("SELECT failed: %v", resp)
	}
	// DBSIZE
	resp, _ = c.SendCommand("DBSIZE")
	if resp == "" {
		t.Errorf("DBSIZE returned empty")
	}
	// INFO
	resp, _ = c.SendCommand("INFO", "server")
	if !strings.Contains(resp, "redis_version") {
		t.Errorf("INFO server failed: %v", resp)
	}
	// CONFIG GET
	resp, _ = c.SendCommand("CONFIG", "GET", "databases")
	if resp == "" || resp == "(nil)" {
		t.Errorf("CONFIG GET failed: %v", resp)
	}
}

// --- Stream Command Tests ---

func TestStreamCommands(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	// XADD
	resp, err := c.SendCommand("XADD", "mystream", "*", "name", "Alice", "action", "login")
	if err != nil || resp == "" {
		t.Errorf("XADD failed: %v %v", resp, err)
	}
	resp, _ = c.SendCommand("XADD", "mystream", "*", "name", "Bob", "action", "logout")
	if resp == "" {
		t.Errorf("XADD 2 failed: %v", resp)
	}
	// XLEN
	resp, _ = c.SendCommand("XLEN", "mystream")
	if resp != "2" {
		t.Errorf("XLEN failed: %v", resp)
	}
	// XRANGE
	resp, _ = c.SendCommand("XRANGE", "mystream", "-", "+")
	if resp == "(nil)" || resp == "" {
		t.Errorf("XRANGE failed: %v", resp)
	}
	c.SendCommand("FLUSHDB")
}

// --- Pub/Sub Tests ---

func TestPubSub(t *testing.T) {
	// Pub/Sub requires two connections
	pub, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect publisher: %v", err)
	}
	defer pub.Close()
	sub, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}
	defer sub.Close()
	// Subscribe
	_, err = sub.SendCommand("SUBSCRIBE", "testchannel")
	if err != nil {
		t.Errorf("SUBSCRIBE failed: %v", err)
	}
	// Small delay to ensure subscription is active
	time.Sleep(50 * time.Millisecond)
	// Publish
	resp, _ := pub.SendCommand("PUBLISH", "testchannel", "hello")
	if resp != "1" {
		t.Errorf("PUBLISH failed: %v (expected 1 subscriber)", resp)
	}
}

// --- Multi-DB Tests ---

func TestMultiDB(t *testing.T) {
	c, err := NewTestClient(testPort)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()
	c.SendCommand("FLUSHDB")
	c.SendCommand("SELECT", "0")
	c.SendCommand("SET", "dbtest", "val0")
	c.SendCommand("SELECT", "1")
	c.SendCommand("SET", "dbtest", "val1")
	// Check isolation
	resp, _ := c.SendCommand("GET", "dbtest")
	if resp != "val1" {
		t.Errorf("DB1 should have val1: %v", resp)
	}
	c.SendCommand("SELECT", "0")
	resp, _ = c.SendCommand("GET", "dbtest")
	if resp != "val0" {
		t.Errorf("DB0 should have val0: %v", resp)
	}
	c.SendCommand("SELECT", "0")
	c.SendCommand("FLUSHDB")
}
