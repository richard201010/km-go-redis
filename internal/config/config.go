// Package config 实现 Redis 配置管理 management.
// This is the Go equivalent of Redis's config.c, supporting redis.conf parsing
// and runtime configuration changes.
package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// Config 保存所有 Redis 服务器配置 parameters.
// Field names match redis.conf directives (case-insensitive matching).
type Config struct {
	// Network
	Bind            []string `conf:"bind"`
	Port            int      `conf:"port"`
	TLSPort         int      `conf:"tls-port"`
	UnixSocket      string   `conf:"unixsocket"`
	UnixSocketPerm  int      `conf:"unixsocketperm"`
	ProtectedMode   bool     `conf:"protected-mode"`
	TCPBacklog      int      `conf:"tcp-backlog"`
	Timeout         int      `conf:"timeout"`
	TCPKeepAlive    int      `conf:"tcp-keepalive"`

	// General
	Daemonize       bool   `conf:"daemonize"`
	Supervised      string `conf:"supervised"`
	PIDFile         string `conf:"pidfile"`
	LogLevel        string `conf:"loglevel"`
	LogFile         string `conf:"logfile"`
	SyslogEnabled   bool   `conf:"syslog-enabled"`
	SyslogIdent     string `conf:"syslog-ident"`
	Databases       int    `conf:"databases"`
	AlwaysShowLogo  bool   `conf:"always-show-logo"`

	// Security
	RequirePass     string   `conf:"requirepass"`
	RenameCommands  []string `conf:"rename-command"`
	ACLFile         string   `conf:"aclfile"`

	// Limits
	MaxClients          int `conf:"maxclients"`
	MaxMemory           int64 `conf:"maxmemory"`
	MaxMemoryPolicy     string `conf:"maxmemory-policy"`
	MaxMemorySamples    int `conf:"maxmemory-samples"`
	MaxMemoryEvictionTenancy int `conf:"maxmemory-eviction-tenancy"`

	// Snapshotting (RDB)
	SaveParams  []SaveParam
	DBFilename  string `conf:"dbfilename"`
	Dir         string `conf:"dir"`
	RDBChecksum bool   `conf:"rdbchecksum"`
	RDBCompression bool `conf:"rdbcompression"`

	// Append Only File
	AppendOnly       bool   `conf:"appendonly"`
	AppendFilename   string `conf:"appendfilename"`
	AppendFSync      string `conf:"appendfsync"`
	NoAppendFsyncOnRewrite bool `conf:"no-appendfsync-on-rewrite"`
	AutoAOFResizePercentage int `conf:"auto-aof-rewrite-percentage"`
	AutoAOFRewriteMinSize   int64 `conf:"auto-aof-rewrite-min-size"`

	// Replication
	ReplicaOf           string `conf:"replicaof"`
	MasterAuth          string `conf:"masterauth"`
	MasterUser          string `conf:"masteruser"`
	ReplicaServeStaleData bool `conf:"replica-serve-stale-data"`
	ReplicaReadOnly     bool   `conf:"replica-read-only"`
	ReplDisklessSync    bool   `conf:"repl-diskless-sync"`
	ReplDisklessSyncDelay int  `conf:"repl-diskless-sync-delay"`
	ReplPingReplicaPeriod int  `conf:"repl-ping-replica-period"`
	ReplTimeout         int    `conf:"repl-timeout"`
	ReplBacklogSize     int64  `conf:"repl-backlog-size"`
	ReplBacklogTTL      int    `conf:"repl-backlog-ttl"`
	MinReplicasToWrite  int    `conf:"min-replicas-to-write"`
	MinReplicasMaxLag   int    `conf:"min-replicas-max-lag"`

	// Cluster
	ClusterEnabled          bool   `conf:"cluster-enabled"`
	ClusterConfigFile       string `conf:"cluster-config-file"`
	ClusterNodeTimeout      int    `conf:"cluster-node-timeout"`
	ClusterReplicaValidity  int    `conf:"cluster-replica-validity-factor"`
	ClusterMigrationBarrier int    `conf:"cluster-migration-barrier"`
	ClusterRequireFullCoverage bool `conf:"cluster-require-full-coverage"`
	ClusterAllowReadsWhenDown bool `conf:"cluster-allow-reads-when-down"`

	// Lua Scripting
	LuaTimeLimit int `conf:"lua-time-limit"`

	// Slow Log
	SlowLogLogSlowerThan int `conf:"slowlog-log-slower-than"`
	SlowLogMaxLen        int `conf:"slowlog-max-len"`

	// Latency Monitor
	LatencyMonitorThreshold int `conf:"latency-monitor-threshold"`

	// IO Threads
	IOThreads      int  `conf:"io-threads"`
	IOThreadsDoReads bool `conf:"io-threads-do-reads"`

	// Misc
	NotifyKeyspaceEvents string `conf:"notify-keyspace-events"`
	HashMaxZiplistEntries int   `conf:"hash-max-ziplist-entries"`
	HashMaxZiplistValue   int   `conf:"hash-max-ziplist-value"`
	ListMaxZiplistSize    int   `conf:"list-max-ziplist-size"`
	ListCompressDepth     int   `conf:"list-compress-depth"`
	SetMaxIntsetEntries   int   `conf:"set-max-intset-entries"`
	ZsetMaxZiplistEntries int   `conf:"zset-max-ziplist-entries"`
	ZsetMaxZiplistValue   int   `conf:"zset-max-ziplist-value"`
	StreamNodeMaxBytes    int   `conf:"stream-node-max-bytes"`
	StreamNodeMaxEntries  int   `conf:"stream-node-max-entries"`
	Activedefrag          bool  `conf:"activedefrag"`

	// Dynamic (runtime changeable) config
	dynamic map[string]bool
}

// SaveParam represents a save directive (seconds, changes).
type SaveParam struct {
	Seconds int
	Changes int
}

// DefaultConfig returns a config with Redis 8.10 default values.
func DefaultConfig() *Config {
	return &Config{
		Bind:            []string{"*"},
		Port:            6379,
		ProtectedMode:   true,
		TCPBacklog:      511,
		Timeout:         0,
		TCPKeepAlive:    300,
		Daemonize:       false,
		Supervised:      "no",
		LogLevel:        "notice",
		Databases:       16,
		MaxClients:      10000,
		MaxMemoryPolicy: "noeviction",
		MaxMemorySamples: 5,
		SaveParams: []SaveParam{
			{Seconds: 3600, Changes: 1},
			{Seconds: 300, Changes: 100},
			{Seconds: 60, Changes: 10000},
		},
		DBFilename:          "dump.rdb",
		Dir:                 "./",
		RDBChecksum:         true,
		RDBCompression:      true,
		AppendOnly:          false,
		AppendFilename:      "appendonlydir/appendonly.aof",
		AppendFSync:         "everysec",
		AutoAOFResizePercentage: 100,
		AutoAOFRewriteMinSize:   64 * 1024 * 1024,
		ReplicaServeStaleData: true,
		ReplicaReadOnly:       true,
		ReplDisklessSync:      false,
		ReplDisklessSyncDelay: 5,
		ReplPingReplicaPeriod: 10,
		ReplTimeout:           60,
		ReplBacklogSize:       1024 * 1024,
		ReplBacklogTTL:        3600,
		ClusterNodeTimeout:    15000,
		ClusterRequireFullCoverage: true,
		LuaTimeLimit:        5000,
		SlowLogLogSlowerThan: 10000,
		SlowLogMaxLen:        128,
		LatencyMonitorThreshold: 0,
		HashMaxZiplistEntries: 128,
		HashMaxZiplistValue:   64,
		ListMaxZiplistSize:    -2,
		ListCompressDepth:     0,
		SetMaxIntsetEntries:   512,
		ZsetMaxZiplistEntries: 128,
		ZsetMaxZiplistValue:   64,
		StreamNodeMaxBytes:    4096,
		StreamNodeMaxEntries:  100,
		dynamic: make(map[string]bool),
	}
}

// LoadFromFile 从 redis.conf 加载配置 file.
// This matches Redis's loadServerConfigFromString() behavior.
func (c *Config) LoadFromFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("opening config file: %w", err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		lines = append(lines, line)
	}
	return c.LoadFromLines(lines)
}

// LoadFromLines loads configuration from a slice of config lines.
func (c *Config) LoadFromLines(lines []string) error {
	v := reflect.ValueOf(c).Elem()
	t := v.Type()

	for _, line := range lines {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 0 {
			continue
		}
		directive := strings.ToLower(parts[0])
		args := ""
		if len(parts) > 1 {
			args = strings.TrimSpace(parts[1])
		}

		// Handle special directives
		switch directive {
		case "save":
			c.parseSave(args)
			continue
		case "bind":
			c.Bind = strings.Fields(args)
			continue
		}

		// Find matching struct field
		found := false
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("conf")
			if tag == directive {
				if err := setField(v.Field(i), args); err != nil {
					return fmt.Errorf("config '%s': %w", directive, err)
				}
				found = true
				break
			}
		}
		if !found {
			// Log unknown directive but don't error (Redis is lenient)
			fmt.Fprintf(os.Stderr, "Warning: unknown config directive '%s'\n", directive)
		}
	}
	return nil
}

func (c *Config) parseSave(args string) {
	args = strings.TrimSpace(args)
	if args == "" {
		c.SaveParams = nil
		return
	}
	parts := strings.Fields(args)
	if len(parts) == 2 {
		sec, _ := strconv.Atoi(parts[0])
		changes, _ := strconv.Atoi(parts[1])
		if sec > 0 && changes > 0 {
			c.SaveParams = append(c.SaveParams, SaveParam{Seconds: sec, Changes: changes})
		}
	}
}

func setField(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int, reflect.Int64:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer: %s", value)
		}
		field.SetInt(n)
	case reflect.Bool:
		switch strings.ToLower(value) {
		case "yes", "true", "1":
			field.SetBool(true)
		case "no", "false", "0":
			field.SetBool(false)
		default:
			return fmt.Errorf("invalid boolean: %s", value)
		}
	default:
		return fmt.Errorf("unsupported field type: %s", field.Kind())
	}
	return nil
}

// ResolveDir resolves the working directory (relative to config file location).
func (c *Config) ResolveDir(configFile string) {
	if c.Dir == "./" && configFile != "" {
		abs, err := filepath.Abs(configFile)
		if err == nil {
			c.Dir = filepath.Dir(abs)
		}
	}
}

// IsDynamic returns true if the config directive can be changed at runtime.
func IsDynamic(directive string) bool {
	// In Redis, CONFIG SET can change most parameters at runtime.
	// This is a simplified check.
	return true
}
