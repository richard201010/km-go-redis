// km-go-redis 服务器 - A pure Go reimplementation of Redis 8.10
// 这是主入口点, equivalent to Redis's redis-server binary.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"github.com/km-dev/km-go-redis/internal/config"
	"github.com/km-dev/km-go-redis/internal/server"
)

var (
	version   = "8.10.0"
	gitSHA    = "unknown"
	buildDate = "unknown"
)

func main() {
	// Parse command line arguments (matching redis-server flags)
	cfg := config.DefaultConfig()
	configFile := ""

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-p", "--port":
			if i+1 < len(args) {
				i++
				cfg.Port, _ = strconv.Atoi(args[i])
			}
		case "-b", "--bind":
			if i+1 < len(args) {
				i++
				cfg.Bind = []string{args[i]}
			}
		case "-c", "--config-file":
			if i+1 < len(args) {
				i++
				configFile = args[i]
			}
		case "--daemonize":
			cfg.Daemonize = true
		case "--loglevel":
			if i+1 < len(args) {
				i++
				cfg.LogLevel = args[i]
			}
		case "--databases":
			if i+1 < len(args) {
				i++
				cfg.Databases, _ = strconv.Atoi(args[i])
			}
		case "--maxmemory":
			if i+1 < len(args) {
				i++
				cfg.MaxMemory, _ = strconv.ParseInt(args[i], 10, 64)
			}
		case "--requirepass":
			if i+1 < len(args) {
				i++
				cfg.RequirePass = args[i]
			}
		case "--appendonly":
			cfg.AppendOnly = true
		case "--dir":
			if i+1 < len(args) {
				i++
				cfg.Dir = args[i]
			}
		case "--dbfilename":
			if i+1 < len(args) {
				i++
				cfg.DBFilename = args[i]
			}
		case "--cluster-enabled":
			if i+1 < len(args) {
				i++
				cfg.ClusterEnabled = args[i] == "yes"
			}
		case "--version", "-v":
			fmt.Printf("km-go-redis server v=%s sha=%s\n", version, gitSHA)
			os.Exit(0)
		case "--help", "-h":
			printUsage()
			os.Exit(0)
		default:
			// If it looks like a .conf file path, load it
			if len(args[i]) > 5 && args[i][len(args[i])-5:] == ".conf" {
				configFile = args[i]
			}
		}
	}

	// Load config file if specified
	if configFile != "" {
		if err := cfg.LoadFromFile(configFile); err != nil {
			log.Fatalf("Error loading config file: %v", err)
		}
		cfg.ResolveDir(configFile)
	}

	// Print startup banner
	printBanner(cfg)

	// Create and start server
	srv := server.NewServer(cfg)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down...", sig)
		srv.Shutdown()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func printBanner(cfg *config.Config) {
	pid := os.Getpid()
	fmt.Printf("km-go-redis v%s (Go port) pid=%d port=%d\n", version, pid, cfg.Port)
	fmt.Printf("Running in standalone mode\n")
	log.Printf("Go version: %s", runtime.Version())
	log.Printf("Configuration loaded: port=%d, databases=%d", cfg.Port, cfg.Databases)
	if cfg.RequirePass != "" {
		log.Printf("Password protection enabled")
	}
}

func printUsage() {
	fmt.Print(`km-go-redis server v8.10.0 (Go port)

Usage: km-go-redis-server [/path/to/redis.conf] [options]
       km-go-redis-server - (read config from stdin)

Options:
  -p, --port <port>          TCP listening port (default: 6379)
  -b, --bind <address>       Bind address (default: *)
  -c, --config-file <file>   Config file path
  --daemonize                Run as daemon
  --loglevel <level>         Log level: debug/verbose/notice/warning
  --databases <num>          Number of databases (default: 16)
  --maxmemory <bytes>        Max memory in bytes
  --requirepass <password>   Require password
  --appendonly               Enable AOF persistence
  --dir <dir>                Working directory
  --dbfilename <name>        RDB filename (default: dump.rdb)
  --cluster-enabled <yes|no> Enable cluster mode
  -v, --version              Show version
  -h, --help                 Show this help

Example:
  km-go-redis-server --port 6380 --appendonly
  km-go-redis-server /etc/redis/redis.conf
`)
}
