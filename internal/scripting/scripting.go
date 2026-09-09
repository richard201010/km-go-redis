// Package scripting 实现 Redis Lua 脚本支持。
// 对应 Redis 的 script.c 和 eval.c。
//
// 使用 gopher-lua 实现 Lua 5.1 兼容脚本。
// 支持 EVAL、EVALSHA、SCRIPT LOAD/EXISTS/FLUSH。
// redis.call() 可真实执行 Redis 命令。
package scripting

import (
	"crypto/sha1"
	"fmt"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
)

// CommandExecutor 是命令执行函数类型。
// 参数: 命令名 + 参数列表
// 返回: 结果字符串 + 是否为错误
type CommandExecutor func(cmd string, args []string) (string, bool)

// Script 表示缓存的 Lua 脚本。
type Script struct {
	SHA  string // 脚本的 SHA1 哈希
	Code string // 脚本源代码
}

// Scripting 管理缓存的 Lua 脚本。
// 对应 Redis 的 scriptingState。
type Scripting struct {
	mu       sync.RWMutex
	scripts  map[string]*Script // SHA -> Script
	enabled  bool
	executor CommandExecutor // 命令执行器
}

// NewScripting 创建新的脚本管理器。
// 参数 enabled: 是否启用 Lua 脚本
// 返回: 脚本管理器实例
func NewScripting(enabled bool) *Scripting {
	return &Scripting{
		scripts: make(map[string]*Script),
		enabled: enabled,
	}
}

// SetExecutor 设置命令执行器。
// 当 Lua 脚本调用 redis.call() 时，会通过此执行器执行命令。
func (s *Scripting) SetExecutor(executor CommandExecutor) {
	s.executor = executor
}

// LoadScript 加载并缓存 Lua 脚本。
// 对应 Redis 的 scriptingCreateFunction()。
// 参数 code: Lua 脚本源代码
// 返回: 脚本 SHA1 哈希，错误
func (s *Scripting) LoadScript(code string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sha := fmt.Sprintf("%x", sha1.Sum([]byte(code)))
	if _, exists := s.scripts[sha]; exists {
		return sha, nil
	}
	// 验证脚本语法
	if err := validateScript(code); err != nil {
		return "", fmt.Errorf("ERR Error compiling script: %v", err)
	}
	s.scripts[sha] = &Script{
		SHA:  sha,
		Code: code,
	}
	return sha, nil
}

// ScriptExists 检查脚本是否存在。
// 对应 Redis 的 SCRIPT EXISTS 命令。
// 参数 shas: 要检查的 SHA1 列表
// 返回: 每个 SHA 的存在状态
func (s *Scripting) ScriptExists(shas []string) []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]bool, len(shas))
	for i, sha := range shas {
		_, result[i] = s.scripts[sha]
	}
	return result
}

// ScriptFlush 清除所有缓存的脚本。
// 对应 Redis 的 SCRIPT FLUSH 命令。
func (s *Scripting) ScriptFlush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts = make(map[string]*Script)
}

// EvalScript 执行 Lua 脚本。
// 对应 Redis 的 evalCommand()。
// 参数:
//   - code: Lua 脚本源代码
//   - keys: KEYS 表（键列表）
//   - args: ARGV 表（参数列表）
//
// 返回: 脚本执行结果，错误
func (s *Scripting) EvalScript(code string, keys []string, args []string) (interface{}, error) {
	if !s.enabled {
		return nil, fmt.Errorf("ERR Lua scripting is disabled")
	}
	sha := fmt.Sprintf("%x", sha1.Sum([]byte(code)))
	s.mu.RLock()
	_, exists := s.scripts[sha]
	s.mu.RUnlock()
	if !exists {
		var err error
		sha, err = s.LoadScript(code)
		if err != nil {
			return nil, err
		}
	}
	return s.execScript(code, keys, args)
}

// EvalSHA 通过 SHA 执行缓存的脚本。
// 对应 Redis 的 evalShaCommand()。
// 参数:
//   - sha: 脚本的 SHA1 哈希
//   - keys: KEYS 表
//   - args: ARGV 表
//
// 返回: 脚本执行结果，错误
func (s *Scripting) EvalSHA(sha string, keys []string, args []string) (interface{}, error) {
	if !s.enabled {
		return nil, fmt.Errorf("ERR Lua scripting is disabled")
	}
	s.mu.RLock()
	script, exists := s.scripts[sha]
	s.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("NOSCRIPT No matching script. Use EVAL.")
	}
	return s.execScript(script.Code, keys, args)
}

// execScript 执行 Lua 脚本。
// 创建 Lua 虚拟机，设置 Redis API，执行脚本，返回结果。
func (s *Scripting) execScript(code string, keys, args []string) (interface{}, error) {
	L := lua.NewState()
	defer L.Close()
	// 设置 Redis API
	s.setupRedisAPI(L, keys, args)
	// 执行脚本
	if err := L.DoString(code); err != nil {
		return nil, fmt.Errorf("ERR Error running script: %v", err)
	}
	// 获取返回值
	ret := L.Get(-1)
	L.Pop(1)
	return luaValueToGo(ret), nil
}

// setupRedisAPI 在 Lua 虚拟机中设置 Redis API。
// 对应 Redis 的 scriptingInit()。
// 包括: redis.call(), redis.pcall(), redis.log(), redis.error_reply() 等。
func (s *Scripting) setupRedisAPI(L *lua.LState, keys, args []string) {
	// 创建 redis 表
	redis := L.NewTable()

	// redis.call(cmd, ...) - 执行命令，错误时抛出异常
	L.SetField(redis, "call", L.NewFunction(func(L *lua.LState) int {
		nargs := L.GetTop()
		if nargs < 1 {
			L.ArgError(1, "command expected")
			return 0
		}
		cmd := L.ToString(1)
		cmdArgs := make([]string, nargs-1)
		for i := 2; i <= nargs; i++ {
			cmdArgs[i-2] = L.ToString(i)
		}
		// 通过执行器执行真实命令
		if s.executor != nil {
			result, isErr := s.executor(cmd, cmdArgs)
			if isErr {
				L.RaiseError(result)
				return 0
			}
			L.Push(lua.LString(result))
			return 1
		}
		// 无执行器时返回 OK
		L.Push(lua.LString("OK"))
		return 1
	}))

	// redis.pcall(cmd, ...) - 执行命令，错误时返回状态+错误
	L.SetField(redis, "pcall", L.NewFunction(func(L *lua.LState) int {
		nargs := L.GetTop()
		if nargs < 1 {
			L.Push(lua.LFalse)
			L.Push(lua.LString("command expected"))
			return 2
		}
		cmd := L.ToString(1)
		cmdArgs := make([]string, nargs-1)
		for i := 2; i <= nargs; i++ {
			cmdArgs[i-2] = L.ToString(i)
		}
		if s.executor != nil {
			result, isErr := s.executor(cmd, cmdArgs)
			if isErr {
				L.Push(lua.LFalse)
				L.Push(lua.LString(result))
				return 2
			}
			L.Push(lua.LTrue)
			L.Push(lua.LString(result))
			return 2
		}
		L.Push(lua.LTrue)
		L.Push(lua.LString("OK"))
		return 2
	}))

	// redis.log(level, message) - 日志
	L.SetField(redis, "log", L.NewFunction(func(L *lua.LState) int {
		// 简化实现，忽略日志
		return 0
	}))

	// redis.error_reply(message) - 创建错误回复
	L.SetField(redis, "error_reply", L.NewFunction(func(L *lua.LState) int {
		msg := L.ToString(1)
		L.Push(lua.LString("-ERR " + msg))
		return 1
	}))

	// redis.status_reply(message) - 创建状态回复
	L.SetField(redis, "status_reply", L.NewFunction(func(L *lua.LState) int {
		msg := L.ToString(1)
		L.Push(lua.LString("+" + msg))
		return 1
	}))

	// redis.sha1hex(str) - 计算 SHA1
	L.SetField(redis, "sha1hex", L.NewFunction(func(L *lua.LState) int {
		str := L.ToString(1)
		sha := fmt.Sprintf("%x", sha1.Sum([]byte(str)))
		L.Push(lua.LString(sha))
		return 1
	}))

	// redis.rep(msg) - 标记回复
	L.SetField(redis, "rep", L.NewFunction(func(L *lua.LState) int {
		L.Push(L.Get(1))
		return 1
	}))

	// 日志级别常量
	L.SetField(redis, "LOG_DEBUG", lua.LNumber(0))
	L.SetField(redis, "LOG_VERBOSE", lua.LNumber(1))
	L.SetField(redis, "LOG_NOTICE", lua.LNumber(2))
	L.SetField(redis, "LOG_WARNING", lua.LNumber(3))

	L.SetGlobal("redis", redis)

	// 设置 KEYS 和 ARGV 全局表
	keysTable := L.NewTable()
	for _, k := range keys {
		keysTable.Append(lua.LString(k))
	}
	L.SetGlobal("KEYS", keysTable)

	argvTable := L.NewTable()
	for _, a := range args {
		argvTable.Append(lua.LString(a))
	}
	L.SetGlobal("ARGV", argvTable)
}

// validateScript 验证 Lua 脚本语法。
func validateScript(code string) error {
	L := lua.NewState()
	defer L.Close()
	// 尝试解析脚本
	if err := L.DoString(code); err != nil {
		// 如果是语法错误，返回错误
		if strings.Contains(err.Error(), "syntax error") {
			return err
		}
		// 运行时错误（如访问全局变量）忽略，只检查语法
	}
	return nil
}

// luaValueToGo 将 Lua 值转换为 Go 值。
func luaValueToGo(val lua.LValue) interface{} {
	switch v := val.(type) {
	case *lua.LNilType:
		return nil
	case lua.LBool:
		return bool(v)
	case lua.LNumber:
		return float64(v)
	case lua.LString:
		return string(v)
	case *lua.LTable:
		// 转换表为数组或映射
		arr := make([]interface{}, 0)
		m := make(map[string]interface{})
		isArray := true
		maxN := 0
		v.ForEach(func(key, value lua.LValue) {
			if n, ok := key.(lua.LNumber); ok {
				if int(n) > maxN {
					maxN = int(n)
				}
			} else {
				isArray = false
				m[key.String()] = luaValueToGo(value)
			}
		})
		if isArray && maxN > 0 {
			for i := 1; i <= maxN; i++ {
				val := v.RawGetInt(i)
				arr = append(arr, luaValueToGo(val))
			}
			return arr
		}
		return m
	default:
		return v.String()
	}
}
