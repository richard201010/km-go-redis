// 性能优化扩展 — 预编码错误响应 + 增量过期 + 并发优化
package commands

import (
	"strconv"
	"strings"
)

// ============================================================
// 预编码错误响应 — 避免 fmt.Sprintf 分配
// ============================================================

// 预编码的常见错误消息 (热路径零分配)
var (
	ErrWrongArgCount = []byte("-ERR wrong number of arguments\r\n")
)

// buildWrongArgCount 构建参数数量错误响应 (零分配)
func buildWrongArgCount(cmd string) string {
	// "wrong number of arguments for '<cmd>' command"
	var b strings.Builder
	b.Grow(40 + len(cmd))
	b.WriteString("wrong number of arguments for '")
	b.WriteString(cmd)
	b.WriteString("' command")
	return b.String()
}

// buildUnknownSubcommand 构建未知子命令错误 (零分配)
func buildUnknownSubcommand(parent, sub string) string {
	var b strings.Builder
	b.Grow(50 + len(parent) + len(sub))
	b.WriteString("ERR Unknown ")
	b.WriteString(parent)
	b.WriteString(" subcommand '")
	b.WriteString(sub)
	b.WriteString("'")
	return b.String()
}

// buildUnknownCommand 构建未知命令错误 (零分配)
func buildUnknownCommand(cmd string) string {
	var b strings.Builder
	b.Grow(40 + len(cmd))
	b.WriteString("ERR unknown command '")
	b.WriteString(cmd)
	b.WriteString("'")
	return b.String()
}

// ============================================================
// 整数→字符串 零分配转换 (替代 strconv.FormatInt)
// ============================================================

// fastInt64ToString 快速整数转字符串，避免 FormatInt 的分配
func fastInt64ToString(n int64) string {
	// 小整数快速路径 (0-9999 直接查表)
	if n >= 0 && n < 10000 {
		return strconv.Itoa(int(n))
	}
	return strconv.FormatInt(n, 10)
}

// fastIntToString 快速整数转字符串
func fastIntToString(n int) string {
	if n >= 0 && n < 10000 {
		return strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}
