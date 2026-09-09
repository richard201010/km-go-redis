// Package resp 实现 Redis 序列化协议（RESP2/RESP3）。
// 本包是 Redis networking.c 中协议解析的 Go 等价物。
//
// RESP 协议类型前缀（与 Redis 8.10 一致）：
//
//	+  简单字符串    -> RESPString
//	-  错误         -> RESPError
//	:  整数         -> RESPInteger
//	$  批量字符串    -> RESPBulkString
//	*  数组         -> RESPArray
//	_  空值         -> RESPNull       (RESP3)
//	#  布尔值       -> RESPBoolean    (RESP3)
//	,  浮点数       -> RESPDouble     (RESP3)
//	(  大数         -> RESPBigNumber  (RESP3)
//	!  块错误       -> RESPBlobError  (RESP3)
//	=  逐字字符串    -> RESPVerbatim   (RESP3)
//	%  映射         -> RESPMap        (RESP3)
//	~  集合         -> RESPSet        (RESP3)
//	|  属性         -> RESPAttribute  (RESP3)
//	>  推送         -> RESPPush       (RESP3)
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// RESP 协议版本常量
const (
	RESP2 = 2 // RESP2 协议
	RESP3 = 3 // RESP3 协议
)

// RESP 类型标签常量，对应协议中的单字节前缀
const (
	TypeSimpleString = '+' // 简单字符串
	TypeError        = '-' // 错误
	TypeInteger      = ':' // 整数
	TypeBulkString   = '$' // 批量字符串
	TypeArray        = '*' // 数组
	TypeNull         = '_' // 空值（RESP3）
	TypeBoolean      = '#' // 布尔值（RESP3）
	TypeDouble       = ',' // 浮点数（RESP3）
)

// RESPValue 表示一个 RESP 协议值，对应 Redis 中的回复类型。
// 与 Redis 的 robj 不同，RESPValue 仅用于协议传输，不用于内部存储。
type RESPValue struct {
	Type    byte                // 类型标签（+, -, :, $, * 等）
	Str     string              // 简单字符串或错误消息
	Num     int64               // 整数值
	Bulk    []byte              // 批量字符串数据
	Array   []RESPValue         // 数组或集合元素
	Double  float64             // 浮点数值（RESP3）
	Boolean bool                // 布尔值（RESP3）
}

// 预编码的常用响应（热路径零分配）。
// 对应 Redis 中的 shared.ok、shared.pong 等共享对象。
var (
	OKBytes       = []byte("+OK\r\n")     // OK 响应的预编码字节
	PONGBytes     = []byte("+PONG\r\n")   // PONG 响应的预编码字节
	QUEUEDBytes   = []byte("+QUEUED\r\n") // QUEUED 响应的预编码字节
	NullBulkBytes = []byte("$-1\r\n")     // 空批量字符串的预编码字节
	NullArrayBytes = []byte("*-1\r\n")    // 空数组的预编码字节
	NullRespBytes  = []byte("_\r\n")      // 空值的预编码字节
	ZeroIntBytes   = []byte(":0\r\n")     // 整数0的预编码字节
	OneIntBytes    = []byte(":1\r\n")     // 整数1的预编码字节
	TwoIntBytes    = []byte(":2\r\n")     // 整数2的预编码字节
	ThreeIntBytes  = []byte(":3\r\n")     // 整数3的预编码字节
)

// 整数缓存表（0-9999），避免重复格式化。
// 对应 Redis 中 shared.integers 的优化思路。
var intBytesCache [10000][]byte

func init() {
	// 预编码0-9999的整数回复
	for i := 0; i < 10000; i++ {
		intBytesCache[i] = []byte(":" + strconv.Itoa(i) + "\r\n")
	}
}

// 整数回复缓冲池
var intBufPool = struct {
	pool interface{ Get() interface{} }
}{
	pool: nil, // 简化实现，直接使用缓存
}

// Writer 是 RESP 协议写入器，对应 Redis 的 writeToClient。
// 使用 bufio.Writer 缓冲减少系统调用，128KB 缓冲区。
type Writer struct {
	w   *bufio.Writer // 底层带缓冲的写入器
	buf []byte        // 临时缓冲区
}

// NewWriter 创建一个新的 RESP 写入器。
// 参数 w: 底层 io.Writer（通常是 net.Conn）
// 返回: 带128KB缓冲的 RESP 写入器
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:   bufio.NewWriterSize(w, 128*1024), // 128KB 缓冲
		buf: make([]byte, 0, 256),             // 256字节临时缓冲
	}
}

// WriteValue 写入一个完整的 RESP 值到线路。
// 对应 Redis 中的 addReply() 系列函数。
// 参数 v: 要写入的 RESP 值
// 返回: 写入错误（如有）
func (w *Writer) WriteValue(v RESPValue) error {
	switch v.Type {
	case TypeSimpleString:
		return w.writeSimpleString(v.Str)
	case TypeError:
		return w.writeError(v.Str)
	case TypeInteger:
		return w.writeInteger(v.Num)
	case TypeBulkString:
		return w.writeBulkString(v.Bulk)
	case TypeArray:
		return w.writeArray(v.Array)
	case TypeNull:
		_, err := w.w.Write(NullRespBytes)
		return err
	case TypeBoolean:
		if v.Boolean {
			_, err := w.w.WriteString("#t\r\n")
			return err
		}
		_, err := w.w.WriteString("#f\r\n")
		return err
	default:
		return fmt.Errorf("不支持的 RESP 类型: %c", v.Type)
	}
}

// writeSimpleString 写入简单字符串（+...）。
// 格式: +<字符串>\r\n
func (w *Writer) writeSimpleString(s string) error {
	w.w.WriteByte('+')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// writeError 写入错误消息（-...）。
// 格式: -<错误消息>\r\n
func (w *Writer) writeError(s string) error {
	w.w.WriteByte('-')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// writeInteger 写入整数（:...）。
// 格式: :<整数>\r\n
// 使用缓存优化0-9999范围内的整数。
func (w *Writer) writeInteger(n int64) error {
	// 快速路径：使用预编码缓存
	if n >= 0 && n < 10000 {
		_, err := w.w.Write(intBytesCache[n])
		return err
	}
	// 通用路径：格式化写入
	w.w.WriteByte(':')
	w.w.WriteString(strconv.FormatInt(n, 10))
	_, err := w.w.WriteString("\r\n")
	return err
}

// writeBulkString 写入批量字符串（$...）。
// 格式: $<长度>\r\n<数据>\r\n
// 空值用 $-1\r\n 表示。
func (w *Writer) writeBulkString(b []byte) error {
	if b == nil {
		_, err := w.w.Write(NullBulkBytes)
		return err
	}
	w.w.WriteByte('$')
	w.w.WriteString(strconv.Itoa(len(b)))
	w.w.WriteString("\r\n")
	w.w.Write(b)
	_, err := w.w.WriteString("\r\n")
	return err
}

// writeArray 写入数组（*...）。
// 格式: *<元素数>\r\n<元素1><元素2>...
// 空值用 *-1\r\n 表示。
func (w *Writer) writeArray(arr []RESPValue) error {
	if arr == nil {
		_, err := w.w.Write(NullArrayBytes)
		return err
	}
	w.w.WriteByte('*')
	w.w.WriteString(strconv.Itoa(len(arr)))
	w.w.WriteString("\r\n")
	for i := range arr {
		if err := w.WriteValue(arr[i]); err != nil {
			return err
		}
	}
	return nil
}

// Flush 将缓冲区数据刷新到底层写入器。
// 对应 Redis 的 writeToClient 中的 write() 调用。
func (w *Writer) Flush() error {
	return w.w.Flush()
}

// WriteOK 直接写入 +OK\r\n（零分配）。
// 对应 Redis 的 addReply(c, shared.ok)。
func (w *Writer) WriteOK() error {
	_, err := w.w.Write(OKBytes)
	return err
}

// WritePONG 直接写入 +PONG\r\n（零分配）。
// 对应 Redis 的 addReply(c, shared.pong)。
func (w *Writer) WritePONG() error {
	_, err := w.w.Write(PONGBytes)
	return err
}

// WriteInt 使用缓存写入整数回复。
// 对应 Redis 的 addReplyLongLong()。
func (w *Writer) WriteInt(n int64) error {
	if n >= 0 && n < 10000 {
		_, err := w.w.Write(intBytesCache[n])
		return err
	}
	return w.writeInteger(n)
}

// WriteBulk 直接写入字节切片作为批量字符串。
func (w *Writer) WriteBulk(b []byte) error {
	return w.writeBulkString(b)
}

// WriteBulkStr 将字符串写入为批量字符串。
// 对应 Redis 的 addReplyBulkBuffer()。
func (w *Writer) WriteBulkStr(s string) error {
	w.w.WriteByte('$')
	w.w.WriteString(strconv.Itoa(len(s)))
	w.w.WriteString("\r\n")
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteNull 写入 $-1\r\n（空批量字符串）。
// 对应 Redis 的 addReply(c, shared.nullbulk)。
func (w *Writer) WriteNull() error {
	_, err := w.w.Write(NullBulkBytes)
	return err
}

// WriteNullArray 写入 *-1\r\n（空数组）。
// 对应 Redis 的 addReply(c, shared.nullarray)。
func (w *Writer) WriteNullArray() error {
	_, err := w.w.Write(NullArrayBytes)
	return err
}

// WriteError 写入错误回复。
// 对应 Redis 的 addReplyError()。
func (w *Writer) WriteError(msg string) error {
	w.w.WriteByte('-')
	w.w.WriteString(msg)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteArrayHeader 写入数组头 *N\r\n。
// 用于分步写入数组：先写头，再逐个写入元素。
func (w *Writer) WriteArrayHeader(n int) error {
	w.w.WriteByte('*')
	w.w.WriteString(strconv.Itoa(n))
	_, err := w.w.WriteString("\r\n")
	return err
}

// --- Reader ---

// Reader 是 RESP 协议读取器，对应 Redis 的 readQueryFromClient。
// 支持 RESP2/RESP3 多批量命令和内联命令。
type Reader struct {
	r *bufio.Reader // 底层带缓冲的读取器
}

// NewReader 创建一个新的 RESP 读取器。
// 参数 r: 底层 io.Reader（通常是 net.Conn）
// 返回: 带128KB缓冲的 RESP 读取器
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 128*1024)}
}

var (
	ErrProtocolError = errors.New("协议错误") // 协议解析错误
)

// ReadCommand 读取一个命令（支持 RESP 和内联协议）。
// 对应 Redis 的 processInputBuffer()，先尝试多批量解析，
// 失败时回退到内联解析。
// 返回: 命令参数切片，错误
func (rd *Reader) ReadCommand() ([]string, error) {
	// 窥探第一个字节判断协议类型
	b, err := rd.r.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] == '*' {
		// RESP 多批量格式
		return rd.readRESPCommand()
	}
	// 内联格式（telnet 风格）
	return rd.readInlineCommand()
}

// readRESPCommand 读取 RESP 多批量命令。
// 格式: *<参数数>\r\n$<长度1>\r\n<参数1>\r\n...
// 对应 Redis 的 processMultibulkBuffer()。
func (rd *Reader) readRESPCommand() ([]string, error) {
	line, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[0] != '*' {
		return nil, ErrProtocolError
	}
	// 解析参数数量
	count := 0
	for i := 1; i < len(line); i++ {
		if line[i] < '0' || line[i] > '9' {
			break
		}
		count = count*10 + int(line[i]-'0')
	}
	if count <= 0 || count > 1024*1024 {
		return nil, ErrProtocolError
	}
	// 逐个读取参数
	args := make([]string, count)
	for i := 0; i < count; i++ {
		// 读取 $<长度>
		line, err := rd.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) < 2 || line[0] != '$' {
			return nil, ErrProtocolError
		}
		bulkLen := 0
		for j := 1; j < len(line); j++ {
			if line[j] < '0' || line[j] > '9' {
				break
			}
			bulkLen = bulkLen*10 + int(line[j]-'0')
		}
		if bulkLen < 0 {
			args[i] = ""
			continue
		}
		// 读取批量数据 + \r\n
		buf := make([]byte, bulkLen+2)
		if _, err := io.ReadFull(rd.r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:bulkLen])
	}
	return args, nil
}

// readInlineCommand 读取内联命令（空格分隔的参数）。
// 格式: <命令> <参数1> <参数2>...\r\n
// 对应 Redis 的 processInlineBuffer()。
func (rd *Reader) readInlineCommand() ([]string, error) {
	line, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	// 去除 \r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if len(line) == 0 {
		return nil, nil
	}
	// 按空格分割参数
	parts := make([]string, 0, 4)
	start := 0
	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == ' ' {
			if i > start {
				parts = append(parts, string(line[start:i]))
			}
			start = i + 1
		}
	}
	return parts, nil
}

// readLine 读取一行（直到 \n）。
// 返回包含 \n 的完整行。
func (rd *Reader) readLine() ([]byte, error) {
	line, err := rd.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return line, nil
}

// ReadValue 读取一个完整的 RESP 值。
// 对应 Redis 的 processMultibulkBuffer 中的值解析。
func (rd *Reader) ReadValue() (RESPValue, error) {
	line, err := rd.readLine()
	if err != nil {
		return RESPValue{}, err
	}
	if len(line) == 0 {
		return RESPValue{}, ErrProtocolError
	}
	prefix := line[0]
	data := line[1:]
	switch prefix {
	case '+':
		return RESPValue{Type: TypeSimpleString, Str: string(data[:len(data)-2])}, nil
	case '-':
		return RESPValue{Type: TypeError, Str: string(data[:len(data)-2])}, nil
	case ':':
		n, _ := strconv.ParseInt(string(data[:len(data)-2]), 10, 64)
		return RESPValue{Type: TypeInteger, Num: n}, nil
	case '$':
		return rd.readBulkString(data)
	case '*':
		return rd.readArray(data)
	case '_':
		return RESPValue{Type: TypeNull}, nil
	default:
		return RESPValue{}, fmt.Errorf("%w: 未知类型前缀 %c", ErrProtocolError, prefix)
	}
}

// readBulkString 读取批量字符串。
func (rd *Reader) readBulkString(prefix []byte) (RESPValue, error) {
	n := 0
	for i := 0; i < len(prefix); i++ {
		if prefix[i] < '0' || prefix[i] > '9' {
			break
		}
		n = n*10 + int(prefix[i]-'0')
	}
	if n == -1 {
		return RESPValue{Type: TypeBulkString, Bulk: nil}, nil
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(rd.r, buf); err != nil {
		return RESPValue{}, err
	}
	return RESPValue{Type: TypeBulkString, Bulk: buf[:n]}, nil
}

// readArray 读取数组。
func (rd *Reader) readArray(prefix []byte) (RESPValue, error) {
	n := 0
	for i := 0; i < len(prefix); i++ {
		if prefix[i] < '0' || prefix[i] > '9' {
			break
		}
		n = n*10 + int(prefix[i]-'0')
	}
	if n == -1 {
		return RESPValue{Type: TypeArray, Array: nil}, nil
	}
	arr := make([]RESPValue, n)
	for i := 0; i < n; i++ {
		val, err := rd.ReadValue()
		if err != nil {
			return RESPValue{}, err
		}
		arr[i] = val
	}
	return RESPValue{Type: TypeArray, Array: arr}, nil
}

// --- 兼容性辅助函数（供命令处理器使用） ---

// 预定义的常用 RESP 值
var (
	NullBulk  = RESPValue{Type: TypeBulkString, Bulk: nil}  // 空批量字符串
	NullArray = RESPValue{Type: TypeArray, Array: nil}      // 空数组
	NullResp  = RESPValue{Type: TypeNull}                   // 空值
	OKReply   = RESPValue{Type: TypeSimpleString, Str: "OK"} // OK 回复
	PONGReply = RESPValue{Type: TypeSimpleString, Str: "PONG"} // PONG 回复
	ZeroReply = RESPValue{Type: TypeInteger, Num: 0}        // 整数0
	OneReply  = RESPValue{Type: TypeInteger, Num: 1}        // 整数1
)

// ErrorReply 创建错误回复。
func ErrorReply(msg string) RESPValue {
	return RESPValue{Type: TypeError, Str: msg}
}

// IntegerReply 创建整数回复。
func IntegerReply(n int64) RESPValue {
	return RESPValue{Type: TypeInteger, Num: n}
}

// BulkStringReply 创建字符串回复。
func BulkStringReply(s string) RESPValue {
	return RESPValue{Type: TypeBulkString, Bulk: []byte(s)}
}

// BulkBytesReply 从字节切片创建回复。
func BulkBytesReply(b []byte) RESPValue {
	return RESPValue{Type: TypeBulkString, Bulk: b}
}

// SimpleStringReply 创建简单字符串回复。
func SimpleStringReply(s string) RESPValue {
	return RESPValue{Type: TypeSimpleString, Str: s}
}

// ArrayReply 创建数组回复。
func ArrayReply(items []RESPValue) RESPValue {
	return RESPValue{Type: TypeArray, Array: items}
}
