// Package persistence 实现 RDB 和 AOF 持久化 for km-go-redis.
// This is the Go equivalent of Redis's rdb.c and aof.c.
//
// RDB Format (matching Redis 8.10):
// - Header: "REDIS" + version (9 bytes)
// - Database selector: FE + db index
// - Key-value pairs: type byte + key + value
// - EOF marker: FF
// - CRC64 checksum
//
// AOF Format:
// - RESP protocol commands (same as wire protocol)
// - FSYNC policies: always, everysec, no
package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/km-dev/km-go-redis/internal/db"
	"github.com/km-dev/km-go-redis/internal/object"
)

// RDB opcodes (matching Redis's rdb.h constants).
const (
	RDBOpcodeModuleAux    = 247
	RDBOpcodeIdle         = 248
	RDBOpcodeFreq         = 249
	RDBOpcodeAux          = 250
	RDBOpcodeResizeDB     = 251
	RDBOpcodeExpireTimeMs = 252
	RDBOpcodeExpireTime   = 253
	RDBOpcodeSelectDB     = 254
	RDBOpcodeEOF          = 255
)

// RDB type constants for value types.
const (
	RDBTypeString       = 0
	RDBTypeList         = 1
	RDBTypeSet          = 2
	RDBTypeZSet         = 3
	RDBTypeHash         = 4
	RDBTypeZSet2        = 5
	RDBTypeModule       = 6
	RDBTypeModule2       = 7
	RDBTypeHashZipmap   = 9
	RDBTypeListZiplist  = 10
	RDBTypeSetIntset    = 11
	RDBTypeZSetZiplist  = 12
	RDBTypeHashZiplist  = 13
	RDBTypeListQuicklist = 14
	RDBTypeStreamListpacks = 15
	RDBTypeHashListpack = 16
	RDBTypeZSetListpack = 17
	RDBTypeListQuicklist2 = 18
	RDBTypeStreamListpacks2 = 19
	RDBTypeSetListpack  = 20
	RDBTypeStream2       = 21
)

// RDB header magic.
var RDBMagic = []byte("REDIS")

// RDB version (Redis 8.10 uses version 12).
const RDBVersion = 12

// CRC64 table for checksums.
var crc64Table = crc64.MakeTable(crc64.ECMA)

// --- RDB Writer ---

// RDBWriter 写入 RDB 快照.
// This is the Go equivalent of Redis's rdbSave().
type RDBWriter struct {
	w     *bufio.Writer
	crc   uint64
	count int
}

// NewRDBWriter creates a new RDB writer.
func NewRDBWriter(w io.Writer) *RDBWriter {
	return &RDBWriter{
		w:   bufio.NewWriterSize(w, 256*1024),
		crc: 0,
	}
}

// WriteHeader writes the RDB file header.
func (rw *RDBWriter) WriteHeader() error {
	// Write "REDIS" magic + 4-byte version
	header := make([]byte, 9)
	copy(header, RDBMagic)
	version := fmt.Sprintf("%04d", RDBVersion)
	copy(header[5:], version)
	_, err := rw.w.Write(header)
	return err
}

// WriteSelectDB writes a database selector.
func (rw *RDBWriter) WriteSelectDB(dbIndex int) error {
	if err := rw.writeByte(RDBOpcodeSelectDB); err != nil {
		return err
	}
	return rw.writeLength(uint64(dbIndex))
}

// WriteAuxField writes an auxiliary field (like redis-ver, ctime, etc).
func (rw *RDBWriter) WriteAuxField(key, value string) error {
	if err := rw.writeByte(RDBOpcodeAux); err != nil {
		return err
	}
	if err := rw.writeString(key); err != nil {
		return err
	}
	return rw.writeString(value)
}

// WriteExpireTime writes an expire time in milliseconds.
func (rw *RDBWriter) WriteExpireTimeMs(expireAt time.Time) error {
	if err := rw.writeByte(RDBOpcodeExpireTimeMs); err != nil {
		return err
	}
	ms := expireAt.UnixMilli()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ms))
	_, err := rw.w.Write(buf)
	return err
}

// WriteResizeDB writes the hash table resize hint.
func (rw *RDBWriter) WriteResizeDB(dbSize, expiresSize int) error {
	if err := rw.writeByte(RDBOpcodeResizeDB); err != nil {
		return err
	}
	if err := rw.writeLength(uint64(dbSize)); err != nil {
		return err
	}
	return rw.writeLength(uint64(expiresSize))
}

// WriteKeyValuePair writes a key-value pair with optional expiration.
func (rw *RDBWriter) WriteKeyValuePair(key string, val *object.Object, expireAt *time.Time) error {
	// Write expiration if present
	if expireAt != nil {
		if err := rw.WriteExpireTimeMs(*expireAt); err != nil {
			return err
		}
	}

	// Write type
	if err := rw.writeValueType(val); err != nil {
		return err
	}

	// Write key
	if err := rw.writeString(key); err != nil {
		return err
	}

	// Write value
	if err := rw.writeValue(val); err != nil {
		return err
	}

	rw.count++
	return nil
}

// WriteEOF writes the EOF marker.
func (rw *RDBWriter) WriteEOF() error {
	return rw.writeByte(RDBOpcodeEOF)
}

// WriteCRC64 writes the CRC64 checksum.
func (rw *RDBWriter) WriteCRC64(checksum uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, checksum)
	_, err := rw.w.Write(buf)
	return err
}

// Flush flushes the writer.
func (rw *RDBWriter) Flush() error {
	return rw.w.Flush()
}

// writeValueType writes the type byte for a value.
func (rw *RDBWriter) writeValueType(val *object.Object) error {
	switch val.Type {
	case object.TypeString:
		return rw.writeByte(RDBTypeString)
	case object.TypeList:
		return rw.writeByte(RDBTypeList)
	case object.TypeSet:
		return rw.writeByte(RDBTypeSet)
	case object.TypeZSet:
		return rw.writeByte(RDBTypeZSet)
	case object.TypeHash:
		return rw.writeByte(RDBTypeHash)
	case object.TypeStream:
		return rw.writeByte(RDBTypeStream2)
	default:
		return fmt.Errorf("unsupported RDB type: %d", val.Type)
	}
}

// writeValue writes the value payload.
func (rw *RDBWriter) writeValue(val *object.Object) error {
	switch val.Type {
	case object.TypeString:
		return rw.writeStringObject(val)
	case object.TypeList:
		return rw.writeList(val)
	case object.TypeSet:
		return rw.writeSet(val)
	case object.TypeHash:
		return rw.writeHash(val)
	case object.TypeZSet:
		return rw.writeZSet(val)
	case object.TypeStream:
		return rw.writeStream(val)
	default:
		return fmt.Errorf("unsupported RDB type: %d", val.Type)
	}
}

func (rw *RDBWriter) writeStringObject(val *object.Object) error {
	s := val.GetString()
	return rw.writeString(s)
}

func (rw *RDBWriter) writeList(val *object.Object) error {
	ql := val.Ptr.(*object.QuickList)
	if err := rw.writeLength(uint64(ql.Len())); err != nil {
		return err
	}
	for i := 0; i < ql.Len(); i++ {
		v, _ := ql.Index(i)
		s, _ := v.(string)
		if err := rw.writeString(s); err != nil {
			return err
		}
	}
	return nil
}

func (rw *RDBWriter) writeSet(val *object.Object) error {
	s := val.Ptr.(*object.Set)
	if err := rw.writeLength(uint64(s.Len())); err != nil {
		return err
	}
	for _, m := range s.Members() {
		if err := rw.writeString(m); err != nil {
			return err
		}
	}
	return nil
}

func (rw *RDBWriter) writeHash(val *object.Object) error {
	h := val.Ptr.(*object.Hash)
	if err := rw.writeLength(uint64(h.Len())); err != nil {
		return err
	}
	for _, field := range h.Fields() {
		v, _ := h.Get(field)
		if err := rw.writeString(field); err != nil {
			return err
		}
		if err := rw.writeString(v); err != nil {
			return err
		}
	}
	return nil
}

func (rw *RDBWriter) writeZSet(val *object.Object) error {
	zs := val.Ptr.(*object.ZSet)
	entries := zs.Range(0, -1, true)
	if err := rw.writeLength(uint64(len(entries))); err != nil {
		return err
	}
	for _, e := range entries {
		if err := rw.writeString(e.Member); err != nil {
			return err
		}
		// Write score as double (8 bytes, little endian)
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(e.Score))
		// Actually need to convert float64 to uint64 bits
		bits := float64ToUint64(e.Score)
		binary.LittleEndian.PutUint64(buf, bits)
		if _, err := rw.w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (rw *RDBWriter) writeStream(val *object.Object) error {
	stream := val.Ptr.(*object.Stream)
	// Write stream length
	if err := rw.writeLength(uint64(stream.Length)); err != nil {
		return err
	}
	// Write last ID
	if err := rw.writeString(stream.LastID); err != nil {
		return err
	}
	// Write entries
	for _, entry := range stream.Entries {
		if err := rw.writeString(entry.ID); err != nil {
			return err
		}
		if err := rw.writeLength(uint64(len(entry.Fields))); err != nil {
			return err
		}
		for k, v := range entry.Fields {
			if err := rw.writeString(k); err != nil {
				return err
			}
			if err := rw.writeString(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rw *RDBWriter) writeByte(b byte) error {
	_, err := rw.w.Write([]byte{b})
	return err
}

func (rw *RDBWriter) writeString(s string) error {
	if err := rw.writeLength(uint64(len(s))); err != nil {
		return err
	}
	_, err := rw.w.WriteString(s)
	return err
}

// writeLength writes a length-encoded integer (matching Redis's rdbSaveLen).
func (rw *RDBWriter) writeLength(length uint64) error {
	if length < (1 << 6) {
		// 6-bit length
		return rw.writeByte(byte(length))
	} else if length < (1 << 14) {
		// 14-bit length
		b0 := byte((length >> 8) | 0x40)
		b1 := byte(length & 0xFF)
		if _, err := rw.w.Write([]byte{b0, b1}); err != nil {
			return err
		}
	} else if length < (1 << 32) {
		// 32-bit length
		buf := make([]byte, 5)
		buf[0] = 0x80
		binary.BigEndian.PutUint32(buf[1:], uint32(length))
		if _, err := rw.w.Write(buf); err != nil {
			return err
		}
	} else {
		// 64-bit length
		buf := make([]byte, 9)
		buf[0] = 0x81
		binary.BigEndian.PutUint64(buf[1:], length)
		if _, err := rw.w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func float64ToUint64(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

// --- RDB Reader ---

// RDBReader 读取 RDB 文件.
type RDBReader struct {
	r *bufio.Reader
}

// NewRDBReader creates a new RDB reader.
func NewRDBReader(r io.Reader) *RDBReader {
	return &RDBReader{r: bufio.NewReaderSize(r, 256*1024)}
}

// ReadHeader reads and validates the RDB header.
func (rr *RDBReader) ReadHeader() (int, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(rr.r, header); err != nil {
		return 0, err
	}
	if string(header[:5]) != "REDIS" {
		return 0, fmt.Errorf("invalid RDB header: %q", header[:5])
	}
	version, err := strconv.Atoi(string(header[5:]))
	if err != nil {
		return 0, fmt.Errorf("invalid RDB version: %q", header[5:])
	}
	return version, nil
}

// ReadOpcode reads a single opcode byte.
func (rr *RDBReader) ReadOpcode() (byte, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(rr.r, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadLength reads a length-encoded integer.
func (rr *RDBReader) ReadLength() (uint64, bool, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(rr.r, b); err != nil {
		return 0, false, err
	}
	b0 := b[0]
	typ := (b0 & 0xC0) >> 6
	switch typ {
	case 0: // 6-bit length
		return uint64(b0 & 0x3F), false, nil
	case 1: // 14-bit length
		if _, err := io.ReadFull(rr.r, b); err != nil {
			return 0, false, err
		}
		return (uint64(b0&0x3F) << 8) | uint64(b[0]), false, nil
	case 2: // 32 or 64 bit length
		if b0 == 0x80 {
			buf := make([]byte, 4)
			if _, err := io.ReadFull(rr.r, buf); err != nil {
				return 0, false, err
			}
			return uint64(binary.BigEndian.Uint32(buf)), false, nil
		} else if b0 == 0x81 {
			buf := make([]byte, 8)
			if _, err := io.ReadFull(rr.r, buf); err != nil {
				return 0, false, err
			}
			return binary.BigEndian.Uint64(buf), false, nil
		}
		return 0, false, fmt.Errorf("unsupported length encoding: %02x", b0)
	case 3: // special encoding (string)
		return uint64(b0 & 0x3F), true, nil
	}
	return 0, false, fmt.Errorf("unreachable")
}

// ReadString reads a string value.
func (rr *RDBReader) ReadString() (string, error) {
	length, isEncoded, err := rr.ReadLength()
	if err != nil {
		return "", err
	}
	if isEncoded {
		// Integer or LZF compressed string
		switch length {
		case 0: // 8-bit integer
			b := make([]byte, 1)
			if _, err := io.ReadFull(rr.r, b); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int8(b[0]))), nil
		case 1: // 16-bit integer
			b := make([]byte, 2)
			if _, err := io.ReadFull(rr.r, b); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int16(binary.LittleEndian.Uint16(b)))), nil
		case 2: // 32-bit integer
			b := make([]byte, 4)
			if _, err := io.ReadFull(rr.r, b); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int32(binary.LittleEndian.Uint32(b)))), nil
		default:
			return "", fmt.Errorf("unsupported string encoding: %d", length)
		}
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(rr.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// --- AOF Writer ---

// AOFWriter 写入 AOF 条目.
// This is the Go equivalent of Redis's feedAppendOnlyFile().
type AOFWriter struct {
	mu       sync.Mutex
	file     *os.File
	w        *bufio.Writer
	filename string
	fsync    string // "always", "everysec", "no"
	lastSync time.Time
	closed   bool
}

// NewAOFWriter creates a new AOF writer.
func NewAOFWriter(filename, fsyncPolicy string) (*AOFWriter, error) {
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating AOF directory: %w", err)
	}
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening AOF file: %w", err)
	}
	aw := &AOFWriter{
		file:     f,
		w:        bufio.NewWriterSize(f, 64*1024),
		filename: filename,
		fsync:    fsyncPolicy,
		lastSync: time.Now(),
	}
	return aw, nil
}

// WriteCommand writes a RESP command to the AOF.
func (aw *AOFWriter) WriteCommand(args []string) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	if aw.closed {
		return fmt.Errorf("AOF writer is closed")
	}
	// Write RESP array format
	aw.w.WriteString("*")
	aw.w.WriteString(strconv.Itoa(len(args)))
	aw.w.WriteString("\r\n")
	for _, arg := range args {
		aw.w.WriteString("$")
		aw.w.WriteString(strconv.Itoa(len(arg)))
		aw.w.WriteString("\r\n")
		aw.w.WriteString(arg)
		aw.w.WriteString("\r\n")
	}
	if aw.fsync == "always" {
		return aw.w.Flush()
	}
	return nil
}

// Flush flushes the AOF buffer to disk.
func (aw *AOFWriter) Flush() error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	return aw.w.Flush()
}

// Fsync forces a sync to disk.
func (aw *AOFWriter) Fsync() error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	if err := aw.w.Flush(); err != nil {
		return err
	}
	return aw.file.Sync()
}

// BackgroundFsync runs fsync in background (for "everysec" policy).
func (aw *AOFWriter) BackgroundFsync() {
	go func() {
		if err := aw.Fsync(); err != nil {
			log.Printf("AOF fsync error: %v", err)
		}
	}()
}

// Close closes the AOF writer.
func (aw *AOFWriter) Close() error {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	aw.closed = true
	if err := aw.w.Flush(); err != nil {
		return err
	}
	return aw.file.Close()
}

// --- AOF Rewrite ---

// RewriteAOF 重写 AOF 文件 from the current database state.
// This is the Go equivalent of Redis's rewriteAppendOnlyFile().
func RewriteAOF(filename string, databases *db.Database) error {
	tmpFile := filename + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 256*1024)

	// Write SELECT commands for each non-empty database
	for i := 0; i < databases.Count; i++ {
		currentDB, _ := databases.GetDB(i)
		hasKeys := false
		currentDB.Keys.Range(func(_ string, _ interface{}) bool {
			hasKeys = true
			return false
		})
		if !hasKeys {
			continue
		}
		// Write SELECT
		w.WriteString("*2\r\n$6\r\nSELECT\r\n$")
		w.WriteString(strconv.Itoa(len(strconv.Itoa(i))))
		w.WriteString("\r\n")
		w.WriteString(strconv.Itoa(i))
		w.WriteString("\r\n")

		// Write all keys
		currentDB.Keys.Range(func(key string, val interface{}) bool {
			k := key
			v := val.(*object.Object)

			// Check expiration
			if expireAt, ok := currentDB.GetExpire(k); ok {
				// Write PEXPIREAT
				w.WriteString("*3\r\n$10\r\nPEXPIREAT\r\n$")
				w.WriteString(strconv.Itoa(len(k)))
				w.WriteString("\r\n")
				w.WriteString(k)
				w.WriteString("\r\n:")
				w.WriteString(strconv.FormatInt(expireAt.UnixMilli(), 10))
				w.WriteString("\r\n")
			}

			// Write the value
			writeAOFValue(w, k, v)
			return true
		})
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return err
	}
	f.Close()

	// Atomic rename
	return os.Rename(tmpFile, filename)
}

func writeAOFValue(w *bufio.Writer, key string, val *object.Object) {
	switch val.Type {
	case object.TypeString:
		s := val.GetString()
		w.WriteString("*3\r\n$3\r\nSET\r\n$")
		w.WriteString(strconv.Itoa(len(key)))
		w.WriteString("\r\n")
		w.WriteString(key)
		w.WriteString("\r\n$")
		w.WriteString(strconv.Itoa(len(s)))
		w.WriteString("\r\n")
		w.WriteString(s)
		w.WriteString("\r\n")
	case object.TypeList:
		ql := val.Ptr.(*object.QuickList)
		for i := 0; i < ql.Len(); i++ {
			v, _ := ql.Index(i)
			s, _ := v.(string)
			w.WriteString("*3\r\n$5\r\nRPUSH\r\n$")
			w.WriteString(strconv.Itoa(len(key)))
			w.WriteString("\r\n")
			w.WriteString(key)
			w.WriteString("\r\n$")
			w.WriteString(strconv.Itoa(len(s)))
			w.WriteString("\r\n")
			w.WriteString(s)
			w.WriteString("\r\n")
		}
	case object.TypeSet:
		s := val.Ptr.(*object.Set)
		for _, m := range s.Members() {
			w.WriteString("*3\r\n$4\r\nSADD\r\n$")
			w.WriteString(strconv.Itoa(len(key)))
			w.WriteString("\r\n")
			w.WriteString(key)
			w.WriteString("\r\n$")
			w.WriteString(strconv.Itoa(len(m)))
			w.WriteString("\r\n")
			w.WriteString(m)
			w.WriteString("\r\n")
		}
	case object.TypeHash:
		h := val.Ptr.(*object.Hash)
		for _, field := range h.Fields() {
			v, _ := h.Get(field)
			w.WriteString("*4\r\n$4\r\nHSET\r\n$")
			w.WriteString(strconv.Itoa(len(key)))
			w.WriteString("\r\n")
			w.WriteString(key)
			w.WriteString("\r\n$")
			w.WriteString(strconv.Itoa(len(field)))
			w.WriteString("\r\n")
			w.WriteString(field)
			w.WriteString("\r\n$")
			w.WriteString(strconv.Itoa(len(v)))
			w.WriteString("\r\n")
			w.WriteString(v)
			w.WriteString("\r\n")
		}
	case object.TypeZSet:
		zs := val.Ptr.(*object.ZSet)
		entries := zs.Range(0, -1, true)
		for _, e := range entries {
			scoreStr := fmt.Sprintf("%g", e.Score)
			w.WriteString("*4\r\n$4\r\nZADD\r\n$")
			w.WriteString(strconv.Itoa(len(key)))
			w.WriteString("\r\n")
			w.WriteString(key)
			w.WriteString("\r\n$")
			w.WriteString(strconv.Itoa(len(scoreStr)))
			w.WriteString("\r\n")
			w.WriteString(scoreStr)
			w.WriteString("\r\n$")
			w.WriteString(strconv.Itoa(len(e.Member)))
			w.WriteString("\r\n")
			w.WriteString(e.Member)
			w.WriteString("\r\n")
		}
	}
}

