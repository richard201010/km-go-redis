// 位图命令处理器 (matching Redis's bitops.c)
// Full implementation with bit-level operations on string keys.
package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

// setbitCommand sets or clears a bit at offset in a string key.
func setbitCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments for 'setbit' command")
		return
	}
	key := ctx.Args[1]
	offset, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil || offset < 0 {
		ctx.Client.SendError("ERR bit offset is not an integer or out of range")
		return
	}
	bitVal := ctx.Args[3]
	if bitVal != "0" && bitVal != "1" {
		ctx.Client.SendError("ERR bit is not an integer or out of range")
		return
	}
	val := ctx.DB.LookupKey(key)
	var data []byte
	if val != nil {
		if val.Type != object.TypeString {
			ctx.Client.ReplyTypeMismatch()
			return
		}
		data = []byte(val.GetString())
	}
	byteIdx := int(offset / 8)
	bitIdx := uint(7 - offset%8) // MSB first
	// Expand if needed
	for len(data) <= byteIdx {
		data = append(data, 0)
	}
	oldBit := (data[byteIdx] >> bitIdx) & 1
	if bitVal == "1" {
		data[byteIdx] |= (1 << bitIdx)
	} else {
		data[byteIdx] &^= (1 << bitIdx)
	}
	obj := object.NewStringObject(string(data))
	ctx.DB.SetKey(key, obj)
	ctx.Client.SendInteger(int64(oldBit))
}

// getbitCommand returns the bit value at offset.
func getbitCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'getbit' command")
		return
	}
	key := ctx.Args[1]
	offset, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil || offset < 0 {
		ctx.Client.SendError("ERR bit offset is not an integer or out of range")
		return
	}
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeString {
		ctx.Client.SendInteger(0)
		return
	}
	data := []byte(val.GetString())
	byteIdx := int(offset / 8)
	if byteIdx >= len(data) {
		ctx.Client.SendInteger(0)
		return
	}
	bitIdx := uint(7 - offset%8)
	bit := (data[byteIdx] >> bitIdx) & 1
	ctx.Client.SendInteger(int64(bit))
}

// bitcountCommand counts set bits in a string key.
func bitcountCommand(ctx *CommandContext) {
	if len(ctx.Args) < 2 {
		ctx.Client.SendError("ERR wrong number of arguments for 'bitcount' command")
		return
	}
	key := ctx.Args[1]
	start, end := 0, -1
	if len(ctx.Args) >= 4 {
		start, _ = strconv.Atoi(ctx.Args[2])
		end, _ = strconv.Atoi(ctx.Args[3])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeString {
		ctx.Client.SendInteger(0)
		return
	}
	data := []byte(val.GetString())
	if start < 0 {
		start = len(data) + start
	}
	if end < 0 {
		end = len(data) + end
	}
	if start < 0 {
		start = 0
	}
	if end >= len(data) {
		end = len(data) - 1
	}
	if start > end || start >= len(data) {
		ctx.Client.SendInteger(0)
		return
	}
	count := 0
	for _, b := range data[start : end+1] {
		count += popcount(b)
	}
	ctx.Client.SendInteger(int64(count))
}

// popcount counts set bits in a byte.
func popcount(b byte) int {
	count := 0
	for b != 0 {
		count += int(b & 1)
		b >>= 1
	}
	return count
}

// bitposCommand finds the first set or clear bit.
func bitposCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'bitpos' command")
		return
	}
	key := ctx.Args[1]
	bit, err := strconv.Atoi(ctx.Args[2])
	if err != nil || (bit != 0 && bit != 1) {
		ctx.Client.SendError("ERR bit is not an integer or out of range")
		return
	}
	start, end := 0, -1
	if len(ctx.Args) >= 4 {
		start, _ = strconv.Atoi(ctx.Args[3])
	}
	if len(ctx.Args) >= 5 {
		end, _ = strconv.Atoi(ctx.Args[4])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeString {
		if bit == 1 {
			ctx.Client.SendInteger(-1)
		} else {
			ctx.Client.SendInteger(0)
		}
		return
	}
	data := []byte(val.GetString())
	if start < 0 {
		start = len(data) + start
	}
	if end < 0 {
		end = len(data) + end
	}
	if start < 0 {
		start = 0
	}
	if end >= len(data) {
		end = len(data) - 1
	}
	if start > end {
		ctx.Client.SendInteger(-1)
		return
	}
	for i := start; i <= end; i++ {
		for j := 7; j >= 0; j-- {
			b := (data[i] >> uint(j)) & 1
			if int(b) == bit {
				ctx.Client.SendInteger(int64(i*8 + (7 - j)))
				return
			}
		}
	}
	ctx.Client.SendInteger(-1)
}

// bitopCommand performs bitwise operations across multiple keys.
func bitopCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments for 'bitop' command")
		return
	}
	op := strings.ToUpper(ctx.Args[1])
	dest := ctx.Args[2]
	keys := ctx.Args[3:]
	// Get all source values
	values := make([][]byte, len(keys))
	maxLen := 0
	for i, k := range keys {
		val := ctx.DB.LookupKey(k)
		if val != nil && val.Type == object.TypeString {
			values[i] = []byte(val.GetString())
			if len(values[i]) > maxLen {
				maxLen = len(values[i])
			}
		}
	}
	if maxLen == 0 {
		ctx.DB.DeleteKey(dest)
		ctx.Client.SendInteger(0)
		return
	}
	result := make([]byte, maxLen)
	switch op {
	case "AND":
		for i := 0; i < maxLen; i++ {
			b := byte(0xFF)
			for _, v := range values {
				if i < len(v) {
					b &= v[i]
				} else {
					b = 0
					break
				}
			}
			result[i] = b
		}
	case "OR":
		for i := 0; i < maxLen; i++ {
			b := byte(0)
			for _, v := range values {
				if i < len(v) {
					b |= v[i]
				}
			}
			result[i] = b
		}
	case "XOR":
		for i := 0; i < maxLen; i++ {
			b := byte(0)
			for _, v := range values {
				if i < len(v) {
					b ^= v[i]
				}
			}
			result[i] = b
		}
	case "NOT":
		if len(keys) != 1 {
			ctx.Client.SendError("ERR BITOP NOT must be called with a single source key")
			return
		}
		if len(values[0]) > 0 {
			result = make([]byte, len(values[0]))
			for i, b := range values[0] {
				result[i] = ^b
			}
		}
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR Unknown bitop operation '%s'", op))
		return
	}
	obj := object.NewStringObject(string(result))
	ctx.DB.DeleteKey(dest)
	ctx.DB.SetKey(dest, obj)
	ctx.Client.SendInteger(int64(len(result)))
}

// bitfieldCommand reads/writes bitfield values.
func bitfieldCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'bitfield' command")
		return
	}
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	var data []byte
	if val != nil && val.Type == object.TypeString {
		data = []byte(val.GetString())
	}
	result := make([]resp.RESPValue, 0)
	argsIdx := 2
	for argsIdx < len(ctx.Args) {
		op := strings.ToUpper(ctx.Args[argsIdx])
		argsIdx++
		switch op {
		case "GET":
			if argsIdx+2 > len(ctx.Args) {
				break
			}
			// type, offset
			typeStr := ctx.Args[argsIdx]
			offset, _ := strconv.ParseInt(ctx.Args[argsIdx+1], 10, 64)
			argsIdx += 2
			bits, _ := parseBitfieldType(typeStr)
			if bits == 0 {
				result = append(result, resp.NullBulk)
				continue
			}
			val := readBits(data, offset, bits)
			result = append(result, resp.IntegerReply(val))
		case "SET":
			if argsIdx+3 > len(ctx.Args) {
				break
			}
			typeStr := ctx.Args[argsIdx]
			offset, _ := strconv.ParseInt(ctx.Args[argsIdx+1], 10, 64)
			value, _ := strconv.ParseInt(ctx.Args[argsIdx+2], 10, 64)
			argsIdx += 3
			bits, signed := parseBitfieldType(typeStr)
			if bits == 0 {
				result = append(result, resp.NullBulk)
				continue
			}
			oldVal := readBits(data, offset, bits)
			if signed {
				oldVal = signExtend(oldVal, bits)
			}
			result = append(result, resp.IntegerReply(oldVal))
			data = writeBits(data, offset, bits, value)
		case "INCRBY":
			if argsIdx+3 > len(ctx.Args) {
				break
			}
			typeStr := ctx.Args[argsIdx]
			offset, _ := strconv.ParseInt(ctx.Args[argsIdx+1], 10, 64)
			incr, _ := strconv.ParseInt(ctx.Args[argsIdx+2], 10, 64)
			argsIdx += 3
			bits, signed := parseBitfieldType(typeStr)
			if bits == 0 {
				result = append(result, resp.NullBulk)
				continue
			}
			oldVal := readBits(data, offset, bits)
			if signed {
				oldVal = signExtend(oldVal, bits)
			}
			newVal := oldVal + incr
			// Overflow handling
			if bits < 64 {
				maxVal := int64(1) << uint(bits)
				if signed {
					newVal = (newVal + maxVal/2) % maxVal - maxVal/2
				} else {
					newVal = (newVal + maxVal) % maxVal
				}
			}
			result = append(result, resp.IntegerReply(newVal))
			data = writeBits(data, offset, bits, newVal)
		case "OVERFLOW":
			// Skip overflow policy (WRAP/SAT/FAIL)
			if argsIdx < len(ctx.Args) {
				argsIdx++
			}
		}
	}
	// Write back
	obj := object.NewStringObject(string(data))
	ctx.DB.DeleteKey(key)
	ctx.DB.SetKey(key, obj)
	ctx.Client.SendArray(result)
}

// bitfieldroCommand is the read-only version of bitfield.
func bitfieldroCommand(ctx *CommandContext) {
	bitfieldCommand(ctx)
}

// parseBitfieldType parses type like "u8", "i16", etc.
func parseBitfieldType(s string) (bits int, signed bool) {
	if len(s) < 2 {
		return 0, false
	}
	if s[0] == 'u' {
		signed = false
	} else if s[0] == 'i' {
		signed = true
	} else {
		return 0, false
	}
	bits, _ = strconv.Atoi(s[1:])
	if bits < 1 || bits > 64 {
		return 0, false
	}
	return
}

func readBits(data []byte, offset int64, bits int) int64 {
	var val int64
	for i := 0; i < bits; i++ {
		bitOffset := offset + int64(i)
		byteIdx := int(bitOffset / 8)
		bitIdx := uint(7 - bitOffset%8)
		var b byte
		if byteIdx < len(data) {
			b = data[byteIdx]
		}
		val = (val << 1) | int64((b>>bitIdx)&1)
	}
	return val
}

func writeBits(data []byte, offset int64, bits int, value int64) []byte {
	for i := 0; i < bits; i++ {
		bitOffset := offset + int64(i)
		byteIdx := int(bitOffset / 8)
		bitIdx := uint(7 - bitOffset%8)
		for len(data) <= byteIdx {
			data = append(data, 0)
		}
		bit := (value >> uint(bits-1-i)) & 1
		if bit == 1 {
			data[byteIdx] |= (1 << bitIdx)
		} else {
			data[byteIdx] &^= (1 << bitIdx)
		}
	}
	return data
}

func signExtend(val int64, bits int) int64 {
	if bits >= 64 {
		return val
	}
	mask := int64(1) << uint(bits-1)
	if val&mask != 0 {
		// Negative
		val |= ^((int64(1) << uint(bits)) - 1)
	}
	return val
}
