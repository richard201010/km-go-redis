// Package resp - RESP3 完整写入支持。
// 本文件添加 RESP3 特有的写入方法，对应 Redis 8.10 的 RESP3 协议。
package resp

import (
	"fmt"
	"strconv"
)

// WriteMap 写入 RESP3 映射（%...）。
// 格式: %<键值对数>\r\n<键1><值1><键2><值2>...
// 对应 Redis 的 addReplyMapLen()。
func (w *Writer) WriteMap(m map[string]RESPValue) error {
	if m == nil {
		_, err := w.w.WriteString("%-1\r\n")
		return err
	}
	w.w.WriteByte('%')
	w.w.WriteString(strconv.Itoa(len(m)))
	w.w.WriteString("\r\n")
	for k, v := range m {
		if err := w.WriteBulkStr(k); err != nil {
			return err
		}
		if err := w.WriteValue(v); err != nil {
			return err
		}
	}
	return nil
}

// WriteSet 写入 RESP3 集合（~...）。
// 格式: ~<元素数>\r\n<元素1><元素2>...
// 对应 Redis 的 addReplySetLen()。
func (w *Writer) WriteSet(items []RESPValue) error {
	if items == nil {
		_, err := w.w.WriteString("~-1\r\n")
		return err
	}
	w.w.WriteByte('~')
	w.w.WriteString(strconv.Itoa(len(items)))
	w.w.WriteString("\r\n")
	for _, item := range items {
		if err := w.WriteValue(item); err != nil {
			return err
		}
	}
	return nil
}

// WritePush 写入 RESP3 推送（>...）。
// 格式: ><元素数>\r\n<元素1><元素2>...
// 对应 Redis 的 addReplyPushLen()。
func (w *Writer) WritePush(items []RESPValue) error {
	w.w.WriteByte('>')
	w.w.WriteString(strconv.Itoa(len(items)))
	w.w.WriteString("\r\n")
	for _, item := range items {
		if err := w.WriteValue(item); err != nil {
			return err
		}
	}
	return nil
}

// WriteBoolean 写入 RESP3 布尔值（#...）。
// 格式: #t\r\n 或 #f\r\n
// 对应 Redis 的 addReplyBool()。
func (w *Writer) WriteBoolean(b bool) error {
	if b {
		_, err := w.w.WriteString("#t\r\n")
		return err
	}
	_, err := w.w.WriteString("#f\r\n")
	return err
}

// WriteDouble 写入 RESP3 浮点数（,...）。
// 格式: ,<浮点数>\r\n
// 对应 Redis 的 addReplyDouble()。
func (w *Writer) WriteDouble(f float64) error {
	w.w.WriteByte(',')
	s := strconv.FormatFloat(f, 'g', -1, 64)
	// 处理特殊情况
	if s == "NaN" {
		s = "nan"
	} else if s == "+Inf" {
		s = "inf"
	} else if s == "-Inf" {
		s = "-inf"
	}
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteBigNumber 写入 RESP3 大数（(...）。
// 格式: (<大数字符串>\r\n
// 对应 Redis 的 addReplyBigNumber()。
func (w *Writer) WriteBigNumber(s string) error {
	w.w.WriteByte('(')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteBlobError 写入 RESP3 块错误（!...）。
// 格式: !<长度>\r\n<错误数据>\r\n
// 对应 Redis 的 addReplyBulkError()。
func (w *Writer) WriteBlobError(msg string) error {
	w.w.WriteByte('!')
	w.w.WriteString(strconv.Itoa(len(msg)))
	w.w.WriteString("\r\n")
	w.w.WriteString(msg)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteVerbatim 写入 RESP3 逐字字符串（=...）。
// 格式: =<长度>\r\n<3字节类型>:<数据>\r\n
// 对应 Redis 的 addReplyVerbatim()。
func (w *Writer) WriteVerbatim(typ string, s string) error {
	if len(typ) != 3 {
		return fmt.Errorf("verbatim type must be 3 characters")
	}
	totalLen := 4 + len(s) // 3字节类型 + ':' + 数据
	w.w.WriteByte('=')
	w.w.WriteString(strconv.Itoa(totalLen))
	w.w.WriteString("\r\n")
	w.w.WriteString(typ)
	w.w.WriteByte(':')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteAttribute 写入 RESP3 属性（|...）。
// 格式: |<键值对数>\r\n<键1><值1>...
// 属性会被客户端忽略，用于元数据传输。
func (w *Writer) WriteAttribute(m map[string]RESPValue) error {
	w.w.WriteByte('|')
	w.w.WriteString(strconv.Itoa(len(m)))
	w.w.WriteString("\r\n")
	for k, v := range m {
		if err := w.WriteBulkStr(k); err != nil {
			return err
		}
		if err := w.WriteValue(v); err != nil {
			return err
		}
	}
	return nil
}

// WriteHelloResponse 写入 HELLO 命令的 RESP3 响应。
// 对应 Redis 的 helloCommand() 中的 RESP3 回复。
func (w *Writer) WriteHelloResponse(proto int, serverName, version string) error {
	if proto >= 3 {
		// RESP3: 返回映射
		return w.WriteMap(map[string]RESPValue{
			"server":  BulkStringReply(serverName),
			"version": BulkStringReply(version),
			"proto":   IntegerReply(int64(proto)),
			"id":      IntegerReply(1),
			"name":    BulkStringReply(""),
			"mode":    BulkStringReply("standalone"),
			"role":    BulkStringReply("master"),
			"modules": ArrayReply([]RESPValue{}),
		})
	}
	// RESP2: 返回数组
	return w.writeArray([]RESPValue{
		BulkStringReply("server"), BulkStringReply(serverName),
		BulkStringReply("version"), BulkStringReply(version),
		BulkStringReply("proto"), IntegerReply(int64(proto)),
		BulkStringReply("id"), IntegerReply(1),
		BulkStringReply("name"), BulkStringReply(""),
		BulkStringReply("mode"), BulkStringReply("standalone"),
		BulkStringReply("role"), BulkStringReply("master"),
		BulkStringReply("modules"), ArrayReply([]RESPValue{}),
	})
}

// WritePubSubMessage 写入 Pub/Sub 消息推送。
// 格式（RESP2）: *3\r\n$7\r\nmessage\r\n$<频道长度>\r\n<频道>\r\n$<消息长度>\r\n<消息>\r\n
// 格式（RESP3）: >3\r\n$7\r\nmessage\r\n$<频道长度>\r\n<频道>\r\n$<消息长度>\r\n<消息>\r\n
func (w *Writer) WritePubSubMessage(channel, message string, resp3 bool) error {
	if resp3 {
		return w.WritePush([]RESPValue{
			BulkStringReply("message"),
			BulkStringReply(channel),
			BulkStringReply(message),
		})
	}
	return w.writeArray([]RESPValue{
		BulkStringReply("message"),
		BulkStringReply(channel),
		BulkStringReply(message),
	})
}

// WritePubSubSubscribe 写入订阅确认。
func (w *Writer) WritePubSubSubscribe(channel string, count int, resp3 bool) error {
	if resp3 {
		return w.WritePush([]RESPValue{
			BulkStringReply("subscribe"),
			BulkStringReply(channel),
			IntegerReply(int64(count)),
		})
	}
	return w.writeArray([]RESPValue{
		BulkStringReply("subscribe"),
		BulkStringReply(channel),
		IntegerReply(int64(count)),
	})
}

// WritePubSubUnsubscribe 写入取消订阅确认。
func (w *Writer) WritePubSubUnsubscribe(channel string, count int, resp3 bool) error {
	if resp3 {
		return w.WritePush([]RESPValue{
			BulkStringReply("unsubscribe"),
			BulkStringReply(channel),
			IntegerReply(int64(count)),
		})
	}
	return w.writeArray([]RESPValue{
		BulkStringReply("unsubscribe"),
		BulkStringReply(channel),
		IntegerReply(int64(count)),
	})
}
