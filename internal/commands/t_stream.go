// 流命令处理器 (t_stream.c equivalent)
package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/km-dev/km-go-redis/internal/object"
	"github.com/km-dev/km-go-redis/internal/resp"
)

func xaddCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	// Parse optional MAXLEN/NOMKSTREAM
	argsIdx := 2
	maxLen := -1
	noMkStream := false
	for argsIdx < len(ctx.Args) {
		switch strings.ToUpper(ctx.Args[argsIdx]) {
		case "MAXLEN":
			argsIdx++
			if argsIdx < len(ctx.Args) && ctx.Args[argsIdx] == "~" {
				argsIdx++
			}
			if argsIdx < len(ctx.Args) {
				maxLen, _ = strconv.Atoi(ctx.Args[argsIdx])
				argsIdx++
			}
			continue
		case "MINID":
			argsIdx += 2 // skip MINID and value
			continue
		case "NOMKSTREAM":
			noMkStream = true
			argsIdx++
			continue
		default:
			goto parseFields
		}
	}
parseFields:
	if argsIdx >= len(ctx.Args) {
		ctx.Client.SendError("ERR wrong number of arguments for 'xadd' command")
		return
	}
	id := ctx.Args[argsIdx]
	argsIdx++
	if (len(ctx.Args)-argsIdx)%2 != 0 || argsIdx >= len(ctx.Args) {
		ctx.Client.SendError("ERR wrong number of arguments for 'xadd' command")
		return
	}
	fields := make(map[string]string)
	for i := argsIdx; i < len(ctx.Args); i += 2 {
		fields[ctx.Args[i]] = ctx.Args[i+1]
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		if noMkStream {
			ctx.Client.SendNull()
			return
		}
		val = object.NewStreamObject()
		ctx.DB.SetKey(key, val)
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	stream := val.Ptr.(*object.Stream)
	newID, err := stream.Add(fields, id)
	if err != nil {
		ctx.Client.SendError(err.Error())
		return
	}
	// Trim if MAXLEN specified
	if maxLen >= 0 && len(stream.Entries) > maxLen {
		stream.Entries = stream.Entries[len(stream.Entries)-maxLen:]
		stream.Length = int64(len(stream.Entries))
	}
	ctx.Client.SendBulkString(newID)
}

func xrangeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	start := ctx.Args[2]
	end := ctx.Args[3]
	count := -1
	if len(ctx.Args) > 4 && strings.ToUpper(ctx.Args[4]) == "COUNT" && len(ctx.Args) > 5 {
		count, _ = strconv.Atoi(ctx.Args[5])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	stream := val.Ptr.(*object.Stream)
	result := make([]resp.RESPValue, 0)
	for _, entry := range stream.Entries {
		if streamIDCompare(entry.ID, start) < 0 {
			continue
		}
		if streamIDCompare(entry.ID, end) > 0 {
			break
		}
		result = append(result, streamEntryToResp(entry))
		if count > 0 && len(result) >= count {
			break
		}
	}
	ctx.Client.SendArray(result)
}

func xrevrangeCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	end := ctx.Args[2]
	start := ctx.Args[3]
	count := -1
	if len(ctx.Args) > 4 && strings.ToUpper(ctx.Args[4]) == "COUNT" && len(ctx.Args) > 5 {
		count, _ = strconv.Atoi(ctx.Args[5])
	}
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	stream := val.Ptr.(*object.Stream)
	result := make([]resp.RESPValue, 0)
	for i := len(stream.Entries) - 1; i >= 0; i-- {
		entry := stream.Entries[i]
		if streamIDCompare(entry.ID, end) > 0 {
			continue
		}
		if streamIDCompare(entry.ID, start) < 0 {
			break
		}
		result = append(result, streamEntryToResp(entry))
		if count > 0 && len(result) >= count {
			break
		}
	}
	ctx.Client.SendArray(result)
}

func xlenCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	ctx.Client.SendInteger(val.Ptr.(*object.Stream).Length)
}

func xreadCommand(ctx *CommandContext) {
	count := -1
	blockMs := -1
	argsIdx := 1
Streams:
	for argsIdx < len(ctx.Args) {
		switch strings.ToUpper(ctx.Args[argsIdx]) {
		case "COUNT":
			argsIdx++
			if argsIdx < len(ctx.Args) {
				count, _ = strconv.Atoi(ctx.Args[argsIdx])
				argsIdx++
			}
		case "BLOCK":
			argsIdx++
			if argsIdx < len(ctx.Args) {
				blockMs, _ = strconv.Atoi(ctx.Args[argsIdx])
				argsIdx++
			}
		case "STREAMS":
			argsIdx++
			break Streams
		default:
			argsIdx++
		}
	}
	// Parse streams and IDs
	remaining := len(ctx.Args) - argsIdx
	if remaining%2 != 0 || remaining == 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xread' command")
		return
	}
	numStreams := remaining / 2
	streams := ctx.Args[argsIdx : argsIdx+numStreams]
	ids := ctx.Args[argsIdx+numStreams:]
	result := make([]resp.RESPValue, 0)
	for i, streamKey := range streams {
		val := ctx.DB.LookupKey(streamKey)
		if val == nil || val.Type != object.TypeStream {
			continue
		}
		stream := val.Ptr.(*object.Stream)
		afterID := ids[i]
		entries := make([]resp.RESPValue, 0)
		for _, entry := range stream.Entries {
			if streamIDCompare(entry.ID, afterID) > 0 {
				entries = append(entries, streamEntryToResp(entry))
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}
		if len(entries) > 0 {
			result = append(result, resp.ArrayReply([]resp.RESPValue{
				resp.BulkStringReply(streamKey),
				resp.ArrayReply(entries),
			}))
		}
	}
	if len(result) == 0 {
		if blockMs >= 0 {
			ctx.Client.SendNil()
		} else {
			ctx.Client.SendNil()
		}
		return
	}
	ctx.Client.SendArray(result)
}

func xdelCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	stream := val.Ptr.(*object.Stream)
	deleted := 0
	for i := 2; i < len(ctx.Args); i++ {
		for j, entry := range stream.Entries {
			if entry.ID == ctx.Args[i] {
				stream.Entries = append(stream.Entries[:j], stream.Entries[j+1:]...)
				stream.Length--
				deleted++
				break
			}
		}
	}
	ctx.Client.SendInteger(int64(deleted))
}

func xtrimCommand(ctx *CommandContext) {
	key := ctx.Args[1]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendInteger(0)
		return
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	stream := val.Ptr.(*object.Stream)
	trimmed := 0
	if len(ctx.Args) >= 4 && strings.ToUpper(ctx.Args[2]) == "MAXLEN" {
		maxLen, _ := strconv.Atoi(ctx.Args[3])
		if len(stream.Entries) > maxLen {
			trimmed = len(stream.Entries) - maxLen
			stream.Entries = stream.Entries[trimmed:]
			stream.Length = int64(len(stream.Entries))
		}
	}
	ctx.Client.SendInteger(int64(trimmed))
}

func xinfoCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xinfo' command")
		return
	}
	subcmd := strings.ToUpper(ctx.Args[1])
	switch subcmd {
	case "STREAM":
		key := ctx.Args[2]
		val := ctx.DB.LookupKey(key)
		if val == nil {
			ctx.Client.SendError("ERR no such key")
			return
		}
		if val.Type != object.TypeStream {
			ctx.Client.ReplyTypeMismatch()
			return
		}
		stream := val.Ptr.(*object.Stream)
		result := make([]resp.RESPValue, 0)
		result = append(result, resp.BulkStringReply("length"), resp.IntegerReply(stream.Length))
		result = append(result, resp.BulkStringReply("last-generated-id"), resp.BulkStringReply(stream.LastID))
		ctx.Client.SendArray(result)
	default:
		ctx.Client.SendArray(nil)
	}
}

// XGROUP CREATE/DESTROY/SETID/DELCONSUMER/CREATECONSUMER
func xgroupCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	subcmd := strings.ToUpper(ctx.Args[1])
	switch subcmd {
	case "CREATE":
		// XGROUP CREATE <key> <group> <id|$> [MKSTREAM]
		if len(ctx.Args) < 6 {
			ctx.Client.SendError("ERR wrong number of arguments for 'xgroup|create' command")
			return
		}
		key := ctx.Args[2]
		groupName := ctx.Args[3]
		lastID := ctx.Args[4]
		mkstream := len(ctx.Args) > 6 && strings.ToUpper(ctx.Args[5]) == "MKSTREAM"
		val := ctx.DB.LookupKey(key)
		if val == nil {
			if mkstream {
				val = object.NewStreamObject()
				ctx.DB.SetKey(key, val)
			} else {
				ctx.Client.SendError("ERR The XGROUP subcommand requires the key to exist")
				return
			}
		}
		if val.Type != object.TypeStream {
			ctx.Client.ReplyTypeMismatch()
			return
		}
		stream := val.Ptr.(*object.Stream)
		if _, exists := stream.Groups[groupName]; exists {
			ctx.Client.SendError("BUSYGROUP Consumer Group name already exists")
			return
		}
		if lastID == "$" {
			lastID = stream.LastID
		}
		stream.Groups[groupName] = &object.StreamConsumerGroup{
			Name:      groupName,
			LastID:    lastID,
			Consumers: make(map[string]*object.StreamConsumer),
		}
		ctx.Client.SendOK()
	case "DESTROY":
		// XGROUP DESTROY <key> <group>
		if len(ctx.Args) < 5 {
			ctx.Client.SendError("ERR wrong number of arguments for 'xgroup|destroy' command")
			return
		}
		key := ctx.Args[2]
		groupName := ctx.Args[3]
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeStream {
			ctx.Client.SendInteger(0)
			return
		}
		stream := val.Ptr.(*object.Stream)
		if _, exists := stream.Groups[groupName]; !exists {
			ctx.Client.SendInteger(0)
			return
		}
		delete(stream.Groups, groupName)
		ctx.Client.SendInteger(1)
	case "SETID":
		// XGROUP SETID <key> <group> <id|$>
		if len(ctx.Args) < 6 {
			ctx.Client.SendError("ERR wrong number of arguments for 'xgroup|setid' command")
			return
		}
		key := ctx.Args[2]
		groupName := ctx.Args[3]
		lastID := ctx.Args[4]
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeStream {
			ctx.Client.SendError("ERR The XGROUP subcommand requires the key to exist")
			return
		}
		stream := val.Ptr.(*object.Stream)
		group, exists := stream.Groups[groupName]
		if !exists {
			ctx.Client.SendError("ERR no such consumer group")
			return
		}
		if lastID == "$" {
			lastID = stream.LastID
		}
		group.LastID = lastID
		ctx.Client.SendOK()
	case "DELCONSUMER":
		// XGROUP DELCONSUMER <key> <group> <consumer>
		if len(ctx.Args) < 6 {
			ctx.Client.SendError("ERR wrong number of arguments for 'xgroup|delconsumer' command")
			return
		}
		key := ctx.Args[2]
		groupName := ctx.Args[3]
		consumerName := ctx.Args[4]
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeStream {
			ctx.Client.SendInteger(0)
			return
		}
		stream := val.Ptr.(*object.Stream)
		group, exists := stream.Groups[groupName]
		if !exists {
			ctx.Client.SendInteger(0)
			return
		}
		consumer, exists := group.Consumers[consumerName]
		if !exists {
			ctx.Client.SendInteger(0)
			return
		}
		pendingCount := len(consumer.Pending)
		delete(group.Consumers, consumerName)
		ctx.Client.SendInteger(int64(pendingCount))
	case "CREATECONSUMER":
		// XGROUP CREATECONSUMER <key> <group> <consumer>
		if len(ctx.Args) < 6 {
			ctx.Client.SendError("ERR wrong number of arguments for 'xgroup|createconsumer' command")
			return
		}
		key := ctx.Args[2]
		groupName := ctx.Args[3]
		consumerName := ctx.Args[4]
		val := ctx.DB.LookupKey(key)
		if val == nil || val.Type != object.TypeStream {
			ctx.Client.SendError("ERR The XGROUP subcommand requires the key to exist")
			return
		}
		stream := val.Ptr.(*object.Stream)
		group, exists := stream.Groups[groupName]
		if !exists {
			ctx.Client.SendError("ERR no such consumer group")
			return
		}
		if _, exists := group.Consumers[consumerName]; exists {
			ctx.Client.SendInteger(0)
			return
		}
		group.Consumers[consumerName] = &object.StreamConsumer{
			Name:    consumerName,
			SeenTime: time.Now(),
			Pending: make(map[string]time.Time),
		}
		ctx.Client.SendInteger(1)
	default:
		ctx.Client.SendError(fmt.Sprintf("ERR Unknown XGROUP subcommand '%s'", subcmd))
	}
}

// XREADGROUP GROUP <group> <consumer> [COUNT <count>] [BLOCK <ms>] [NOACK] STREAMS <key> [<key>...] <id> [<id>...]
func xreadgroupCommand(ctx *CommandContext) {
	if len(ctx.Args) < 7 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xreadgroup' command")
		return
	}
	// 解析参数
	idx := 1
	groupName := ""
	consumerName := ""
	count := -1
	// 解析 GROUP <group> <consumer>
	if strings.ToUpper(ctx.Args[idx]) == "GROUP" {
		groupName = ctx.Args[idx+1]
		consumerName = ctx.Args[idx+2]
		idx += 3
	}
	// 解析 COUNT
	for idx < len(ctx.Args) && strings.ToUpper(ctx.Args[idx]) == "COUNT" {
		count, _ = strconv.Atoi(ctx.Args[idx+1])
		idx += 2
	}
	// 跳过 NOACK/BLOCK
	for idx < len(ctx.Args) {
		switch strings.ToUpper(ctx.Args[idx]) {
		case "BLOCK":
			idx += 2
		case "NOACK":
			idx++
		default:
			goto parseStreamsG
		}
	}
parseStreamsG:
	if strings.ToUpper(ctx.Args[idx]) != "STREAMS" {
		ctx.Client.SendError("ERR syntax error")
		return
	}
	idx++
	remaining := len(ctx.Args) - idx
	if remaining%2 != 0 || remaining == 0 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xreadgroup' command")
		return
	}
	numStreams := remaining / 2
	streamKeys := ctx.Args[idx : idx+numStreams]
	streamIDs := ctx.Args[idx+numStreams:]
	result := make([]resp.RESPValue, 0)
	for i, streamKey := range streamKeys {
		val := ctx.DB.LookupKey(streamKey)
		if val == nil || val.Type != object.TypeStream {
			continue
		}
		stream := val.Ptr.(*object.Stream)
		group, exists := stream.Groups[groupName]
		if !exists {
			ctx.Client.SendError("ERR no such consumer group")
			return
		}
		// 获取或创建消费者
		consumer, exists := group.Consumers[consumerName]
		if !exists {
			consumer = &object.StreamConsumer{
				Name:     consumerName,
				SeenTime: time.Now(),
				Pending:  make(map[string]time.Time),
			}
			group.Consumers[consumerName] = consumer
		}
		afterID := streamIDs[i]
		if afterID == ">" {
			afterID = group.LastID
		}
		entries := make([]resp.RESPValue, 0)
		for _, entry := range stream.Entries {
			if streamIDCompare(entry.ID, afterID) > 0 {
				entries = append(entries, streamEntryToResp(entry))
				// 添加到 PEL（Pending Entries List）
				consumer.Pending[entry.ID] = time.Now()
				consumer.SeenTime = time.Now()
				group.LastID = entry.ID
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}
		if len(entries) > 0 {
			result = append(result, resp.ArrayReply([]resp.RESPValue{
				resp.BulkStringReply(streamKey),
				resp.ArrayReply(entries),
			}))
		}
	}
	if len(result) == 0 {
		ctx.Client.SendNil()
		return
	}
	ctx.Client.SendArray(result)
}

// XACK <key> <group> <id> [<id> ...]
func xackCommand(ctx *CommandContext) {
	if len(ctx.Args) < 4 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xack' command")
		return
	}
	key := ctx.Args[2]
	groupName := ctx.Args[2]
	ids := ctx.Args[3:]
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeStream {
		ctx.Client.SendInteger(0)
		return
	}
	stream := val.Ptr.(*object.Stream)
	group, exists := stream.Groups[groupName]
	if !exists {
		ctx.Client.SendInteger(0)
		return
	}
	acked := int64(0)
	for _, id := range ids {
		for _, consumer := range group.Consumers {
			if _, pending := consumer.Pending[id]; pending {
				delete(consumer.Pending, id)
				acked++
			}
		}
	}
	ctx.Client.SendInteger(acked)
}

// XCLAIM <key> <group> <consumer> <min-idle-time> <id> [<id>...] [IDLE <ms>] [TIME <ms-unix-time>] [RETRYCOUNT <count>] [FORCE] [JUSTID]
func xclaimCommand(ctx *CommandContext) {
	if len(ctx.Args) < 6 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xclaim' command")
		return
	}
	key := ctx.Args[2]
	groupName := ctx.Args[2]
	consumerName := ctx.Args[3]
	_, _ = strconv.ParseInt(ctx.Args[4], 10, 64) // min-idle-time
	ids := ctx.Args[5:]
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeStream {
		ctx.Client.SendArray([]resp.RESPValue{})
		return
	}
	stream := val.Ptr.(*object.Stream)
	group, exists := stream.Groups[groupName]
	if !exists {
		ctx.Client.SendError("ERR no such consumer group")
		return
	}
	// 获取或创建消费者
	consumer, exists := group.Consumers[consumerName]
	if !exists {
		consumer = &object.StreamConsumer{
			Name:     consumerName,
			SeenTime: time.Now(),
			Pending:  make(map[string]time.Time),
		}
		group.Consumers[consumerName] = consumer
	}
	result := make([]resp.RESPValue, 0)
	for _, id := range ids {
		// 从原消费者 PEL 移除，添加到新消费者 PEL
		for _, c := range group.Consumers {
			delete(c.Pending, id)
		}
		consumer.Pending[id] = time.Now()
		// 查找对应的条目
		for _, entry := range stream.Entries {
			if entry.ID == id {
				result = append(result, streamEntryToResp(entry))
				break
			}
		}
	}
	ctx.Client.SendArray(result)
}

// XPENDING <key> <group> [[IDLE <min-idle-time>] <start> <stop> <count> [<consumer>]]
func xpendingCommand(ctx *CommandContext) {
	if len(ctx.Args) < 3 {
		ctx.Client.SendError("ERR wrong number of arguments for 'xpending' command")
		return
	}
	key := ctx.Args[2]
	groupName := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil || val.Type != object.TypeStream {
		ctx.Client.SendArray(nil)
		return
	}
	stream := val.Ptr.(*object.Stream)
	group, exists := stream.Groups[groupName]
	if !exists {
		ctx.Client.SendError("ERR no such consumer group")
		return
	}
	// 简化版：返回汇总信息
	totalPending := 0
	minID := ""
	maxID := ""
	consumers := make(map[string]int)
	for _, consumer := range group.Consumers {
		for id := range consumer.Pending {
			totalPending++
			consumers[consumer.Name]++
			if minID == "" || streamIDCompare(id, minID) < 0 {
				minID = id
			}
			if maxID == "" || streamIDCompare(id, maxID) > 0 {
				maxID = id
			}
		}
	}
	if totalPending == 0 {
		ctx.Client.SendArray([]resp.RESPValue{
			resp.IntegerReply(0),
			resp.NullArray,
			resp.NullArray,
			resp.NullArray,
		})
		return
	}
	consumerList := make([]resp.RESPValue, 0)
	for name, count := range consumers {
		consumerList = append(consumerList, resp.ArrayReply([]resp.RESPValue{
			resp.BulkStringReply(name),
			resp.BulkStringReply(fmt.Sprintf("%d", count)),
		}))
	}
	ctx.Client.SendArray([]resp.RESPValue{
		resp.IntegerReply(int64(totalPending)),
		resp.BulkStringReply(minID),
		resp.BulkStringReply(maxID),
		resp.ArrayReply(consumerList),
	})
}

func xsetidCommand(ctx *CommandContext) {
	key := ctx.Args[2]
	id := ctx.Args[2]
	val := ctx.DB.LookupKey(key)
	if val == nil {
		ctx.Client.SendError("ERR no such key")
		return
	}
	if val.Type != object.TypeStream {
		ctx.Client.ReplyTypeMismatch()
		return
	}
	val.Ptr.(*object.Stream).LastID = id
	ctx.Client.SendOK()
}

func xautoclaimCommand(ctx *CommandContext) {
	ctx.Client.SendArray([]resp.RESPValue{
		resp.BulkStringReply("0-0"),
		resp.ArrayReply([]resp.RESPValue{}),
	})
}

// Helper functions
func streamIDCompare(entryID, boundary string) int {
	// Handle special boundaries
	if boundary == "-" {
		// "-" means minimum possible ID, every entry is >= "-"
		return 1
	}
	if boundary == "+" {
		// "+" means maximum possible ID, every entry is <= "+"
		return -1
	}
	// Parse "ms-seq" format
	var eMs, eSeq, bMs, bSeq int64
	fmt.Sscanf(entryID, "%d-%d", &eMs, &eSeq)
	fmt.Sscanf(boundary, "%d-%d", &bMs, &bSeq)
	if eMs < bMs {
		return -1
	}
	if eMs > bMs {
		return 1
	}
	if eSeq < bSeq {
		return -1
	}
	if eSeq > bSeq {
		return 1
	}
	return 0
}

func streamEntryToResp(entry object.StreamEntry) resp.RESPValue {
	fields := make([]resp.RESPValue, 0, len(entry.Fields)*2+2)
	fields = append(fields, resp.BulkStringReply(entry.ID))
	fieldArray := make([]resp.RESPValue, 0, len(entry.Fields)*2)
	for k, v := range entry.Fields {
		fieldArray = append(fieldArray, resp.BulkStringReply(k), resp.BulkStringReply(v))
	}
	fields = append(fields, resp.ArrayReply(fieldArray))
	return resp.ArrayReply(fields)
}
