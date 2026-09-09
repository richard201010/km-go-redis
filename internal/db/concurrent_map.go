// Package db 提供自定义并发哈希表，替代 sync.Map 以降低锁竞争。
// 对应 Redis 的 dict.c，采用分片锁策略。
package db

import (
	"sync"
	"sync/atomic"
)

const (
	// 分片数量，必须是2的幂
	shardCount = 256
	shardMask  = shardCount - 1
)

// ConcurrentMap 是一个分片并发哈希表，对应 Redis 的 dict。
// 使用多个独立的锁来降低并发写入时的竞争。
type ConcurrentMap struct {
	shards [shardCount]*shard
	size   int64
}

// shard 是哈希表的一个分片，对应 Redis dict 中的单个哈希桶数组。
type shard struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// NewConcurrentMap 创建新的并发哈希表。
func NewConcurrentMap() *ConcurrentMap {
	cm := &ConcurrentMap{}
	for i := 0; i < shardCount; i++ {
		cm.shards[i] = &shard{
			data: make(map[string]interface{}, 16),
		}
	}
	return cm
}

// getShard 根据键计算所属分片。
// 使用 FNV-1a 哈希，与 Redis 的 dictGetHash 类似。
func (cm *ConcurrentMap) getShard(key string) *shard {
	h := fnv32(key)
	return cm.shards[h&shardMask]
}

// Store 存储键值对，对应 Redis 的 dictAdd/dictReplace。
func (cm *ConcurrentMap) Store(key string, value interface{}) {
	s := cm.getShard(key)
	s.mu.Lock()
	if _, exists := s.data[key]; !exists {
		atomic.AddInt64(&cm.size, 1)
	}
	s.data[key] = value
	s.mu.Unlock()
}

// Load 加载键对应的值，对应 Redis 的 dictFind。
func (cm *ConcurrentMap) Load(key string) (interface{}, bool) {
	s := cm.getShard(key)
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	return val, ok
}

// LoadAndDelete 加载并删除键，对应 Redis 的 dictUnlink。
func (cm *ConcurrentMap) LoadAndDelete(key string) (interface{}, bool) {
	s := cm.getShard(key)
	s.mu.Lock()
	val, ok := s.data[key]
	if ok {
		delete(s.data, key)
		atomic.AddInt64(&cm.size, -1)
	}
	s.mu.Unlock()
	return val, ok
}

// Delete 删除键，对应 Redis 的 dictDelete。
func (cm *ConcurrentMap) Delete(key string) {
	s := cm.getShard(key)
	s.mu.Lock()
	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		atomic.AddInt64(&cm.size, -1)
	}
	s.mu.Unlock()
}

// Swap 交换键值对，返回旧值和是否存在。
func (cm *ConcurrentMap) Swap(key string, value interface{}) (interface{}, bool) {
	s := cm.getShard(key)
	s.mu.Lock()
	old, exists := s.data[key]
	if !exists {
		atomic.AddInt64(&cm.size, 1)
	}
	s.data[key] = value
	s.mu.Unlock()
	return old, exists
}

// Len 返回键值对数量，对应 Redis 的 dictSize。
func (cm *ConcurrentMap) Len() int64 {
	return atomic.LoadInt64(&cm.size)
}

// Range 遍历所有键值对，对应 Redis 的 dictGetIterator + dictNext。
// 注意：遍历期间其他 goroutine 可以并发修改。
func (cm *ConcurrentMap) Range(fn func(key string, value interface{}) bool) {
	for i := 0; i < shardCount; i++ {
		s := cm.shards[i]
		s.mu.RLock()
		for k, v := range s.data {
			if !fn(k, v) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}

// Keys 返回所有键。
func (cm *ConcurrentMap) Keys() []string {
	keys := make([]string, 0, atomic.LoadInt64(&cm.size))
	cm.Range(func(key string, _ interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Values 返回所有值。
func (cm *ConcurrentMap) Values() []interface{} {
	vals := make([]interface{}, 0, atomic.LoadInt64(&cm.size))
	cm.Range(func(_ string, value interface{}) bool {
		vals = append(vals, value)
		return true
	})
	return vals
}

// Clear 清空所有键值对。
func (cm *ConcurrentMap) Clear() {
	for i := 0; i < shardCount; i++ {
		s := cm.shards[i]
		s.mu.Lock()
		s.data = make(map[string]interface{}, 16)
		s.mu.Unlock()
	}
	atomic.StoreInt64(&cm.size, 0)
}

// fnv32 计算 FNV-1a 哈希值。
// 对应 Redis 的 dictGenHashFunction。
func fnv32(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// RangeFrom 从指定全局游标开始遍历 (用于增量过期扫描).
// cursor 是全局游标，会映射到具体的 shard 和 shard 内偏移。
func (cm *ConcurrentMap) RangeFrom(cursor int, fn func(key string, value interface{}) bool) {
	totalShards := shardCount
	if totalShards == 0 {
		return
	}
	// 计算起始 shard 和 shard 内偏移
	startShard := cursor % totalShards
	offset := cursor / totalShards

	scanned := 0
	for i := 0; i < totalShards; i++ {
		shardIdx := (startShard + i) % totalShards
		s := cm.shards[shardIdx]
		s.mu.RLock()
		idx := 0
		for k, v := range s.data {
			if idx >= offset || i > 0 {
				scanned++
				if !fn(k, v) {
					s.mu.RUnlock()
					return
				}
			}
			idx++
		}
		s.mu.RUnlock()
		offset = 0 // 只在第一个 shard 使用 offset
	}
}
