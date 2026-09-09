// Package db 实现 Redis 数据库层.
// 这是 Redis's db.c, managing the keyspace,
// key expiration, and multi-database support.
package db

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/km-dev/km-go-redis/internal/object"
)

const (
	MaxDBs       = 16 // Default number of databases
	SharedHeader = -1 // Shared object refcount
)

// DB 表示单个 Redis 数据库 (redisDb in Redis).
// It holds the keyspace, expiration table, and blocking key state.
type DB struct {
	ID        int
	Keys      *ConcurrentMap // key -> *object.Object
	Expires   *ConcurrentMap // key -> time.Time
	AvgTTL    int64
	stats     DBStats
}

// DBStats tracks per-database statistics.
type DBStats struct {
	Keys      int64
	Expires   int64
	AvgTTL    int64
}

// Database 是全局数据库数组, matching Redis's server.db.
type Database struct {
	Dbs     []*DB
	Count   int
	Current int // Currently selected DB index
}

// New 创建新的数据库数组 with the given number of databases.
func New(count int) *Database {
	db := &Database{
		Dbs:   make([]*DB, count),
		Count: count,
	}
	for i := 0; i < count; i++ {
		db.Dbs[i] = &DB{ID: i, Keys: NewConcurrentMap(), Expires: NewConcurrentMap()}
	}
	return db
}

// GetDB 返回指定索引的数据库.
func (d *Database) GetDB(index int) (*DB, error) {
	if index < 0 || index >= d.Count {
		return nil, fmt.Errorf("ERR invalid DB index")
	}
	return d.Dbs[index], nil
}

// Select 切换当前数据库.
func (d *Database) Select(index int) error {
	if index < 0 || index >= d.Count {
		return fmt.Errorf("ERR invalid DB index")
	}
	d.Current = index
	return nil
}

// --- Key Operations (matching Redis's db.c lookupKey*, setKey*, deleteKey*) ---

// LookupKey 在数据库中查找键. Returns nil if not found or expired.
// This matches Redis's lookupKeyRead().
func (db *DB) LookupKey(key string) *object.Object {
	// Check expiration first
	if db.isExpired(key) {
		db.deleteKey(key)
		return nil
	}
	val, ok := db.Keys.Load(key)
	if !ok {
		return nil
	}
	return val.(*object.Object)
}

// LookupKeyWrite finds a key for write operations (no lazy expire in simplified version).
func (db *DB) LookupKeyWrite(key string) *object.Object {
	return db.LookupKey(key)
}

// LookupKeyOrCreate finds a key or creates it with the given type.
func (db *DB) LookupKeyOrCreate(key string, typ byte) *object.Object {
	val := db.LookupKeyWrite(key)
	if val != nil {
		return val
	}
	var newObj *object.Object
	switch typ {
	case object.TypeString:
		newObj = object.NewStringObject("")
	case object.TypeList:
		newObj = object.NewListObject()
	case object.TypeSet:
		newObj = object.NewSetObject()
	case object.TypeHash:
		newObj = object.NewHashObject()
	case object.TypeZSet:
		newObj = object.NewZSetObject()
	case object.TypeStream:
		newObj = object.NewStreamObject()
	default:
		return nil
	}
	db.SetKey(key, newObj)
	return newObj
}

// SetKey 存储键值对 in the database.
// This matches Redis's setKey().
func (db *DB) SetKey(key string, val *object.Object) {
	_, loaded := db.Keys.Swap(key, val)
	if !loaded {
		atomic.AddInt64(&db.stats.Keys, 1)
	}
}

// DeleteKey 删除键 from the database.
// This matches Redis's dbDelete().
func (db *DB) DeleteKey(key string) bool {
	_, loaded := db.Keys.LoadAndDelete(key)
	if loaded {
		atomic.AddInt64(&db.stats.Keys, -1)
		db.Expires.Delete(key)
		return true
	}
	return false
}

// deleteKey is an internal method that also cleans up expiration.
func (db *DB) deleteKey(key string) {
	_, loaded := db.Keys.LoadAndDelete(key)
	if loaded {
		atomic.AddInt64(&db.stats.Keys, -1)
	}
	db.Expires.Delete(key)
}

// Exists 检查键是否存在 in the database.
func (db *DB) Exists(key string) bool {
	return db.LookupKey(key) != nil
}

// Type 返回值的类型 stored at key.
func (db *DB) Type(key string) string {
	val := db.LookupKey(key)
	if val == nil {
		return "none"
	}
	return object.TypeName(val.Type)
}

// Rename 重命名键.
func (db *DB) Rename(oldKey, newKey string) error {
	val := db.LookupKey(oldKey)
	if val == nil {
		return fmt.Errorf("ERR no such key")
	}
	db.DeleteKey(newKey) // Remove new key if it exists
	db.SetKey(newKey, val)
	// Copy TTL if exists
	if expireAt, ok := db.Expires.Load(oldKey); ok {
		db.Expires.Store(newKey, expireAt)
	}
	db.DeleteKey(oldKey)
	return nil
}

// RenameNX renames a key only if the new key does not exist.
func (db *DB) RenameNX(oldKey, newKey string) (bool, error) {
	if db.Exists(newKey) {
		return false, nil
	}
	err := db.Rename(oldKey, newKey)
	return err == nil, err
}

// RandomKey 返回随机键 from the database.
func (db *DB) RandomKey() string {
	var keys []string
	db.Keys.Range(func(key string, _ interface{}) bool {
		keys = append(keys, key)
		return len(keys) < 20 // Collect up to 20 keys
	})
	if len(keys) == 0 {
		return ""
	}
	return keys[rand.Intn(len(keys))]
}

// GetKeys 返回匹配的所有键 the given pattern.
// This matches Redis's KEYS command behavior.
func (db *DB) GetKeys(pattern string) []string {
	var result []string
	matchAll := pattern == "*"
	db.Keys.Range(func(key string, _ interface{}) bool {
		k := key
		if matchAll || matchPattern(pattern, k) {
			if !db.isExpired(k) {
				result = append(result, k)
			}
		}
		return true
	})
	return result
}

// Scan 迭代键空间 using a cursor.
// Returns (newCursor, keys).
func (db *DB) Scan(cursor uint64, pattern string, count int) (uint64, []string) {
	var allKeys []string
	db.Keys.Range(func(key string, _ interface{}) bool {
		allKeys = append(allKeys, key)
		return true
	})
	if len(allKeys) == 0 {
		return 0, nil
	}
	if count <= 0 {
		count = 10
	}
	start := int(cursor)
	if start >= len(allKeys) {
		start = 0
	}
	var result []string
	end := start + count
	if end > len(allKeys) {
		end = len(allKeys)
	}
	for _, k := range allKeys[start:end] {
		if pattern == "" || pattern == "*" || matchPattern(pattern, k) {
			if !db.isExpired(k) {
				result = append(result, k)
			}
		}
	}
	newCursor := uint64(end)
	if newCursor >= uint64(len(allKeys)) {
		newCursor = 0
	}
	return newCursor, result
}

// DBSize 返回键数量 in the database.
func (db *DB) DBSize() int64 {
	count := int64(0)
	db.Keys.Range(func(_ string, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// --- Expiration (matching Redis's expire.c) ---

// SetExpire 设置过期时间 for a key.
func (db *DB) SetExpire(key string, expireAt time.Time) {
	db.Expires.Store(key, expireAt)
}

// GetExpire 返回过期时间 for a key.
func (db *DB) GetExpire(key string) (time.Time, bool) {
	v, ok := db.Expires.Load(key)
	if !ok {
		return time.Time{}, false
	}
	return v.(time.Time), true
}

// TTL 返回键的 TTL in seconds (-1 if no expire, -2 if not found).
func (db *DB) TTL(key string) int64 {
	if !db.Exists(key) {
		return -2
	}
	expireAt, ok := db.Expires.Load(key)
	if !ok {
		return -1
	}
	ttl := time.Until(expireAt.(time.Time)).Seconds()
	if ttl <= 0 {
		// Expired, clean up
		db.deleteKey(key)
		return -2
	}
	return int64(ttl)
}

// PTTL 返回键的毫秒 TTL.
func (db *DB) PTTL(key string) int64 {
	if !db.Exists(key) {
		return -2
	}
	expireAt, ok := db.Expires.Load(key)
	if !ok {
		return -1
	}
	ttl := time.Until(expireAt.(time.Time)).Milliseconds()
	if ttl <= 0 {
		db.deleteKey(key)
		return -2
	}
	return ttl
}

// Persist 移除过期时间 from a key.
func (db *DB) Persist(key string) bool {
	_, ok := db.Expires.LoadAndDelete(key)
	return ok
}

// isExpired 检查键是否过期.
func (db *DB) isExpired(key string) bool {
	expireAt, ok := db.Expires.Load(key)
	if !ok {
		return false
	}
	return time.Now().After(expireAt.(time.Time))
}

// ExpireIfNeeded lazily expires a key if needed.
// Returns true if the key was expired and deleted.
func (db *DB) ExpireIfNeeded(key string) bool {
	if db.isExpired(key) {
		db.deleteKey(key)
		return true
	}
	return false
}

// ActiveExpireCycle 执行主动过期 (matching Redis's activeExpireCycle).
// This should be called periodically to clean up expired keys.
func (db *DB) ActiveExpireCycle(sampleSize int) int {
	if sampleSize <= 0 {
		sampleSize = 20
	}
	expired := 0
	var keys []string
	db.Expires.Range(func(key string, _ interface{}) bool {
		keys = append(keys, key)
		return len(keys) < sampleSize*10
	})
	for _, key := range keys {
		if db.isExpired(key) {
			db.deleteKey(key)
			expired++
			if expired >= sampleSize {
				break
			}
		}
	}
	return expired
}

// FlushDB 清空当前数据库.
func (db *DB) FlushDB() {
	// 先收集所有键，再删除（避免在 Range 回调中 Delete 导致死锁）
	var keys []string
	db.Keys.Range(func(key string, _ interface{}) bool {
		keys = append(keys, key)
		return true
	})
	for _, key := range keys {
		db.Keys.Delete(key)
	}
	var expireKeys []string
	db.Expires.Range(func(key string, _ interface{}) bool {
		expireKeys = append(expireKeys, key)
		return true
	})
	for _, key := range expireKeys {
		db.Expires.Delete(key)
	}
	atomic.StoreInt64(&db.stats.Keys, 0)
}

// FlushAll 清空所有数据库.
func (d *Database) FlushAll() {
	for _, db := range d.Dbs {
		db.FlushDB()
	}
}

// Shared integer objects (matching Redis's shared.integers).
var SharedIntegers [10000]*object.Object

func init() {
	for i := 0; i < 10000; i++ {
		SharedIntegers[i] = object.NewStringObjectFromInt(int64(i))
		SharedIntegers[i].RefCount = SharedHeader
	}
}

// GetSharedInteger returns a shared integer object.
func GetSharedInteger(n int64) *object.Object {
	if n >= 0 && n < 10000 {
		return SharedIntegers[n]
	}
	return object.NewStringObjectFromInt(n)
}

// --- Helper functions ---

// matchPattern matches a key against a glob pattern (like Redis's stringmatchlen).
func matchPattern(pattern, key string) bool {
	return matchPatternLen(pattern, key)
}

func matchPatternLen(pattern, key string) bool {
	pi, ki := 0, 0
	pLen, kLen := len(pattern), len(key)
	for pi < pLen || ki < kLen {
		if pi < pLen {
			switch pattern[pi] {
			case '*':
				// Match any sequence
				pi++
				if pi == pLen {
					return true // * at end matches everything
				}
				// Try matching at each position
				for i := ki; i <= kLen; i++ {
					if matchPatternLen(pattern[pi:], key[i:]) {
						return true
					}
				}
				return false
			case '?':
				if ki >= kLen {
					return false
				}
				pi++
				ki++
				continue
			case '[':
				if ki >= kLen {
					return false
				}
				pi++
				negate := false
				if pi < pLen && pattern[pi] == '^' {
					negate = true
					pi++
				}
				matched := false
				if pi < pLen && pattern[pi] == ']' {
					// ] as first char is literal
					if key[ki] == ']' {
						matched = true
					}
					pi++
				}
				for pi < pLen && pattern[pi] != ']' {
					if pi+2 < pLen && pattern[pi+1] == '-' {
						// Range: a-z
						if key[ki] >= pattern[pi] && key[ki] <= pattern[pi+2] {
							matched = true
						}
						pi += 3
					} else {
						if key[ki] == pattern[pi] {
							matched = true
						}
						pi++
					}
				}
				if pi < pLen {
					pi++ // skip ]
				}
				if negate {
					matched = !matched
				}
				if !matched {
					return false
				}
				ki++
				continue
			case '\\':
				pi++
				if pi >= pLen {
					return false
				}
				fallthrough
			default:
				if ki >= kLen || pattern[pi] != key[ki] {
					return false
				}
				pi++
				ki++
				continue
			}
		}
		return false
	}
	return pi == pLen && ki == kLen
}

// SortFunc is the sort implementation for LIST/SET/ZSET (matching Redis's sort.c).
func SortFunc(values []string, desc bool, alpha bool, limit *SortLimit) []string {
	result := make([]string, len(values))
	copy(result, values)

	// Simple comparison
	for i := 1; i < len(result); i++ {
		key := result[i]
		j := i - 1
		for j >= 0 {
			less := false
			if alpha {
				less = strings.Compare(result[j], key) > 0
			} else {
				// Try numeric comparison
				var a, b float64
				fmt.Sscanf(result[j], "%f", &a)
				fmt.Sscanf(key, "%f", &b)
				less = a > b
			}
			if desc {
				less = !less
			}
			if !less {
				break
			}
			result[j+1] = result[j]
			j--
		}
		result[j+1] = key
	}

	if limit != nil {
		start := limit.Offset
		if start > len(result) {
			return nil
		}
		end := start + limit.Count
		if end > len(result) {
			end = len(result)
		}
		return result[start:end]
	}
	return result
}

// SortLimit represents offset/count for SORT command.
type SortLimit struct {
	Offset int
	Count  int
}
