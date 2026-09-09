// Package object 实现 Redis 对象系统 (robj).
// 这是 Redis's object.h/object.c.
//
// In Redis, every value is wrapped in a robj (redisObject) which contains:
//   - type: OBJ_STRING, OBJ_LIST, OBJ_SET, OBJ_ZSET, OBJ_HASH, OBJ_STREAM
//   - encoding: how the value is stored in memory
//   - refcount: reference counting for memory management
//   - ptr: pointer to the actual data
//
// In Go, we use interfaces and structs to model this.
package object

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 对象类型常量（对应 Redis's OBJ_* constants).
const (
	TypeString = 0
	TypeList   = 1
	TypeSet    = 2
	TypeZSet   = 3
	TypeHash   = 4
	TypeStream = 5
	TypeModule = 6
)

// 对象编码常量（对应 Redis's OBJ_ENCODING_* constants).
const (
	EncRaw     = 0 // 原始 SDS 字符串
	EncInt     = 1 // 整数（存储在 ptr 中）
	EncEmbStr  = 2 // 嵌入式 SDS 字符串 (small strings)
	EncHT      = 3 // 哈希表
	EncZipMap  = 4 // Zipmap (deprecated)
	EncList    = 5 // 链表
	EncIntSet  = 6 // 整数集合
	EncSkipList = 7 // 跳表
	EncZSet    = 8 // 有序集合 (skip list + hash table)
	EncQuickList = 9  // 快速列表
	EncZipList = 10 // 压缩列表
	EncListPack = 11 // Listpack (replaces ziplist in Redis 7+)
	EncStream  = 12 // 流
	EncLinkedList = 13 // 通用链表
)

// TypeName returns the string name for an object type (matching Redis's getObjectTypeName).
func TypeName(t byte) string {
	switch t {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	case TypeHash:
		return "hash"
	case TypeStream:
		return "stream"
	case TypeModule:
		return "module"
	default:
		return "unknown"
	}
}

// EncodingName returns the string name for an object encoding.
func EncodingName(e byte) string {
	switch e {
	case EncRaw:
		return "raw"
	case EncInt:
		return "int"
	case EncEmbStr:
		return "embstr"
	case EncHT:
		return "hashtable"
	case EncList:
		return "list"
	case EncIntSet:
		return "intset"
	case EncSkipList:
		return "skiplist"
	case EncZSet:
		return "ziplist"
	case EncQuickList:
		return "quicklist"
	case EncZipList:
		return "ziplist"
	case EncListPack:
		return "listpack"
	case EncStream:
		return "stream"
	case EncLinkedList:
		return "linkedlist"
	default:
		return "unknown"
	}
}

// Object is the Go equivalent of Redis's redisObject (robj).
// It wraps a value with type/encoding metadata and reference counting.
type Object struct {
	Type     byte        // OBJ_STRING, OBJ_LIST, etc.
	Encoding byte        // OBJ_ENCODING_RAW, OBJ_ENCODING_INT, etc.
	RefCount int32       // Reference count (atomic)
	LRU      int64       // LRU clock or LFU data
	Ptr      interface{} // The actual value
}

var objectPool = sync.Pool{
	New: func() interface{} {
		return &Object{}
	},
}

// New creates a new Object with the given type, encoding, and value.
func New(typ, enc byte, ptr interface{}) *Object {
	o := objectPool.Get().(*Object)
	o.Type = typ
	o.Encoding = enc
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = ptr
	return o
}

// IncrRefCount increments the reference count.
func (o *Object) IncrRefCount() {
	atomic.AddInt32(&o.RefCount, 1)
}

// DecrRefCount decrements the reference count and frees if zero.
// Returns true if the object was freed.
func (o *Object) DecrRefCount() bool {
	newCount := atomic.AddInt32(&o.RefCount, -1)
	if newCount == 0 {
		o.freeObject()
		return true
	}
	if newCount < 0 {
		panic(fmt.Sprintf("Object refcount < 0: type=%d encoding=%d", o.Type, o.Encoding))
	}
	return false
}

func (o *Object) freeObject() {
	o.Ptr = nil
	o.Type = 0
	o.Encoding = 0
	o.RefCount = 0
	objectPool.Put(o)
}

// GetRefcount returns the current reference count.
func (o *Object) GetRefcount() int32 {
	return atomic.LoadInt32(&o.RefCount)
}

// IsShared returns true if this is a shared/cached object.
func (o *Object) IsShared() bool {
	return o.RefCount == -1 // OBJ_STATIC_REFCOUNT or OBJ_SHARED_REFCOUNT
}

// String implements Stringer for debugging.
func (o *Object) String() string {
	return fmt.Sprintf("Object{type=%s enc=%s refcount=%d ptr=%v}",
		TypeName(o.Type), EncodingName(o.Encoding), o.RefCount, o.Ptr)
}

// --- String helpers ---

// NewStringObject creates a new string object with optimal encoding.
func NewStringObject(s string) *Object {
	// Try integer encoding first (like Redis's tryObjectEncoding)
	if len(s) > 0 && len(s) <= 20 {
		var n int64
		neg := false
		start := 0
		if s[0] == '-' {
			neg = true
			start = 1
		}
		valid := start < len(s)
		for i := start; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				valid = false
				break
			}
			n = n*10 + int64(s[i]-'0')
		}
		if valid {
			if neg {
				n = -n
			}
			return New(TypeString, EncInt, n)
		}
	}

	// Small strings use embstr encoding (like Redis)
	if len(s) <= 44 {
		return New(TypeString, EncEmbStr, s)
	}
	return New(TypeString, EncRaw, s)
}

// NewStringObjectFromInt creates an integer-encoded string object.
func NewStringObjectFromInt(n int64) *Object {
	return New(TypeString, EncInt, n)
}

// GetString returns the string value from a string object.
func (o *Object) GetString() string {
	switch o.Encoding {
	case EncInt:
		return fmt.Sprintf("%d", o.Ptr.(int64))
	case EncEmbStr, EncRaw:
		return o.Ptr.(string)
	default:
		return ""
	}
}

// GetInt returns the integer value if this is an integer-encoded string.
func (o *Object) GetInt() (int64, bool) {
	if o.Encoding == EncInt {
		return o.Ptr.(int64), true
	}
	return 0, false
}

// GetStringLen returns the length of the string value.
func (o *Object) GetStringLen() int {
	switch o.Encoding {
	case EncInt:
		return len(fmt.Sprintf("%d", o.Ptr.(int64)))
	case EncEmbStr, EncRaw:
		return len(o.Ptr.(string))
	default:
		return 0
	}
}

// Append appends to the string value, converting encoding if needed.
func (o *Object) Append(s string) {
	switch o.Encoding {
	case EncInt:
		// Convert to raw encoding
		str := fmt.Sprintf("%d", o.Ptr.(int64)) + s
		o.Encoding = EncRaw
		o.Ptr = str
	case EncEmbStr:
		str := o.Ptr.(string) + s
		if len(str) <= 44 {
			o.Ptr = str
		} else {
			o.Encoding = EncRaw
			o.Ptr = str
		}
	case EncRaw:
		o.Ptr = o.Ptr.(string) + s
	}
}

// --- Numeric helpers ---

// IncrementInt increments an integer-encoded string object by the given delta.
func (o *Object) IncrementInt(delta int64) (*Object, error) {
	if o.Encoding == EncInt {
		n := o.Ptr.(int64) + delta
		return NewStringObjectFromInt(n), nil
	}
	// Try to parse as integer
	s := o.GetString()
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return nil, fmt.Errorf("ERR value is not an integer or out of range")
	}
	return NewStringObjectFromInt(n + delta), nil
}

// IncrementFloat increments a string object by a float delta.
func (o *Object) IncrementFloat(delta float64) (*Object, error) {
	s := o.GetString()
	var f float64
	if o.Encoding == EncInt {
		f = float64(o.Ptr.(int64))
	} else {
		_, err := fmt.Sscanf(s, "%f", &f)
		if err != nil {
			return nil, fmt.Errorf("ERR value is not a valid float")
		}
	}
	f += delta
	return NewStringObject(fmt.Sprintf("%g", f)), nil
}

// --- List helpers ---

// NewListObject creates a new list object.
func NewListObject() *Object {
	return New(TypeList, EncQuickList, NewQuickList())
}

// --- Set helpers ---

// NewSetObject creates a new set object.
func NewSetObject() *Object {
	return New(TypeSet, EncHT, NewSet())
}

// --- Hash helpers ---

// NewHashObject creates a new hash object.
func NewHashObject() *Object {
	return New(TypeHash, EncHT, NewHash())
}

// --- Sorted set helpers ---

// NewZSetObject creates a new sorted set object.
func NewZSetObject() *Object {
	return New(TypeZSet, EncSkipList, NewZSet())
}

// --- Stream helpers ---

// NewStreamObject creates a new stream object.
func NewStreamObject() *Object {
	return New(TypeStream, EncStream, NewStream())
}

// --- TTL support ---

// TTLEntry pairs a key with its expiration time.
type TTLEntry struct {
	ExpireAt time.Time
}

// --- QuickList (for OBJ_LIST) ---

// QuickList is a doubly-ended queue optimized for O(1) push/pop at both ends.
// Uses two slices: 'left' (reversed, grows toward index 0) and 'right' (grows toward end).
// This avoids the O(n) copy of prepend on a single slice.
type QuickList struct {
	Length    int
	left      []interface{} // elements before the logical start, stored in reverse
	right     []interface{} // elements from logical start onward
}

// NewQuickList creates a new quicklist.
func NewQuickList() *QuickList {
	return &QuickList{
		left:  make([]interface{}, 0, 16),
		right: make([]interface{}, 0, 16),
	}
}

func (ql *QuickList) Len() int { return ql.Length }

func (ql *QuickList) PushLeft(v interface{}) {
	ql.left = append(ql.left, v)
	ql.Length++
}

func (ql *QuickList) PushRight(v interface{}) {
	ql.right = append(ql.right, v)
	ql.Length++
}

func (ql *QuickList) PopLeft() (interface{}, bool) {
	if ql.Length == 0 {
		return nil, false
	}
	ql.Length--
	// Pop from left slice (which stores elements in reverse logical order)
	if len(ql.left) > 0 {
		v := ql.left[len(ql.left)-1]
		ql.left = ql.left[:len(ql.left)-1]
		return v, true
	}
	// Fall through to right slice
	v := ql.right[0]
	ql.right = ql.right[1:]
	return v, true
}

func (ql *QuickList) PopRight() (interface{}, bool) {
	if ql.Length == 0 {
		return nil, false
	}
	ql.Length--
	if len(ql.right) > 0 {
		v := ql.right[len(ql.right)-1]
		ql.right = ql.right[:len(ql.right)-1]
		return v, true
	}
	// Fall through to left slice (which stores in reverse)
	v := ql.left[len(ql.left)-1]
	ql.left = ql.left[:len(ql.left)-1]
	return v, true
}

func (ql *QuickList) at(i int) interface{} {
	if i < len(ql.left) {
		// Access from left slice (reversed): logical index i maps to left[len(left)-1-i]
		return ql.left[len(ql.left)-1-i]
	}
	// Access from right slice: logical index i-leftLen maps to right[i-leftLen]
	return ql.right[i-len(ql.left)]
}

func (ql *QuickList) Index(i int) (interface{}, bool) {
	if i < 0 {
		i = ql.Length + i
	}
	if i < 0 || i >= ql.Length {
		return nil, false
	}
	return ql.at(i), true
}

func (ql *QuickList) Set(i int, v interface{}) bool {
	if i < 0 {
		i = ql.Length + i
	}
	if i < 0 || i >= ql.Length {
		return false
	}
	if i < len(ql.left) {
		ql.left[len(ql.left)-1-i] = v
	} else {
		ql.right[i-len(ql.left)] = v
	}
	return true
}

func (ql *QuickList) Insert(i int, v interface{}) bool {
	if i < 0 {
		i = ql.Length + i
	}
	if i < 0 || i > ql.Length {
		return false
	}
	// Convert to flat slice, insert, then re-split
	flat := make([]interface{}, 0, ql.Length+1)
	for j := 0; j < ql.Length; j++ {
		flat = append(flat, ql.at(j))
	}
	flat = append(flat, nil)
	copy(flat[i+1:], flat[i:])
	flat[i] = v
	ql.left = ql.left[:0]
	ql.right = flat
	ql.Length++
	return true
}

func (ql *QuickList) RemoveRange(start, count int) {
	if start < 0 {
		start = ql.Length + start
	}
	if start < 0 {
		start = 0
	}
	if start+count > ql.Length {
		count = ql.Length - start
	}
	// Convert to flat, remove, re-split
	flat := make([]interface{}, 0, ql.Length-count)
	for j := 0; j < ql.Length; j++ {
		if j < start || j >= start+count {
			flat = append(flat, ql.at(j))
		}
	}
	ql.left = ql.left[:0]
	ql.right = flat
	ql.Length -= count
}

func (ql *QuickList) Range(start, stop int) []interface{} {
	if start < 0 {
		start = ql.Length + start
	}
	if stop < 0 {
		stop = ql.Length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= ql.Length {
		stop = ql.Length - 1
	}
	if start > stop || start >= ql.Length {
		return nil
	}
	result := make([]interface{}, stop-start+1)
	for i := start; i <= stop; i++ {
		result[i-start] = ql.at(i)
	}
	return result
}

func (ql *QuickList) Trim(start, keep int) {
	if start < 0 {
		start = ql.Length + start
	}
	if start < 0 {
		start = 0
	}
	end := start + keep
	if end > ql.Length {
		end = ql.Length
	}
	flat := make([]interface{}, 0, end-start)
	for i := start; i < end; i++ {
		flat = append(flat, ql.at(i))
	}
	ql.left = ql.left[:0]
	ql.right = flat
	ql.Length = len(flat)
}

// RemoveValue removes occurrences of a value from the quicklist.
// count > 0: remove first count occurrences
// count < 0: remove last |count| occurrences
// count == 0: remove all occurrences
func (ql *QuickList) RemoveValue(v interface{}, count int) int {
	removed := 0
	if count == 0 {
		// Remove all
		newRight := make([]interface{}, 0, len(ql.right))
		for _, item := range ql.right {
			if item != v {
				newRight = append(newRight, item)
			} else {
				removed++
			}
		}
		newLeft := make([]interface{}, 0, len(ql.left))
		for i := len(ql.left) - 1; i >= 0; i-- {
			if ql.left[i] != v {
				newLeft = append(newLeft, ql.left[i])
			} else {
				removed++
			}
		}
		// Re-reverse left
		for i, j := 0, len(newLeft)-1; i < j; i, j = i+1, j-1 {
			newLeft[i], newLeft[j] = newLeft[j], newLeft[i]
		}
		ql.left = newLeft
		ql.right = newRight
		ql.Length -= removed
		return removed
	}
	// Convert to flat, remove, re-split
	flat := make([]interface{}, 0, ql.Length)
	if count > 0 {
		for j := 0; j < ql.Length; j++ {
			item := ql.at(j)
			if item == v && removed < count {
				removed++
			} else {
				flat = append(flat, item)
			}
		}
	} else {
		// count < 0: remove from end
		skipped := 0
		for j := ql.Length - 1; j >= 0; j-- {
			item := ql.at(j)
			if item == v && skipped < -count {
				skipped++
			} else {
				flat = append([]interface{}{item}, flat...)
			}
		}
		removed = skipped
	}
	ql.left = ql.left[:0]
	ql.right = flat
	ql.Length = len(flat)
	return removed
}

// Clear removes all elements from the quicklist.
func (ql *QuickList) Clear() {
	ql.left = ql.left[:0]
	ql.right = ql.right[:0]
	ql.Length = 0
}

// --- Set (hash table based) ---

// Set implements a hash set, matching Redis's set encoding.
type Set struct {
	dict map[string]struct{}
}

func NewSet() *Set {
	return &Set{dict: make(map[string]struct{})}
}

func (s *Set) Add(v string) bool {
	_, exists := s.dict[v]
	s.dict[v] = struct{}{}
	return !exists
}

func (s *Set) Remove(v string) bool {
	_, exists := s.dict[v]
	delete(s.dict, v)
	return exists
}

func (s *Set) Contains(v string) bool {
	_, exists := s.dict[v]
	return exists
}

func (s *Set) Len() int { return len(s.dict) }

func (s *Set) Members() []string {
	members := make([]string, 0, len(s.dict))
	for k := range s.dict {
		members = append(members, k)
	}
	return members
}

func (s *Set) Pop(count int) []string {
	if count <= 0 || len(s.dict) == 0 {
		return nil
	}
	if count > len(s.dict) {
		count = len(s.dict)
	}
	result := make([]string, 0, count)
	for k := range s.dict {
		delete(s.dict, k)
		result = append(result, k)
		if len(result) >= count {
			break
		}
	}
	return result
}

func (s *Set) RandomMembers(count int) []string {
	if count <= 0 || len(s.dict) == 0 {
		return nil
	}
	withReplacement := count > len(s.dict) || count < 0
	if count < 0 {
		count = -count
	}
	result := make([]string, 0, count)
	if withReplacement {
		for i := 0; i < count; i++ {
			for k := range s.dict {
				result = append(result, k)
				break
			}
		}
	} else {
		if count > len(s.dict) {
			count = len(s.dict)
		}
		for k := range s.dict {
			result = append(result, k)
			if len(result) >= count {
				break
			}
		}
	}
	return result
}

// --- Hash ---

// Hash implements a string->string hash map, matching Redis's hash encoding.
type Hash struct {
	dict map[string]string
}

func NewHash() *Hash {
	return &Hash{dict: make(map[string]string)}
}

func (h *Hash) Set(field, value string) bool {
	_, exists := h.dict[field]
	h.dict[field] = value
	return !exists
}

func (h *Hash) Get(field string) (string, bool) {
	v, ok := h.dict[field]
	return v, ok
}

func (h *Hash) Delete(field string) bool {
	_, exists := h.dict[field]
	delete(h.dict, field)
	return exists
}

func (h *Hash) Len() int { return len(h.dict) }

func (h *Hash) Fields() []string {
	fields := make([]string, 0, len(h.dict))
	for k := range h.dict {
		fields = append(fields, k)
	}
	return fields
}

func (h *Hash) Values() []string {
	vals := make([]string, 0, len(h.dict))
	for _, v := range h.dict {
		vals = append(vals, v)
	}
	return vals
}

func (h *Hash) All() map[string]string {
	result := make(map[string]string, len(h.dict))
	for k, v := range h.dict {
		result[k] = v
	}
	return result
}

func (h *Hash) IncrementBy(field string, delta int64) (int64, error) {
	s, _ := h.dict[field]
	var n int64
	if s != "" {
		_, err := fmt.Sscanf(s, "%d", &n)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}
	}
	n += delta
	h.dict[field] = fmt.Sprintf("%d", n)
	return n, nil
}

func (h *Hash) IncrementByFloat(field string, delta float64) (float64, error) {
	s, _ := h.dict[field]
	var f float64
	if s != "" {
		_, err := fmt.Sscanf(s, "%f", &f)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not a valid float")
		}
	}
	f += delta
	h.dict[field] = fmt.Sprintf("%g", f)
	return f, nil
}

// --- Sorted Set ---

// ZSetEntry is a member-score pair in a sorted set.
type ZSetEntry struct {
	Member string
	Score  float64
}

// ZSet implements a sorted set, matching Redis's zset encoding.
type ZSet struct {
	dict   map[string]float64 // member -> score
	sorted []ZSetEntry        // sorted by score (simplified; production would use skip list)
	dirty  bool               // needs re-sort
}

func NewZSet() *ZSet {
	return &ZSet{
		dict: make(map[string]float64),
	}
}

func (z *ZSet) rebuildSorted() {
	if z.dirty || len(z.sorted) != len(z.dict) {
		z.sorted = make([]ZSetEntry, 0, len(z.dict))
		for m, s := range z.dict {
			z.sorted = append(z.sorted, ZSetEntry{Member: m, Score: s})
		}
		// Sort by score, then by member lex
		sortZSetEntries(z.sorted)
		z.dirty = false
	}
}

func sortZSetEntries(entries []ZSetEntry) {
	// Insertion sort for small sets, quicksort for large
	n := len(entries)
	if n <= 1 {
		return
	}
	for i := 1; i < n; i++ {
		key := entries[i]
		j := i - 1
		for j >= 0 && (entries[j].Score > key.Score ||
			(entries[j].Score == key.Score && entries[j].Member > key.Member)) {
			entries[j+1] = entries[j]
			j--
		}
		entries[j+1] = key
	}
}

func (z *ZSet) Add(member string, score float64) bool {
	_, exists := z.dict[member]
	z.dict[member] = score
	z.dirty = true
	return !exists
}

func (z *ZSet) Remove(member string) bool {
	_, exists := z.dict[member]
	delete(z.dict, member)
	if exists {
		z.dirty = true
	}
	return exists
}

func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.dict[member]
	return s, ok
}

func (z *ZSet) Len() int { return len(z.dict) }

func (z *ZSet) IncrementBy(member string, delta float64) float64 {
	s := z.dict[member]
	s += delta
	z.dict[member] = s
	z.dirty = true
	return s
}

func (z *ZSet) Rank(member string) (int, bool) {
	z.rebuildSorted()
	for i, e := range z.sorted {
		if e.Member == member {
			return i, true
		}
	}
	return -1, false
}

func (z *ZSet) RevRank(member string) (int, bool) {
	z.rebuildSorted()
	rank, ok := z.Rank(member)
	if !ok {
		return -1, false
	}
	return len(z.sorted) - 1 - rank, true
}

func (z *ZSet) Range(start, stop int, withScores bool) []ZSetEntry {
	z.rebuildSorted()
	if start < 0 {
		start = len(z.sorted) + start
	}
	if stop < 0 {
		stop = len(z.sorted) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(z.sorted) {
		stop = len(z.sorted) - 1
	}
	if start > stop || start >= len(z.sorted) {
		return nil
	}
	result := make([]ZSetEntry, stop-start+1)
	copy(result, z.sorted[start:stop+1])
	return result
}

func (z *ZSet) RangeByScore(min, max float64, withScores bool, offset, count int) []ZSetEntry {
	z.rebuildSorted()
	result := make([]ZSetEntry, 0)
	skipped := 0
	for _, e := range z.sorted {
		if e.Score < min {
			continue
		}
		if e.Score > max {
			break
		}
		if skipped < offset {
			skipped++
			continue
		}
		result = append(result, e)
		if count >= 0 && len(result) >= count {
			break
		}
	}
	return result
}

func (z *ZSet) RemoveRangeByRank(start, stop int) int {
	z.rebuildSorted()
	if start < 0 {
		start = len(z.sorted) + start
	}
	if stop < 0 {
		stop = len(z.sorted) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(z.sorted) {
		stop = len(z.sorted) - 1
	}
	if start > stop {
		return 0
	}
	removed := z.sorted[start : stop+1]
	for _, e := range removed {
		delete(z.dict, e.Member)
	}
	z.dirty = true
	return len(removed)
}

func (z *ZSet) RemoveRangeByScore(min, max float64) int {
	count := 0
	for m, s := range z.dict {
		if s >= min && s <= max {
			delete(z.dict, m)
			count++
		}
	}
	if count > 0 {
		z.dirty = true
	}
	return count
}

// --- Stream ---

// 流Entry represents a single entry in a stream.
// StreamEntry 表示流中的单条消息。
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// StreamConsumer 表示消费者组中的单个消费者。
// 对应 Redis 的 streamConsumer 结构。
type StreamConsumer struct {
	Name      string                          // 消费者名称
	SeenTime  time.Time                       // 最后活跃时间
	Pending   map[string]time.Time            // entryID -> delivery time（PEL）
}

// StreamConsumerGroup 表示消费者组。
// 对应 Redis 的 streamCG 结构。
type StreamConsumerGroup struct {
	Name      string                          // 消费者组名称
	LastID    string                          // 最后投递的 ID
	Consumers map[string]*StreamConsumer      // consumer name -> consumer
}

// Stream 实现 Redis 流。
// 对应 Redis 的 stream 结构。
type Stream struct {
	Entries []StreamEntry
	Length  int64
	LastID  string
	Groups  map[string]*StreamConsumerGroup  // group name -> group
}

// NewStream 创建新的流。
func NewStream() *Stream {
	return &Stream{
		Groups: make(map[string]*StreamConsumerGroup),
	}
}

func (s *Stream) Add(fields map[string]string, id string) (string, error) {
	if id == "" || id == "*" {
		// Auto-generate ID: <ms>-<seq>
		ms := time.Now().UnixMilli()
		seq := int64(0)
		if s.LastID != "" {
			var lastMs, lastSeq int64
			fmt.Sscanf(s.LastID, "%d-%d", &lastMs, &lastSeq)
			if ms <= lastMs {
				ms = lastMs
				seq = lastSeq + 1
			}
		}
		id = fmt.Sprintf("%d-%d", ms, seq)
	}
	entry := StreamEntry{ID: id, Fields: fields}
	s.Entries = append(s.Entries, entry)
	s.Length++
	s.LastID = id
	return id, nil
}
