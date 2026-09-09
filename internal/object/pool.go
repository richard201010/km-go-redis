// 对象池管理，减少 GC 压力 with sync.Pool.
// This file implements object pooling to reduce GC pressure.
package object

import (
	"sync"
)

// 常用类型的对象池
var (
	stringObjPool = sync.Pool{
		New: func() interface{} { return &Object{} },
	}
	listObjPool = sync.Pool{
		New: func() interface{} { return &Object{Type: TypeList, Encoding: EncQuickList} },
	}
	hashObjPool = sync.Pool{
		New: func() interface{} { return &Object{Type: TypeHash, Encoding: EncHT} },
	}
	setObjPool = sync.Pool{
		New: func() interface{} { return &Object{Type: TypeSet, Encoding: EncHT} },
	}
	zsetObjPool = sync.Pool{
		New: func() interface{} { return &Object{Type: TypeZSet, Encoding: EncSkipList} },
	}
	streamObjPool = sync.Pool{
		New: func() interface{} { return &Object{Type: TypeStream, Encoding: EncStream} },
	}
)

// PooledStringObject 从池中创建字符串对象.
func PooledStringObject(s string) *Object {
	// Try integer encoding first
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
			return PooledIntObject(n)
		}
	}
	o := stringObjPool.Get().(*Object)
	o.Type = TypeString
	if len(s) <= 44 {
		o.Encoding = EncEmbStr
	} else {
		o.Encoding = EncRaw
	}
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = s
	return o
}

// PooledIntObject 从池中创建整数对象.
func PooledIntObject(n int64) *Object {
	o := stringObjPool.Get().(*Object)
	o.Type = TypeString
	o.Encoding = EncInt
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = n
	return o
}

// PooledListObject 从池中创建列表对象.
func PooledListObject() *Object {
	o := listObjPool.Get().(*Object)
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = NewQuickList()
	return o
}

// PooledHashObject 从池中创建哈希对象.
func PooledHashObject() *Object {
	o := hashObjPool.Get().(*Object)
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = NewHash()
	return o
}

// PooledSetObject 从池中创建集合对象.
func PooledSetObject() *Object {
	o := setObjPool.Get().(*Object)
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = NewSet()
	return o
}

// PooledZSetObject 从池中创建有序集合对象.
func PooledZSetObject() *Object {
	o := zsetObjPool.Get().(*Object)
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = NewZSet()
	return o
}

// PooledStreamObject 从池中创建流对象.
func PooledStreamObject() *Object {
	o := streamObjPool.Get().(*Object)
	o.RefCount = 1
	o.LRU = 0
	o.Ptr = NewStream()
	return o
}

// ReleaseObject 将对象返回到池中.
func ReleaseObject(o *Object) {
	if o == nil || o.RefCount < 0 {
		return
	}
	// Clear the object
	o.Ptr = nil
	o.LRU = 0
	switch o.Type {
	case TypeString:
		stringObjPool.Put(o)
	case TypeList:
		o.Encoding = EncQuickList
		listObjPool.Put(o)
	case TypeHash:
		o.Encoding = EncHT
		hashObjPool.Put(o)
	case TypeSet:
		o.Encoding = EncHT
		setObjPool.Put(o)
	case TypeZSet:
		o.Encoding = EncSkipList
		zsetObjPool.Put(o)
	case TypeStream:
		o.Encoding = EncStream
		streamObjPool.Put(o)
	}
}

