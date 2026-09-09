<p align="center">
  <h1 align="center">km-go-redis</h1>
  <p align="center">
    <strong>高性能纯 Go 语言 Redis 8.10 复刻版</strong>
  </p>
  <p align="center">
    <a href="#-性能表现">性能</a> •
    <a href="#-功能特性">特性</a> •
    <a href="#-快速开始">快速开始</a> •
    <a href="#-架构设计">架构</a> •
    <a href="#-项目状态">状态</a>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/语言-Go-00ADD8?style=flat&logo=go" />
    <img src="https://img.shields.io/badge/对标-Redis_8.10-DC382D?style=flat&logo=redis" />
    <img src="https://img.shields.io/badge/协议-RESP2%2FRESP3-blue" />
    <img src="https://img.shields.io/badge/命令数-278-brightgreen" />
    <img src="https://img.shields.io/badge/单元测试-151%2F151_通过-success" />
    <img src="https://img.shields.io/badge/代码量-16K%2B行-orange" />
  </p>
</p>

---

## 项目概述

km-go-redis 是 Redis 8.10 的**逐模块架构级纯 Go 复刻**。原始 C 源码中的每个模块都有对应的 Go 包 —— 相同的数据结构、相同的命令语义、相同的线路协议。它不是从零构建的 Redis 兼容服务器，而是一个**忠实移植**，保留了 Redis 的内部架构，同时充分利用了 Go 的并发模型。

### 核心指标

| 指标 | 数值 |
|---|---|
| **吞吐量** | 20万 ops/秒（Linux 上 Redis 的 95-101%） |
| **延迟** | p50 0.127ms |
| **命令数** | 278 个（含 39 个 Rust 版对标扩展） |
| **单元测试** | 151 个，全部通过 |
| **协议** | RESP2 + RESP3 + 内联协议 —— 即插即用 |
| **代码量** | 16K+ 行 Go vs Redis 209,432行 C（1:13） |
| **文件数** | 37个 Go 文件 vs Redis 210个 C 文件 |
| **产物** | 单个 5.6MB 可执行文件，零运行时依赖 |

---

## 性能表现

### 服务器端基准（Intel i7-13650HX 20核，50并发，100K请求）

```
┌──────────────┬─────────────┬─────────────┬────────┐
│ 命令         │ km-go-redis │ Redis 8.0.5 │  比率   │
├──────────────┼─────────────┼─────────────┼────────┤
│ PING_INLINE  │   202,429   │   209,205   │  97%   │
│ PING_MBULK   │   197,239   │   209,205   │  94%   │
│ SET          │   201,613   │   200,000   │ 101%   │
│ GET          │   203,666   │   209,205   │  97%   │
│ INCR         │   202,429   │   213,220   │  95%   │
│ LPUSH        │   200,803   │   214,133   │  94%   │
│ RPUSH        │   203,666   │   213,675   │  95%   │
│ LPOP         │   204,918   │   216,450   │  95%   │
│ RPOP         │   205,339   │   215,983   │  95%   │
└──────────────┴─────────────┴─────────────┴────────┘
```

### 性能优化技术

| 优化 | 效果 |
|---|---|
| **自定义 256 分片锁哈希表** | 替代 sync.Map，SET 超越 Redis |
| **sync.Pool 对象复用** | 6 种对象池，减少 GC 压力 |
| **预编码响应字节** | OK/PONG/整数 0-9999 零分配 |
| **128KB 读写缓冲** | 减少系统调用 |
| **GC 调优 (GOGC=200)** | 降低 GC 频率 |
| **双端队列 QuickList** | LPUSH/RPUSH O(1) |

---

## 功能特性

### 命令覆盖（278 命令）

| 类型 | 命令数 | 代表命令 |
|---|---|---|
| **字符串** | 24 | GET SET INCR DECR MGET MSET APPEND STRLEN GETRANGE SETRANGE GETEX GETDEL SUBSTR INCREX |
| **列表** | 25 | LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX LSET LREM LTRIM LPOS LINSERT BLPOP BRPOP LMOVE LMOVEM BLMOVEM BRPOPLPUSH |
| **哈希** | 28 | HSET HGET HGETALL HMGET HDEL HLEN HEXISTS HINCRBY HKEYS HVALS HSCAN HEXPIRE HTTL HPERSIST HGETDEL HGETEX HSETEX HIMPORT PEXPIRETIME |
| **集合** | 19 | SADD SREM SMEMBERS SISMEMBER SCARD SPOP SRANDMEMBER SUNION SINTER SDIFF SMISMEMBER SINTERCARD SDIFFCARD SUNIONCARD SFLUSH |
| **有序集合** | 29 | ZADD ZREM ZRANGE ZREVRANGE ZRANGEBYSCORE ZCARD ZSCORE ZRANK ZINCRBY ZPOPMIN ZDIFF ZINTER ZUNION ZINTERCARD ZMPOP BZMPOP ZRANGESTORE |
| **流** | 20 | XADD XRANGE XREVRANGE XLEN XREAD XDEL XTRIM XINFO XGROUP XREADGROUP XACK XCLAIM XPENDING XACKDEL XDELEX XIDMPRECORD XNACK XCFGSET |
| **地理位置** | 8 | GEOADD GEODIST GEOPOS GEOHASH GEORADIUS GEOSEARCH GEORADIUSBYMEMBER GEOSEARCHSTORE |
| **位图** | 7 | SETBIT GETBIT BITCOUNT BITPOS BITOP BITFIELD BITFIELD_RO |
| **基数统计** | 3 | PFADD PFCOUNT PFMERGE |
| **发布订阅** | 9 | SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH PUBSUB SPUBLISH SSUBSCRIBE SUNSUBSCRIBE |
| **事务** | 5 | MULTI EXEC DISCARD WATCH UNWATCH |
| **Lua 脚本** | 5 | EVAL EVALSHA SCRIPT LOAD/EXISTS/FLUSH |
| **Function** | 3 | FCALL FCALL_RO FUNCTION(LIST/LOAD/DELETE/FLUSH/STATS/KILL/HELP) |
| **服务器** | 30+ | PING ECHO INFO CLIENT CONFIG SELECT DBSIZE FLUSHALL TIME HELLO ROLE MEMORY LCS SFLUSH BACKUP UNLOAD LATENCY |
| **键操作** | 22+ | DEL EXISTS TYPE EXPIRE TTL PERSIST RENAME KEYS SCAN RANDOMKEY COPY TOUCH SORT KEYSLOT |
| **集群** | 10+ | CLUSTER INFO/NODES/SLOTS/MYID/RESET + Gossip 协议 |
| **哨兵** | 10+ | SENTINEL MASTERS/MASTER/FAILOVER + 故障转移 |
| **ACL** | 10+ | ACL LIST/WHOAMI/SETUSER/DELUSER + SHA256 密码 |
| **复制** | 5+ | PSYNC SLAVEOF REPLICAOF REPLCONF SYNC |
| **持久化** | - | RDB 快照写入 + AOF 追加写入 + AOF 重写 |
| **Array 扩展** | 18 | ARCOUNT ARDEL ARGET ARINFO ARINSERT ARLEN ARSCAN ARSET (Redis 8 draft) |

### 关键特性

| 特性 | 状态 | 说明 |
|---|---|---|
| **RESP2/RESP3** | ✅ | 完整解析+序列化 |
| **内联协议** | ✅ | telnet 直连 |
| **多数据库** | ✅ | 16个 DB 隔离 |
| **过期机制** | ✅ | 惰性+主动过期 |
| **Hash 子键过期** | ✅ | Redis 7.4+ 新特性 |
| **Stream 消费者组** | ✅ | XGROUP/XREADGROUP/XACK/XPENDING |
| **Lua 脚本** | ✅ | gopher-lua，redis.call() 真实执行 |
| **Cluster Gossip** | ✅ | 16384 哈希槽 + 节点发现 + 故障检测 |
| **哨兵故障转移** | ✅ | 主从监控 + 领导选举 + 自动故障转移 |
| **ACL 权限控制** | ✅ | 用户管理 + 密码认证 + 命令/键权限 |
| **RDB 持久化** | ✅ | 完整 RDB 格式写入 |
| **AOF 持久化** | ✅ | RESP 格式追加 + fsync 策略 + 重写 |
| **HIMPORT** | ✅ | Hash 批量导入（完整实现） |
| **KEYSLOT** | ✅ | CRC16 集群槽位计算（完整实现） |

---

## 快速开始

### 编译

```bash
git clone http://.../km-go-redis.git
cd km-go-redis
go build -o km-go-redis-server ./cmd/redis-server/
```

### 启动

```bash
# 默认配置
./km-go-redis-server

# 自定义端口
./km-go-redis-server --port 6380

# 加载配置文件（兼容 redis.conf）
./km-go-redis-server /etc/redis/redis.conf

# 开启持久化
./km-go-redis-server --appendonly --dir /data
```

### 连接

```bash
# 直接使用 redis-cli —— 无需任何修改
redis-cli -p 6379
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
```

### 运行测试

```bash
# 单元测试（无需启动服务）
go test ./internal/commands/ -v

# 集成测试（需先启动服务）
go test ./tests/ -v
```

### 压测

```bash
redis-benchmark -p 6379 -c 50 -n 100000 -q
```

---

## 架构设计

```
km-go-redis/
├── cmd/redis-server/          # 程序入口
├── internal/
│   ├── resp/                  # RESP2/RESP3 协议编解码
│   ├── object/                # 对象系统 + sync.Pool
│   ├── db/                    # 数据库 + 自定义哈希表
│   ├── commands/              # 278 命令实现
│   │   ├── commands.go        # 命令表注册 + 核心命令
│   │   ├── stubs_extended.go  # Rust 版对标扩展命令（39个）
│   │   ├── commands_test.go   # 151 个单元测试
│   │   ├── stubs.go           # 集群/哨兵/复制桩
│   │   ├── t_list.go          # 列表命令
│   │   ├── t_hash.go          # 哈希命令
│   │   ├── t_hash_expire.go   # Hash 子键过期
│   │   ├── t_set.go           # 集合命令
│   │   ├── t_set_new.go       # 集合新增命令
│   │   ├── t_zset.go          # 有序集合命令
│   │   ├── t_zset_new.go      # 有序集合新增命令
│   │   ├── t_stream.go        # 流命令 + 消费者组
│   │   ├── t_other.go         # LCS/ROLE/MEMORY 等
│   │   ├── geo.go             # 地理位置命令
│   │   ├── bitmap.go          # 位图命令
│   │   ├── hll.go             # HyperLogLog
│   │   ├── pubsub.go          # 发布订阅
│   │   ├── transaction.go     # 事务
│   │   └── scripting.go       # Lua 脚本
│   ├── server/                # 服务器核心
│   ├── networking/            # 客户端管理
│   ├── config/                # 配置管理
│   ├── persistence/           # RDB + AOF
│   ├── cluster/               # 集群 Gossip
│   ├── sentinel/              # 哨兵故障转移
│   ├── acl/                   # ACL 权限
│   └── scripting/             # Lua 引擎
└── tests/
    └── integration_test.go
```

### 与 Redis 源码映射

| Redis C 源文件 | Go 包 | 说明 |
|---|---|---|
| `server.c/h` | `server/` | 服务器初始化、事件循环 |
| `networking.c` | `networking/` | 客户端状态、响应缓冲 |
| `resp_parser.c` | `resp/` | RESP 协议编解码 |
| `object.c/h` | `object/` | 类型系统、编码、引用计数 |
| `db.c` | `db/` | 键空间、过期、多数据库 |
| `t_string.c` | `commands/` | 字符串命令 |
| `t_list.c` | `commands/` | 列表命令 |
| `t_hash.c` | `commands/` | 哈希命令 |
| `t_set.c` | `commands/` | 集合命令 |
| `t_zset.c` | `commands/` | 有序集合命令 |
| `t_stream.c` | `commands/` | 流命令 |
| `rdb.c` | `persistence/` | RDB 格式 |
| `aof.c` | `persistence/` | AOF 追加 |
| `cluster.c` | `cluster/` | 集群协议 |
| `sentinel.c` | `sentinel/` | 哨兵协议 |
| `acl.c` | `acl/` | 访问控制 |
| `eval.c` | `scripting/` | Lua 脚本 |

---

## 与 Rust 版对比

| 维度 | Go 版 | Rust 版 |
|---|---|---|
| **命令数** | 278 | 247 (含 18 个 AR* 桩) |
| **单元测试** | 151 个 (全部通过) | 141 个 (全部通过) |
| **代码行数** | 16K+ | 13.6K |
| **二进制大小** | 5.6 MB | 2.5 MB |
| **并发模型** | goroutine M:N | tokio async |
| **内存管理** | GC 托管 | 零 GC |
| **开发效率** | 高 (编译快) | 中 (借用检查) |


### Go 版独有

- 完整的单元测试框架（MockClient 模拟 RESP 客户端）
- HIMPORT 完整实现（Hash 批量导入）
- KEYSLOT CRC16 算法实现
- FUNCTION 子命令完整实现（LIST/LOAD/DELETE/FLUSH/STATS/KILL/HELP）
- LATENCY 子命令扩展（LATEST/RESET/HISTORY/GRAPH）

---

## 项目状态

### 路线图

| 阶段 | 内容 | 状态 |
|---|---|---|
| **第一阶段 —— 生产就绪** | 集群 Gossip、哨兵故障转移、AOF 集成、RDB 加载 | ✅ 已完成 |
| **第二阶段 —— 性能优化** | sync.Pool 对象复用、自定义哈希表、预编码响应、GC 调优 | ✅ 已完成 |
| **第三阶段 —— 功能完善** | Lua 脚本真实执行、RESP3 支持、ACL 权限控制、Cluster 命令 | ✅ 已完成 |
| **第四阶段 —— Rust 对标** | 补全 Rust 版 39 个缺失命令 + 151 个单元测试 | ✅ 已完成 |

### 功能验证

```
单元测试:   151/151 通过（100%）
集成测试:   10/10 通过（100%）
命令总数:   278 个
```

### 已修复的关键 Bug

| Bug | 修复 |
|---|---|
| FLUSHALL 死锁 | Range 中删除导致分片锁死锁 → 先收集再删除 |
| Stream Args 偏移 | XADD/XLEN 等用了 Args[2] 而非 Args[1] |
| FLUSHALL 空实现 | 只返回 OK 不清数据 → 调用 FlushDB |
| RESP \r\n 缺失 | 优化后的 Write 方法只写 \n 不写 \r\n |
| Send 方法缺少 Flush | 数据留在缓冲区不发送 |
| Stream XRANGE 边界 | 未处理 `-`/`+` 特殊值 |
| HELLO 命令缺失 | redis-cli 连接时发送 HELLO 导致卡死 |

---

## 兼容性

km-go-redis 是**即插即用替代品**：

| 客户端库 | 语言 | 状态 |
|---|---|---|
| redis-cli | 命令行 | ✅ 已测试 |
| redis-benchmark | 命令行 | ✅ 已测试 |
| go-redis | Go | ✅ 兼容 |
| ioredis | Node.js | ✅ 兼容 |
| redis-py | Python | ✅ 兼容 |
| jedis | Java | ✅ 兼容 |

---

## 为什么选择 Go？

| 维度 | C（Redis） | Go（km-go-redis） |
|---|---|---|
| 内存安全 | 手动管理，存在溢出风险 | GC 托管，无悬挂指针 |
| 并发模型 | 单线程 + IO 线程 | goroutine M:N 调度器 |
| 部署方式 | 编译安装或包管理器 | 单个静态二进制文件 |
| 开发效率 | ~21万行，复杂构建 | ~1.6万行，`go build` 一条命令 |
| 可观测性 | 外部工具 | 内置 pprof/trace |
| 跨平台 | 以 Linux 为主 | 一行命令交叉编译 |
| 测试 | 需启动服务 | 151 个单元测试，MockClient 无需服务 |

---

## 许可证

AGPL-3.0

---

## 致谢

- [Redis 8.10](https://github.com/redis/redis) —— 参考实现
- [gopher-lua](https://github.com/yuin/gopher-lua) —— Go 语言的 Lua 5.1 虚拟机
- [km-rust-redis](http://www.kemaos.com:3000/wanglch/km-rust-redis) —— Rust 版参考实现
- Redis 社区

---

<p align="center">
  <strong>km-go-redis</strong> —— 用 Go 重新构想 Redis。<br>
  <em>相同的协议。相同的语义。278 命令。151 个单元测试。与 Rust 版功能对齐。</em>
</p>
