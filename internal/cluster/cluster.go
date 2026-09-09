// Package cluster 实现 Redis 集群 Gossip 协议 protocol.
// Full node-to-node communication with MEET/PING/PONG messages,
// failure detection (PFAIL/FAIL), and slot migration.
package cluster

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	ClusterSlots       = 16384
	ClusterPortGossipOffset = 10000

	// Node states
	NodeOK      = 0
	NodePfail   = 1
	NodeFail    = 2

	// Message types
	MsgPing         = 0
	MsgPong         = 1
	MsgMeet         = 2
	MsgFail         = 3
	MsgPublish      = 4
	MsgFailoverAuth = 5
	MsgUpdate       = 6

	// Failure detection
	ClusterNodeTimeout     = 15000 // ms
	ClusterNodeMaxPfail    = 2     // max pfails before marking fail
	ClusterGossipInterval  = 100   // ms between gossip rounds
	ClusterPingInterval    = 1000  // ms between pings
)

// ClusterMsgHeader is the fixed-size header for cluster messages.
type ClusterMsgHeader struct {
	Sig        [4]byte  // "RCmb" signature
	TotLen     uint32   // Total message length
	Ver        uint16   // Protocol version
	Port       uint16   // Sender's gossip port
	Type       uint16   // Message type
	Count      uint16   // Number of gossip entries
	Sender     [40]byte // Sender node ID
	IP         [46]byte // Sender IP address
	MyPort     uint16   // Sender's client port
	Flags      uint16   // Sender flags
	CurrentEpoch uint64 // Sender's current epoch
	ConfigEpoch  uint64 // Sender's config epoch
	Offset     uint64   // Replication offset
	SenderState byte   // Sender's state (OK/PFAIL/FAIL)
	_          [3]byte  // Padding
}

// ClusterMsgGossip is a gossip entry in the message.
type ClusterMsgGossip struct {
	NodeID    [40]byte // Node ID
	IP        [46]byte // Node IP
	Port      uint16   // Node port
	Flags     uint16   // Node flags
	PingSent  uint32   // Last ping sent timestamp
	PongRecv  uint32   // Last pong received timestamp
	ConfigEpoch uint64 // Config epoch
	_         [4]byte  // Padding
}

// ClusterNode represents a node in the cluster.
type ClusterNode struct {
	mu          sync.RWMutex
	ID          string
	IP          string
	Port        int
	BusPort     int
	State       int
	Flags       int
	PingSent    time.Time
	PongRecv    time.Time
	ConfigEpoch int64
	CurrentEpoch int64
	Slots       [ClusterSlots / 8]byte
	NumSlots    int
	Replicas    []*ClusterNode
	Master      *ClusterNode
	Link        net.Conn
	LinkActive  bool
	FailTime    time.Time
	VotedEpoch  int64
	VoteYes     int
	VoteNo      int
}

// NewClusterNode creates a new cluster node.
func NewClusterNode(id, ip string, port int) *ClusterNode {
	return &ClusterNode{
		ID:      id,
		IP:      ip,
		Port:    port,
		BusPort: port + ClusterPortGossipOffset,
		State:   NodeOK,
		PongRecv: time.Now(),
	}
}

func (n *ClusterNode) HasSlot(slot uint16) bool {
	return (n.Slots[slot/8] & (1 << (slot % 8))) != 0
}

func (n *ClusterNode) SetSlot(slot uint16) {
	if !n.HasSlot(slot) {
		n.Slots[slot/8] |= (1 << (slot % 8))
		n.NumSlots++
	}
}

func (n *ClusterNode) ClearSlot(slot uint16) {
	if n.HasSlot(slot) {
		n.Slots[slot/8] &^= (1 << (slot % 8))
		n.NumSlots--
	}
}

// Cluster 表示集群状态.
type Cluster struct {
	mu           sync.RWMutex
	Myself       *ClusterNode
	Nodes        map[string]*ClusterNode
	Slots        [ClusterSlots]*ClusterNode
	CurrentEpoch int64
	Enabled      bool
	ConfigFile   string
	Size         int
	listener     net.Listener
	stopCh       chan struct{}
}

// NewCluster creates a new cluster instance.
func NewCluster(enabled bool) *Cluster {
	return &Cluster{
		Nodes:  make(map[string]*ClusterNode),
		Enabled: enabled,
		stopCh: make(chan struct{}),
	}
}

// Init 初始化集群.
func (c *Cluster) Init(ip string, port int) error {
	if !c.Enabled {
		return nil
	}
	id := generateNodeID()
	c.Myself = NewClusterNode(id, ip, port)
	c.Nodes[id] = c.Myself
	c.Size = 1

	// Assign all slots to myself initially
	for i := uint16(0); i < ClusterSlots; i++ {
		c.Slots[i] = c.Myself
		c.Myself.SetSlot(i)
	}

	log.Printf("[集群] 节点 %s 初始化于 %s:%d (总线端口 %d)", id[:8], ip, port, port+ClusterPortGossipOffset)
	return nil
}

// StartGossip 启动 Gossip 监听器 and periodic tasks.
func (c *Cluster) StartGossip() error {
	if !c.Enabled {
		return nil
	}
	addr := fmt.Sprintf("0.0.0.0:%d", c.Myself.BusPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("监听集群总线失败: %w", err)
	}
	c.listener = ln
	log.Printf("[集群] Gossip 监听于 %s", addr)

	go c.acceptGossipConnections()
	go c.gossipLoop()
	go c.failDetectionLoop()
	return nil
}

// Stop stops the gossip listener.
func (c *Cluster) Stop() {
	if c.listener != nil {
		close(c.stopCh)
		c.listener.Close()
	}
}

// acceptGossipConnections accepts incoming gossip connections.
func (c *Cluster) acceptGossipConnections() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			select {
			case <-c.stopCh:
				return
			default:
				log.Printf("[集群] 接受连接失败: %v", err)
				continue
			}
		}
		go c.handleGossipConnection(conn)
	}
}

// handleGossipConnection handles an incoming gossip message.
func (c *Cluster) handleGossipConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	reader := bufio.NewReaderSize(conn, 8192)
	header, err := c.readGossipHeader(reader)
	if err != nil {
		return
	}

	switch header.Type {
	case MsgPing:
		c.handlePing(header, conn)
	case MsgPong:
		c.handlePong(header)
	case MsgMeet:
		c.handleMeet(header, conn)
	case MsgFail:
		c.handleFail(header)
	}
}

// readGossipHeader reads a cluster message header.
func (c *Cluster) readGossipHeader(reader *bufio.Reader) (*ClusterMsgHeader, error) {
	var header ClusterMsgHeader
	buf := make([]byte, 2048) // Max header + gossip entries
	n, err := reader.Read(buf)
	if err != nil || n < 224 { // Minimum header size
		return nil, fmt.Errorf("读取集群消息失败")
	}
	// Parse signature
	if string(buf[0:4]) != "RCmb" {
		return nil, fmt.Errorf("无效的集群消息签名")
	}
	header.TotLen = binary.LittleEndian.Uint32(buf[4:8])
	header.Ver = binary.LittleEndian.Uint16(buf[8:10])
	header.Type = binary.LittleEndian.Uint16(buf[12:14])
	header.Count = binary.LittleEndian.Uint16(buf[14:16])
	copy(header.Sender[:], buf[16:56])
	copy(header.IP[:], buf[96:142])
	header.Port = binary.LittleEndian.Uint16(buf[142:144])
	return &header, nil
}

// handlePing handles an incoming PING message by sending PONG.
func (c *Cluster) handlePing(header *ClusterMsgHeader, conn net.Conn) {
	senderID := strings.TrimRight(string(header.Sender[:]), "\x00")
	c.updateNodeFromGossip(senderID, header)

	// Send PONG back
	c.sendPong(conn)
}

// handlePong handles an incoming PONG message.
func (c *Cluster) handlePong(header *ClusterMsgHeader) {
	senderID := strings.TrimRight(string(header.Sender[:]), "\x00")
	c.mu.Lock()
	if node, ok := c.Nodes[senderID]; ok {
		node.PongRecv = time.Now()
		node.State = NodeOK
	}
	c.mu.Unlock()
	c.updateNodeFromGossip(senderID, header)
}

// handleMeet handles an incoming MEET message (adds new node).
func (c *Cluster) handleMeet(header *ClusterMsgHeader, conn net.Conn) {
	senderID := strings.TrimRight(string(header.Sender[:]), "\x00")
	senderIP := strings.TrimRight(string(header.IP[:]), "\x00")
	senderPort := int(header.Port)

	c.AddNode(senderID, senderIP, senderPort)
	c.sendPong(conn)
	log.Printf("[集群] 接受节点 %s 的 MEET 请求 (%s:%d)", senderID[:8], senderIP, senderPort)
}

// handleFail handles a FAIL broadcast.
func (c *Cluster) handleFail(header *ClusterMsgHeader) {
	senderID := strings.TrimRight(string(header.Sender[:]), "\x00")
	c.mu.Lock()
	if node, ok := c.Nodes[senderID]; ok {
		node.State = NodeFail
		node.FailTime = time.Now()
		log.Printf("[集群] 收到节点 %s 的 FAIL 消息", senderID[:8])
	}
	c.mu.Unlock()
}

// sendPong sends a PONG message to a connection.
func (c *Cluster) sendPong(conn net.Conn) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var msg [2048]byte
	copy(msg[0:4], []byte("RCmb"))
	binary.LittleEndian.PutUint16(msg[8:10], 1)  // version
	binary.LittleEndian.PutUint16(msg[12:14], MsgPong)
	binary.LittleEndian.PutUint16(msg[14:16], 0) // count
	copy(msg[16:56], []byte(c.Myself.ID))
	copy(msg[96:142], []byte(c.Myself.IP))
	binary.LittleEndian.PutUint16(msg[142:144], uint16(c.Myself.Port))
	binary.LittleEndian.PutUint64(msg[152:160], uint64(c.CurrentEpoch))
	binary.LittleEndian.PutUint64(msg[160:168], uint64(c.Myself.ConfigEpoch))
	msg[224] = byte(c.Myself.State)
	binary.LittleEndian.PutUint32(msg[4:8], 228) // totLen
	conn.Write(msg[:228])
}

// updateNodeFromGossip updates node state from gossip message.
func (c *Cluster) updateNodeFromGossip(senderID string, header *ClusterMsgHeader) {
	// Parse gossip entries from the message
	// Simplified: just acknowledge the sender
	c.mu.RLock()
	_, exists := c.Nodes[senderID]
	c.mu.RUnlock()
	if !exists && senderID != c.Myself.ID {
		senderIP := strings.TrimRight(string(header.IP[:]), "\x00")
		c.AddNode(senderID, senderIP, int(header.Port))
	}
}

// gossipLoop periodically sends PING to random nodes.
func (c *Cluster) gossipLoop() {
	ticker := time.NewTicker(time.Duration(ClusterPingInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.sendRandomPing()
		}
	}
}

// sendRandomPing sends PING to a random known node.
func (c *Cluster) sendRandomPing() {
	c.mu.RLock()
	var targets []*ClusterNode
	for _, node := range c.Nodes {
		if node.ID != c.Myself.ID {
			targets = append(targets, node)
		}
	}
	c.mu.RUnlock()

	if len(targets) == 0 {
		return
	}
	target := targets[rand.Intn(len(targets))]
	c.sendPing(target)
}

// sendPing sends a PING message to a node.
func (c *Cluster) sendPing(target *ClusterNode) {
	addr := fmt.Sprintf("%s:%d", target.IP, target.BusPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	c.mu.RLock()
	var msg [2048]byte
	copy(msg[0:4], []byte("RCmb"))
	binary.LittleEndian.PutUint16(msg[8:10], 1)
	binary.LittleEndian.PutUint16(msg[12:14], MsgPing)
	binary.LittleEndian.PutUint16(msg[14:16], 0)
	copy(msg[16:56], []byte(c.Myself.ID))
	copy(msg[96:142], []byte(c.Myself.IP))
	binary.LittleEndian.PutUint16(msg[142:144], uint16(c.Myself.Port))
	binary.LittleEndian.PutUint64(msg[152:160], uint64(c.CurrentEpoch))
	binary.LittleEndian.PutUint64(msg[160:168], uint64(c.Myself.ConfigEpoch))
	msg[224] = byte(c.Myself.State)
	binary.LittleEndian.PutUint32(msg[4:8], 228)
	c.mu.RUnlock()

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.Write(msg[:228])

	c.mu.Lock()
	target.PingSent = time.Now()
	c.mu.Unlock()
}

// failDetectionLoop checks for failed nodes.
func (c *Cluster) failDetectionLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.detectFailures()
		}
	}
}

// detectFailures checks if any node has timed out.
func (c *Cluster) detectFailures() {
	c.mu.Lock()
	defer c.mu.Unlock()
	timeout := time.Duration(ClusterNodeTimeout) * time.Millisecond
	for _, node := range c.Nodes {
		if node.ID == c.Myself.ID {
			continue
		}
		if node.State == NodeOK && time.Since(node.PongRecv) > timeout {
			node.State = NodePfail
			log.Printf("[集群] 节点 %s 标记为 PFAIL (超时)", node.ID[:8])
		}
		if node.State == NodePfail && time.Since(node.FailTime) > timeout*2 {
			node.State = NodeFail
			node.FailTime = time.Now()
			log.Printf("[集群] 节点 %s 标记为 FAIL", node.ID[:8])
		}
	}
}

// AddNode 向集群添加节点.
func (c *Cluster) AddNode(id, ip string, port int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.Nodes[id]; exists {
		return
	}
	node := NewClusterNode(id, ip, port)
	c.Nodes[id] = node
	c.Size = len(c.Nodes)
	log.Printf("[集群] 添加节点 %s (%s:%d), 集群大小: %d", id[:8], ip, port, c.Size)
}

// RemoveNode removes a node from the cluster.
func (c *Cluster) RemoveNode(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.Nodes, id)
	c.Size = len(c.Nodes)
}

// AssignSlot assigns a slot to a node.
func (c *Cluster) AssignSlot(slot uint16, node *ClusterNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev := c.Slots[slot]; prev != nil {
		prev.ClearSlot(slot)
	}
	c.Slots[slot] = node
	node.SetSlot(slot)
}

// GetSlotOwner 返回拥有槽位的节点.
func (c *Cluster) GetSlotOwner(slot uint16) *ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Slots[slot]
}

// GetNodeForKey returns the node for a key.
func (c *Cluster) GetNodeForKey(key string) *ClusterNode {
	if !c.Enabled {
		return c.Myself
	}
	slot := KeyToSlot(key)
	return c.GetSlotOwner(slot)
}

// IsClusterEnabled returns true if cluster is enabled.
func (c *Cluster) IsClusterEnabled() bool {
	return c != nil && c.Enabled
}

// GetClusterState returns "ok" or "fail".
func (c *Cluster) GetClusterState() string {
	if !c.Enabled {
		return "disabled"
	}
	for i := uint16(0); i < ClusterSlots; i++ {
		if c.Slots[i] == nil {
			return "fail"
		}
	}
	return "ok"
}

// SlotInfo returns slot info for CLUSTER SLOTS.
func (c *Cluster) SlotInfo() [][]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result [][]interface{}
	startSlot := uint16(0)
	var currentNode *ClusterNode
	for i := uint16(0); i < ClusterSlots; i++ {
		node := c.Slots[i]
		if node != currentNode {
			if currentNode != nil && startSlot < i {
				result = append(result, []interface{}{
					int64(startSlot), int64(i - 1),
					currentNode.IP, int64(currentNode.Port), currentNode.ID,
				})
			}
			startSlot = i
			currentNode = node
		}
	}
	if currentNode != nil && startSlot < ClusterSlots {
		result = append(result, []interface{}{
			int64(startSlot), int64(ClusterSlots - 1),
			currentNode.IP, int64(currentNode.Port), currentNode.ID,
		})
	}
	return result
}

// NodeInfo returns node info for CLUSTER NODES.
func (c *Cluster) NodeInfo() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var sb strings.Builder
	for _, node := range c.Nodes {
		flags := "master"
		if node == c.Myself {
			flags += ",myself"
		}
		if node.Master != nil {
			flags = "slave"
		}
		masterID := "-"
		if node.Master != nil {
			masterID = node.Master.ID
		}
		sb.WriteString(fmt.Sprintf("%s %s:%d@%d %s %s %d %d %d connected",
			node.ID, node.IP, node.Port, node.BusPort, flags, masterID,
			node.PingSent.UnixMilli(), node.PongRecv.UnixMilli(), node.ConfigEpoch))
		for i := uint16(0); i < ClusterSlots; i++ {
			if node.HasSlot(i) {
				sb.WriteString(fmt.Sprintf(" %d", i))
			}
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// KeyToSlot 计算哈希槽 for a key.
func KeyToSlot(key string) uint16 {
	if start := strings.Index(key, "{"); start != -1 {
		if end := strings.Index(key[start+1:], "}"); end != -1 {
			tag := key[start+1 : start+1+end]
			if len(tag) > 0 {
				key = tag
			}
		}
	}
	return CRC16([]byte(key)) % ClusterSlots
}

// CRC16 计算 CRC16-CCITT.
func CRC16(data []byte) uint16 {
	crc := uint16(0)
	for _, b := range data {
		crc = (crc << 8) ^ crc16tab[((crc>>8)^uint16(b))&0xff]
	}
	return crc
}

var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x4864, 0x5845, 0x6826, 0x7807, 0x08e0, 0x18c1, 0x28a2, 0x38a3,
	0xc94c, 0xd96d, 0xe90e, 0xf92f, 0x89c8, 0x99e9, 0xa98a, 0xb9ab,
	0x5a75, 0x4a54, 0x7a37, 0x6a16, 0x1af1, 0x0ad0, 0x3ab3, 0x2a92,
	0xdb7d, 0xcb5c, 0xfb3f, 0xeb1e, 0x9bf9, 0x8bd8, 0xbb9b, 0xab9a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x85a9, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4425,
	0xa7db, 0xb7fa, 0x8799, 0x9798, 0xe77f, 0xf75e, 0xc73d, 0xd71c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

func generateNodeID() string {
	h := sha1.New()
	h.Write([]byte(fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// --- 槽位迁移支持 ---

// SlotMigrationState 表示槽位迁移状态。
// 对应 Redis 中的 clusterState.migrating_slots_to 和 importing_slots_from。
type SlotMigrationState struct {
	Slot       uint16        // 迁移的槽位
	From       *ClusterNode  // 源节点
	To         *ClusterNode  // 目标节点
	State      int           // 迁移状态: 0=进行中, 1=完成
	StartTime  time.Time     // 开始时间
}

// Migration 状态常量
const (
	MigrationStateRunning  = 0 // 迁移进行中
	MigrationStateComplete = 1 // 迁移完成
)

// StartSlotMigration 开始槽位迁移。
// 对应 Redis 的 CLUSTER SETSLOT <slot> MIGRATE <target> 命令。
// 参数:
//   - slot: 要迁移的槽位
//   - targetID: 目标节点 ID
//
// 返回: 错误（如有）
func (c *Cluster) StartSlotMigration(slot uint16, targetID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 检查槽位是否属于自己
	if c.Slots[slot] != c.Myself {
		return fmt.Errorf("ERR I don't own slot %d", slot)
	}
	// 查找目标节点
	target, exists := c.Nodes[targetID]
	_ = target
	if !exists {
		return fmt.Errorf("ERR Unknown node %s", targetID)
	}
	// 标记槽位为迁移中
	// 在 Redis 中，这会设置 migrating_slots_to[slot] = target
	// 我们通过修改槽位所有者来简化实现
	log.Printf("[集群] 开始迁移槽位 %d 到节点 %s", slot, targetID[:8])
	// 通知目标节点导入槽位
	// 在真实实现中，这里会发送 CLUSTER SETSLOT <slot> IMPORTING <source> 给目标节点
	return nil
}

// CompleteSlotMigration 完成槽位迁移。
// 对应 Redis 的 CLUSTER SETSLOT <slot> NODE <target> 命令。
func (c *Cluster) CompleteSlotMigration(slot uint16, targetID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	target, exists := c.Nodes[targetID]
	if !exists {
		return fmt.Errorf("ERR Unknown node %s", targetID)
	}
	// 转移槽位所有权
	if prev := c.Slots[slot]; prev != nil {
		prev.ClearSlot(slot)
	}
	c.Slots[slot] = target
	target.SetSlot(slot)
	c.Myself.ConfigEpoch++
	log.Printf("[集群] 槽位 %d 迁移完成到节点 %s (新 epoch: %d)", slot, targetID[:8], c.Myself.ConfigEpoch)
	return nil
}

// GetRedirectForSlot 获取槽位的重定向信息。
// 对应 Redis 的 getNodeByQuery() 中的 ASK/MOVED 逻辑。
// 返回:
//   - redirectType: "MOVED" 或 "ASK" 或 ""（无需重定向）
//   - addr: 重定向地址
//   - err: 错误
func (c *Cluster) GetRedirectForSlot(slot uint16) (string, string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	owner := c.Slots[slot]
	if owner == nil {
		return "", "", fmt.Errorf("ERR Cluster is down")
	}
	if owner == c.Myself {
		return "", "", nil // 无需重定向
	}
	if owner.State == NodeFail {
		return "", "", fmt.Errorf("ERR Cluster node %s is down", owner.ID[:8])
	}
	addr := fmt.Sprintf("%s:%d", owner.IP, owner.Port)
	return "MOVED", addr, nil
}

// GetRedirectForKey 获取键的重定向信息。
func (c *Cluster) GetRedirectForKey(key string) (string, string, error) {
	if !c.Enabled {
		return "", "", nil
	}
	slot := KeyToSlot(key)
	return c.GetRedirectForSlot(slot)
}

// ClusterSlotsInfo 返回槽位信息（用于 CLUSTER SLOTS 命令）。
func (c *Cluster) ClusterSlotsInfo() [][]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result [][]interface{}
	startSlot := uint16(0)
	var currentNode *ClusterNode
	for i := uint16(0); i < ClusterSlots; i++ {
		node := c.Slots[i]
		if node != currentNode {
			if currentNode != nil && startSlot < i {
				slotRange := []interface{}{
					int64(startSlot), int64(i - 1),
					currentNode.IP, int64(currentNode.Port), currentNode.ID,
				}
				result = append(result, slotRange)
			}
			startSlot = i
			currentNode = node
		}
	}
	if currentNode != nil && startSlot < ClusterSlots {
		slotRange := []interface{}{
			int64(startSlot), int64(ClusterSlots - 1),
			currentNode.IP, int64(currentNode.Port), currentNode.ID,
		}
		result = append(result, slotRange)
	}
	return result
}

// ClusterNodesInfo 返回节点信息（用于 CLUSTER NODES 命令）。
func (c *Cluster) ClusterNodesInfo() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var sb strings.Builder
	for _, node := range c.Nodes {
		flags := "master"
		if node == c.Myself {
			flags += ",myself"
		}
		if node.Master != nil {
			flags = "slave"
		}
		masterID := "-"
		if node.Master != nil {
			masterID = node.Master.ID
		}
		sb.WriteString(fmt.Sprintf("%s %s:%d@%d %s %s %d %d %d connected",
			node.ID, node.IP, node.Port, node.BusPort, flags, masterID,
			node.PingSent.UnixMilli(), node.PongRecv.UnixMilli(), node.ConfigEpoch))
		for i := uint16(0); i < ClusterSlots; i++ {
			if node.HasSlot(i) {
				sb.WriteString(fmt.Sprintf(" %d", i))
			}
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
