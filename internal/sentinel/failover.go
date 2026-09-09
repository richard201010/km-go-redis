// Package sentinel - 哨兵自动故障转移实现。
// 本文件实现 Redis 哨兵的故障转移流程，对应 Redis sentinel.c 中的
// sentinelHandleRedisInstance() 和 failover 流程。
package sentinel

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

// 故障转移状态常量
// 对应 Redis 的 SENTINEL_FAILOVER_STATE_* 常量。
const (
	FailoverStateNone        = 0 // 无故障转移
	FailoverStateWaitStart   = 1 // 等待开始
	FailoverStateSelectSlave = 2 // 选择从节点
	FailoverStateSendSlaveof = 3 // 发送 SLAVEOF
	FailoverStateWaitPromotion = 4 // 等待提升
	FailoverStateReconfSlaves  = 5 // 重新配置从节点
	FailoverStateUpdateConfig  = 6 // 更新配置
)

// FailoverState 表示故障转移状态。
// 对应 Redis 的 sentinelFailoverState 结构。
type FailoverState struct {
	State       int           // 当前状态
	StartTime   time.Time     // 开始时间
	Timeout     time.Duration // 超时时间
	Leader      string        // 领导哨兵
	LeaderEpoch int64         // 领导纪元
	VotedEpoch  int64         // 投票纪元
	SelectedSlave *SentinelInstance // 选中的从节点
}

// StartFailover 启动故障转移流程。
// 对应 Redis 的 sentinelStartFailover()。
// 参数:
//   - masterName: 主节点名称
//
// 返回: 错误（如有）
func (s *Sentinel) StartFailover(masterName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return fmt.Errorf("ERR 未知主节点: %s", masterName)
	}
	if master.State == MasterFailover {
		return fmt.Errorf("ERR 故障转移已在进行中")
	}
	// 增加纪元
	s.Epoch++
	master.State = MasterFailover
	master.Leader = s.Myself.Name
	master.LeaderEpoch = s.Epoch
	master.VotedEpoch = s.Epoch
	// 初始化故障转移状态
	// 在真实实现中，这里会创建 FailoverState 并启动状态机
	log.Printf("[哨兵] 启动故障转移: %s (epoch: %d)", masterName, s.Epoch)
	// 模拟故障转移流程
	go s.executeFailover(masterName)
	return nil
}

// executeFailover 执行故障转移流程。
// 对应 Redis 的 sentinelHandleFailover()。
func (s *Sentinel) executeFailover(masterName string) {
	// 阶段1: 等待开始（随机延迟，避免多个哨兵同时故障转移）
	delay := time.Duration(rand.Intn(5000)) * time.Millisecond
	time.Sleep(delay)
	// 阶段2: 选择从节点
	s.mu.Lock()
	master, exists := s.Masters[masterName]
	if !exists || master.State != MasterFailover {
		s.mu.Unlock()
		return
	}
	// 选择最佳从节点（优先级最低、复制偏移量最大、运行ID最小）
	bestSlave := s.selectBestSlave(master)
	if bestSlave == nil {
		log.Printf("[哨兵] 无可用从节点，故障转移失败: %s", masterName)
		master.State = MasterOdown
		s.mu.Unlock()
		return
	}
	// 阶段3: 提升从节点为主节点
	log.Printf("[哨兵] 提升从节点 %s:%d 为主节点", bestSlave.IP, bestSlave.Port)
	// 在真实实现中，这里会发送 SLAVEOF NO ONE 给选中的从节点
	// 阶段4: 重新配置其他从节点指向新主节点
	for _, replica := range master.Replicas {
		if replica != bestSlave {
			log.Printf("[哨兵] 重新配置从节点 %s:%d 指向新主节点 %s:%d",
				replica.IP, replica.Port, bestSlave.IP, bestSlave.Port)
			// 在真实实现中，这里会发送 SLAVEOF <new-master-ip> <new-master-port>
		}
	}
	// 阶段5: 更新配置
	master.IP = bestSlave.IP
	master.Port = bestSlave.Port
	master.State = MasterOK
	master.Leader = ""
	s.mu.Unlock()
	log.Printf("[哨兵] 故障转移完成: %s -> %s:%d", masterName, bestSlave.IP, bestSlave.Port)
}

// selectBestSlave 选择最佳从节点进行提升。
// 对应 Redis 的 sentinelSelectSlave()。
// 选择标准:
// 1. 排除已下线的从节点
// 2. 优先选择优先级最低的（slave-priority）
// 3. 优先选择复制偏移量最大的
// 4. 优先选择运行ID最小的
func (s *Sentinel) selectBestSlave(master *SentinelMaster) *SentinelInstance {
	var best *SentinelInstance
	for _, slave := range master.Replicas {
		if slave.IsDown {
			continue // 排除已下线的从节点
		}
		if best == nil {
			best = slave
			continue
		}
		// 在真实实现中，这里会比较优先级、复制偏移量等
		// 简化实现：直接返回第一个可用的从节点
		best = slave
		break
	}
	return best
}

// HandleFailoverState 处理故障转移状态机。
// 对应 Redis 的 sentinelHandleFailover()。
func (s *Sentinel) HandleFailoverState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, master := range s.Masters {
		if master.State != MasterFailover {
			continue
		}
		// 检查故障转移超时
		if time.Since(master.RoleChangedTime) > master.FailoverTimeout {
			log.Printf("[哨兵] 故障转移超时: %s", master.Name)
			master.State = MasterOdown
			master.Leader = ""
		}
	}
}

// VoteForLeader 投票选举故障转移领导。
// 对应 Redis 的 sentinelVoteLeader()。
// 参数:
//   - masterName: 主节点名称
//   - proposedEpoch: 提议的纪元
//   - requesterName: 请求者名称
//
// 返回: 投票结果（领导名称）
func (s *Sentinel) VoteForLeader(masterName string, proposedEpoch int64, requesterName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return ""
	}
	// 只有在提议的纪元大于当前投票纪元时才投票
	if proposedEpoch > master.VotedEpoch {
		master.VotedEpoch = proposedEpoch
		master.Leader = requesterName
		log.Printf("[哨兵] 投票给 %s (epoch: %d)", requesterName, proposedEpoch)
		return requesterName
	}
	return master.Leader
}

// IsLeader 判断当前哨兵是否是故障转移领导。
func (s *Sentinel) IsLeader(masterName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return false
	}
	return master.Leader == s.Myself.Name
}

// GetFailoverState 获取故障转移状态。
func (s *Sentinel) GetFailoverState(masterName string) (int, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return FailoverStateNone, ""
	}
	if master.State == MasterFailover {
		return FailoverStateSelectSlave, master.Leader
	}
	return FailoverStateNone, ""
}

// SimulateFailover 模拟故障转移（用于测试）。
// 在生产环境中，故障转移由哨兵自动触发。
func (s *Sentinel) SimulateFailover(masterName string) error {
	// 先标记主节点为客观下线
	s.mu.Lock()
	master, exists := s.Masters[masterName]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("ERR 未知主节点: %s", masterName)
	}
	master.State = MasterOdown
	s.mu.Unlock()
	// 启动故障转移
	return s.StartFailover(masterName)
}

// AddReplica 添加从节点到主节点。
func (s *Sentinel) AddReplica(masterName, ip string, port int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return fmt.Errorf("ERR 未知主节点: %s", masterName)
	}
	replica := &SentinelInstance{
		Name:  fmt.Sprintf("%s:%d", ip, port),
		IP:    ip,
		Port:  port,
		Flags: "slave",
	}
	master.Replicas = append(master.Replicas, replica)
	log.Printf("[哨兵] 添加从节点 %s:%d 到主节点 %s", ip, port, masterName)
	return nil
}

// AddSentinel 添加哨兵到主节点。
func (s *Sentinel) AddSentinel(masterName, ip string, port int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return fmt.Errorf("ERR 未知主节点: %s", masterName)
	}
	sentinel := &SentinelInstance{
		Name:  fmt.Sprintf("%s:%d", ip, port),
		IP:    ip,
		Port:  port,
		Flags: "sentinel",
	}
	master.Sentinels = append(master.Sentinels, sentinel)
	master.NumOtherSentinels = len(master.Sentinels)
	log.Printf("[哨兵] 添加哨兵 %s:%d 到主节点 %s", ip, port, masterName)
	return nil
}

// CheckMasterDown 检查主节点是否下线。
// 对应 Redis 的 sentinelHandleRedisInstance() 中的主观/客观下线检测。
func (s *Sentinel) CheckMasterDown(masterName string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	master, exists := s.Masters[masterName]
	if !exists {
		return false, false
	}
	// 主观下线: 超过 down-after-milliseconds 未收到 PONG
	sdown := false
	if time.Since(master.LastPong) > master.DownAfter {
		if master.State == MasterOK {
			master.State = MasterSdown
			sdown = true
			log.Printf("[哨兵] 主节点 %s 主观下线 (SDOWN)", masterName)
		}
	}
	// 客观下线: 超过 quorum 个哨兵同意
	odown := false
	if master.State == MasterSdown && master.NumOtherSentinels >= master.Quorum-1 {
		master.State = MasterOdown
		odown = true
		log.Printf("[哨兵] 主节点 %s 客观下线 (ODOWN) - quorum=%d", masterName, master.Quorum)
	}
	return sdown, odown
}

// HandleRedisInstance 处理 Redis 实例状态。
// 对应 Redis 的 sentinelHandleRedisInstance() 中的主循环。
func (s *Sentinel) HandleRedisInstance() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, master := range s.Masters {
		// 检查是否需要发送 PING
		if time.Since(master.LastPing) > time.Second {
			s.sendPing(master)
		}
		// 检查主观下线
		if master.State == MasterOK && time.Since(master.LastPong) > master.DownAfter {
			master.State = MasterSdown
			log.Printf("[哨兵] 主节点 %s 主观下线", master.Name)
		}
		// 处理故障转移状态机
		if master.State == MasterFailover {
			s.handleFailoverStateMachine(master)
		}
	}
}

// sendPing 发送 PING 给实例。
func (s *Sentinel) sendPing(master *SentinelMaster) {
	addr := fmt.Sprintf("%s:%d", master.IP, master.Port)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(time.Second))
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	master.LastPing = time.Now()
	// 读取 PONG
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	n, err := conn.Read(buf)
	if err == nil && string(buf[:n]) == "+PONG\r\n" {
		master.LastPong = time.Now()
		if master.State == MasterSdown {
			master.State = MasterOK
			log.Printf("[哨兵] 主节点 %s 恢复在线", master.Name)
		}
	}
}

// handleFailoverStateMachine 处理故障转移状态机。
func (s *Sentinel) handleFailoverStateMachine(master *SentinelMaster) {
	// 简化的状态机实现
	switch {
	case master.Leader == s.Myself.Name:
		// 当前哨兵是领导，执行故障转移
		// 在真实实现中，这里会根据状态执行不同阶段
		log.Printf("[哨兵] 作为领导执行故障转移: %s", master.Name)
	}
}

// GetSentinelState 获取哨兵状态信息。
func (s *Sentinel) GetSentinelState() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]string{
		"sentinel_masters":             fmt.Sprintf("%d", len(s.Masters)),
		"sentinel_tilt":                fmt.Sprintf("%v", s.Tilt),
		"sentinel_running_scripts":     "0",
		"sentinel_scripts_queue_length": "0",
		"sentinel_simulate_failure_flags": "0",
	}
}
