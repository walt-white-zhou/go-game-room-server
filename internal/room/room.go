// Package room 所有请求串行处理，彻底消灭房间内部的全局互斥锁。
package room

import (
	"fmt"
	"time"

	"github.com/aceld/zinx/ziface"
)

// PlayerStatus 玩家在房间生命周期内的状态枚举
type PlayerStatus int

const (
	// StatusOnline 玩家在线状态：正常接收位置同步、可以上报行为
	StatusOnline PlayerStatus = 1

	// StatusSuspended 玩家断线挂起状态：长连接已断开，但内存实体和坐标现场保留在房间中
	// 允许在租期保护窗口（如 30s）内重连恢复；超时未重连则触发彻底销毁
	StatusSuspended PlayerStatus = 2
)

// Player 玩家在房间内的内存映射实体
type Player struct {
	ID         int64              // 玩家全局唯一 ID
	Name       string             // 玩家昵称
	Conn       ziface.IConnection // 绑定的底层 TCP 长连接实例（断线时置为 nil）
	X, Y, Z    float32            // 玩家在房间场景中的三维空间坐标
	Status     PlayerStatus       // 玩家当前生命周期状态（Online / Suspended）
	LastActive time.Time          // 最后一次活跃时间戳（心跳或上报位移）
}

// EventType 房间 Actor 事件类型枚举
type EventType int

const (
	EventJoin         EventType = iota + 1 // 首次加入房间
	EventMove                              // 位置移动同步
	EventDisconnect                        // 网络底层断开（触发进入挂起态）
	EventReconnect                         // 携带旧 PlayerID 重新连接并恢复会话
	EventTimeoutPurge                      // 重连租期超时，彻底从房间清除实体
)

// Event 投递给房间独占 Actor 事件通道的消息结构体
type Event struct {
	Type    EventType          // 事件类型
	Player  *Player            // 触发事件的玩家基础信息
	X, Y, Z float32            // 移动坐标增量/新坐标
	NewConn ziface.IConnection // 重连场景下绑定的全新 TCP 连接
}

// Room 房间实体（核心 Actor 模式实现）
// 房间内所有核心数据读写（如 players map 增删改查、坐标变更、状态机迁移）
// 均严格封装在独占的 loop 协程内串行化执行，完全消除了全局大锁竞争与死锁风险。
type Room struct {
	ID        int64             // 房间唯一标识
	players   map[int64]*Player // 房间内的玩家列表（仅在 loop 协程内部无锁安全读写）
	eventChan chan Event        // 异步事件输入通道（Actor 信箱）
	quitChan  chan struct{}     // 房间销毁与生命周期终止通道
}

// NewRoom 创建并启动一个新房间的 Actor 协程
func NewRoom(id int64) *Room {
	r := &Room{
		ID:        id,
		players:   make(map[int64]*Player),
		eventChan: make(chan Event, 2048), // 带缓冲通道，用于平滑吸收网络并发高峰
		quitChan:  make(chan struct{}),
	}
	// 启动房间私有的独立单协程事件循环
	go r.loop()
	return r
}

// loop 房间核心事件循环（单协程独占，单线程模型保证无锁安全）
func (r *Room) loop() {
	for {
		select {
		case ev := <-r.eventChan:
			switch ev.Type {
			case EventJoin:
				// --- 场景 1：玩家首次加入房间 ---
				r.players[ev.Player.ID] = ev.Player
				ev.Player.Status = StatusOnline
				ev.Player.LastActive = time.Now()
				fmt.Printf("[Room %d] 玩家 %d 加入房间，当前总人数: %d\n", r.ID, ev.Player.ID, len(r.players))

			case EventMove:
				// --- 场景 2：玩家高频移动 ---
				// 仅处理处于在线状态（StatusOnline）的玩家位移，忽略已挂起断线玩家的残余包
				if p, ok := r.players[ev.Player.ID]; ok && p.Status == StatusOnline {
					p.X, p.Y, p.Z = ev.X, ev.Y, ev.Z
					p.LastActive = time.Now()
					// 触发同房间零拷贝广播
					r.broadcastMove(p)
				}

			case EventDisconnect:
				// --- 场景 3：弱网或客户端主动断线（触发断线挂起） ---
				if p, ok := r.players[ev.Player.ID]; ok {
					// 状态切换为 Suspended，置空底层断开的 Socket 引用
					p.Status = StatusSuspended
					p.Conn = nil
					fmt.Printf("[Room %d] 玩家 %d 连接中断，进入 30s 重连租期保留状态\n", r.ID, p.ID)

					// 启动独立的租期保护定时协程：
					// 30 秒后若玩家未恢复连接，投递 EventTimeoutPurge 事件执行最终销毁
					go func(pid int64) {
						time.Sleep(30 * time.Second)
						r.PostEvent(Event{
							Type:   EventTimeoutPurge,
							Player: &Player{ID: pid},
						})
					}(p.ID)
				}

			case EventReconnect:
				// --- 场景 4：重连恢复握手 ---
				if p, ok := r.players[ev.Player.ID]; ok {
					// 重新挂载新建立的物理长连接
					p.Conn = ev.NewConn
					p.Status = StatusOnline
					p.LastActive = time.Now()
					fmt.Printf("[Room %d] 玩家 %d 重连成功！恢复游戏现场与下行推送通道\n", r.ID, p.ID)
				}

			case EventTimeoutPurge:
				// --- 场景 5：重连租期超时兜底清理 ---
				// 关键校验：必须仍然处于 Suspended 状态才清理。
				// 若玩家在 30s 内已重新连接（变为了 StatusOnline），则放弃清除。
				if p, ok := r.players[ev.Player.ID]; ok && p.Status == StatusSuspended {
					delete(r.players, p.ID)
					fmt.Printf("[Room %d] 玩家 %d 超过 30s 未重连，实体彻底销毁，剩余玩家数: %d\n", r.ID, p.ID, len(r.players))
				}
			}

		case <-r.quitChan:
			// 房间关闭，退出事件循环，回收 Goroutine 资源
			return
		}
	}
}

// PostEvent 向房间 Actor 投递事件（非阻塞，带有背压控制）
// 【防雪崩机制】：当恶劣网络或恶意刷包导致通道填满时，丢弃超频位移包，防止服务器 OOM
func (r *Room) PostEvent(ev Event) {
	select {
	case r.eventChan <- ev:
	default:
		// 通道积压时触发背压（Backpressure）丢弃，避免阻塞网络读协程
	}
}

// HasPlayer 查询玩家是否已存在于房间内存中（用于快速鉴别初次加入 vs 断线重连）
func (r *Room) HasPlayer(playerID int64) bool {
	_, ok := r.players[playerID]
	return ok
}

// broadcastMove 执行房间内的位移状态广播
func (r *Room) broadcastMove(source *Player) {
	// 1. 过滤出本房间内所有在线且具备有效 Socket 连接的其他玩家
	targets := make([]ziface.IConnection, 0, len(r.players))
	for id, p := range r.players {
		if id != source.ID && p.Status == StatusOnline && p.Conn != nil {
			targets = append(targets, p.Conn)
		}
	}

	count := int32(len(targets))
	if count == 0 {
		return
	}

	// 2. 从对象池申请零分配缓冲并初始化原子引用计数（引用计数 = 接收端总数）
	pkt := AcquirePacket(source.ID, source.X, source.Y, source.Z, count)

	// 3. 多路异步推送到各连接的写缓冲区
	for _, conn := range targets {
		go func(c ziface.IConnection) {
			// 消息号 202: 移动广播推送
			_ = c.SendMsg(202, *pkt.Data)
			// 该连接处理完毕，原子释放一次计数，最后一个连接自动将切片还池
			pkt.Release()
		}(conn)
	}
}
