// Package room 所有请求串行处理，彻底消灭房间内部的全局互斥锁。
package room

import (
	"github.com/aceld/zinx/ziface"
)

// Player 玩家在房间内的内存映射实体
type Player struct {
	ID   int64              // 玩家全局唯一标识
	Name string             // 玩家昵称
	Conn ziface.IConnection // 绑定的长连接实例，用于下行推送
	X    float32            // 本地模拟坐标 X
	Y    float32            // 本地模拟坐标 Y
	Z    float32            // 本地模拟坐标 Z
}

// EventType 房间事件类型枚举
type EventType int

const (
	EventJoin  EventType = iota + 1 // 玩家加入事件
	EventMove                       // 玩家移动事件
	EventLeave                      // 玩家退出/断开连接事件
)

// Event 投递给房间 Actor 事件队列的通用消息体
type Event struct {
	Type    EventType // 事件类型
	Player  *Player   // 关联触发玩家
	X, Y, Z float32   // 移动参数载荷
}

// Room 房间实体（核心 Actor 模型）
// 房间内所有的状态读写（坐标变动、玩家进出）全部由独占的单个 Goroutine（loop）串行执行。
// 彻底放弃在玩家 Map 上使用 sync.RWMutex，从根本上杜绝死锁与锁竞争损耗。
type Room struct {
	ID        int64             // 房间 ID
	players   map[int64]*Player // 房间内的玩家列表（仅在 loop 协程内部无锁读写）
	eventChan chan Event        // 异步事件通道（Actor 信箱）
	quitChan  chan struct{}     // 房间关闭通知通道
}

// NewRoom 创建并启动一个新房间的 Actor 协程
func NewRoom(id int64) *Room {
	r := &Room{
		ID:        id,
		players:   make(map[int64]*Player),
		eventChan: make(chan Event, 2048), // 带缓冲队列，吸收瞬时发包波峰
		quitChan:  make(chan struct{}),
	}
	// 启动房间私有的独立事件循环
	go r.loop()
	return r
}

// loop 房间核心事件循环（单协程独占执行）
func (r *Room) loop() {
	for {
		select {
		case ev := <-r.eventChan:
			switch ev.Type {
			case EventJoin:
				// 处理玩家进入：注册到房间玩家列表
				r.players[ev.Player.ID] = ev.Player

			case EventMove:
				// 处理玩家移动：单协程无锁更新坐标并触发多播
				if p, ok := r.players[ev.Player.ID]; ok {
					p.X, p.Y, p.Z = ev.X, ev.Y, ev.Z
					r.broadcastMove(p)
				}

			case EventLeave:
				// 处理玩家离开：清理房间内资源
				delete(r.players, ev.Player.ID)
			}
		case <-r.quitChan:
			// 房间销毁退出
			return
		}
	}
}

// PostEvent 向房间 Actor 投递事件（非阻塞，支持背压丢包）
// 【防雪崩机制】：当客户端恶意超频发包导致信箱满载时，直接丢弃非核心冗余包，保护服务不崩溃
func (r *Room) PostEvent(ev Event) {
	select {
	case r.eventChan <- ev:
	default:
		// 队列打满时触发背压（Backpressure），丢弃该帧事件，防止服务器内存雪崩
	}
}

// broadcastMove 执行房间内的位移状态广播
func (r *Room) broadcastMove(source *Player) {
	// 1. 过滤并收集所有有效的其他玩家长连接
	targets := make([]ziface.IConnection, 0, len(r.players))
	for id, p := range r.players {
		if id != source.ID && p.Conn != nil {
			targets = append(targets, p.Conn)
		}
	}

	count := int32(len(targets))
	if count == 0 {
		return
	}

	// 2. 从对象池构建引用计数包（初始引用为接收端总数）
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
