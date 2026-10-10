package main

import (
	"encoding/binary"
	"fmt"
	"github.com/aceld/zinx/ziface"
	"github.com/aceld/zinx/znet"
	"go-game-room-server/internal/room"
	"go-game-room-server/internal/router"
)

// JoinRouter 玩家进入房间路由（MsgID: 101）
// 客户端上行二进制协议：PlayerID(8B) + RoomID(8B) = 16 字节
type JoinRouter struct {
	znet.BaseRouter
}

func (jr *JoinRouter) Handle(req ziface.IRequest) {
	data := req.GetData()
	if len(data) < 16 {
		return
	}

	// 解析出玩家 ID 与想要进入的房间号
	playerID := int64(binary.BigEndian.Uint64(data[0:8]))
	roomID := int64(binary.BigEndian.Uint64(data[8:16]))

	// 获取或创建房间实体
	r := room.RoomMgr.GetOrCreate(roomID)

	// 初始化玩家上下文
	p := &room.Player{
		ID:   playerID,
		Name: fmt.Sprintf("Player_%d", playerID),
		Conn: req.GetConnection(),
	}

	// 利用 Zinx 的属性注入将业务实体绑定到长连接会话中
	req.GetConnection().SetProperty("player", p)
	req.GetConnection().SetProperty("room", r)

	// 投递加入事件到房间 Actor
	r.PostEvent(room.Event{
		Type:   room.EventJoin,
		Player: p,
	})
}

// onLost 连接断开生命周期钩子（客户端掉线、主动离线或超时踢出）
func onLost(conn ziface.IConnection) {
	pProp, err1 := conn.GetProperty("player")
	rProp, err2 := conn.GetProperty("room")
	if err1 == nil && err2 == nil {
		// 客户端断开连接时，安全通知房间 Actor 将其剔除，避免内存泄漏与僵尸连接
		rProp.(*room.Room).PostEvent(room.Event{
			Type:   room.EventLeave,
			Player: pProp.(*room.Player),
		})
	}
}

func main() {
	// 创建 Zinx TCP 服务端实例
	s := znet.NewServer()

	// 注册连接断开监听钩子
	s.SetOnConnStop(onLost)

	// 注册协议路由
	s.AddRouter(101, &JoinRouter{})        // 101: 登录/进入房间
	s.AddRouter(102, &router.MoveRouter{}) // 102: 位置移动同步

	fmt.Println("游戏房间服务端启动完成，正在监听连接...")
	// 阻塞启动服务
	s.Serve()
}
