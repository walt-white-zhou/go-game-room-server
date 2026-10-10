package main

import (
	"encoding/binary"
	"fmt"
	"go-game-room-server/internal/room"
	"go-game-room-server/internal/router"

	"github.com/aceld/zinx/ziface"
	"github.com/aceld/zinx/znet"
)

// JoinRouter 玩家进入房间 / 断线重连统一路由 (MsgID: 101)
// 客户端上行二进制协议：PlayerID(8B) + RoomID(8B) = 16 字节
type JoinRouter struct {
	znet.BaseRouter
}

// Handle 处理客户端登录/进入房间请求
func (jr *JoinRouter) Handle(req ziface.IRequest) {
	data := req.GetData()
	if len(data) < 16 {
		return // 过滤非法长度畸形包
	}

	// 解析出玩家唯一 ID 和目标房间号
	playerID := int64(binary.BigEndian.Uint64(data[0:8]))
	roomID := int64(binary.BigEndian.Uint64(data[8:16]))

	// 获取或新建房间实例
	r := room.RoomMgr.GetOrCreate(roomID)

	p := &room.Player{
		ID:   playerID,
		Name: fmt.Sprintf("Player_%d", playerID),
		Conn: req.GetConnection(),
	}

	// 利用 Zinx 的上下文属性注入机制，将房间和玩家实体绑定到网络会话中
	req.GetConnection().SetProperty("player", p)
	req.GetConnection().SetProperty("room", r)

	// 【重连鉴权逻辑分支】：
	// 判断房间内存中是否已有该玩家（例如之前掉线进入挂起态）
	if r.HasPlayer(playerID) {
		// 命中重连逻辑：更新长连接句柄并恢复在线状态
		r.PostEvent(room.Event{
			Type:    room.EventReconnect,
			Player:  p,
			NewConn: req.GetConnection(),
		})
	} else {
		// 初次加入房间逻辑
		r.PostEvent(room.Event{
			Type:   room.EventJoin,
			Player: p,
		})
	}
}

// HeartbeatRouter 网络探活与心跳 Ping 处理 (MsgID: 103)
type HeartbeatRouter struct {
	znet.BaseRouter
}

// Handle 收到客户端 Ping 心跳，立即响应 Pong（MsgID: 203）
func (hr *HeartbeatRouter) Handle(req ziface.IRequest) {
	// 回复极简的二进制/字符串 Pong 包
	_ = req.GetConnection().SendMsg(203, []byte("pong"))
}

// onLost Zinx 连接断开生命周期监听钩子（客户端弱网断开、网络波动或主动断线时触发）
func onLost(conn ziface.IConnection) {
	pProp, err1 := conn.GetProperty("player")
	rProp, err2 := conn.GetProperty("room")
	if err1 == nil && err2 == nil {
		// 触发会话挂起断线事件，进入 30s 重连租期保留，而不是立刻注销
		rProp.(*room.Room).PostEvent(room.Event{
			Type:   room.EventDisconnect,
			Player: pProp.(*room.Player),
		})
	}
}

func main() {
	// 初始化 Zinx TCP 服务端引擎
	s := znet.NewServer()

	// 注册全局连接断开事件回调
	s.SetOnConnStop(onLost)

	// 注册长连接消息路由
	s.AddRouter(101, &JoinRouter{})        // 101: 登录 / 进入 / 重连
	s.AddRouter(102, &router.MoveRouter{}) // 102: 位置移动同步
	s.AddRouter(103, &HeartbeatRouter{})   // 103: 客户端心跳探活

	fmt.Println("游戏房间服务端 v1.1.0 启动完成，正在监听连接...")

	// 启动长连接监听并阻塞服务进程
	s.Serve()
}
