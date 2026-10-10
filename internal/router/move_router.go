// Package router 路由逻辑
package router

import (
	"encoding/binary"
	"github.com/aceld/zinx/ziface"
	"github.com/aceld/zinx/znet"
	"go-game-room-server/internal/room"
	"math"
)

// MoveRouter 玩家移动请求路由处理（MsgID: 102）
type MoveRouter struct {
	znet.BaseRouter
}

// Handle 处理网络层收到的客户端移动二进制包
// 客户端上行二进制协议：X(4B) + Y(4B) + Z(4B) = 12 字节
func (mr *MoveRouter) Handle(req ziface.IRequest) {
	data := req.GetData()
	if len(data) < 12 {
		return // 校验包长度，非法畸形包直接丢弃
	}

	// 零反射快速二进制反序列化
	x := math.Float32frombits(binary.BigEndian.Uint32(data[0:4]))
	y := math.Float32frombits(binary.BigEndian.Uint32(data[4:8]))
	z := math.Float32frombits(binary.BigEndian.Uint32(data[8:12]))

	conn := req.GetConnection()

	// 从 Zinx 连接属性中提取绑定的游戏上下文
	pProp, err1 := conn.GetProperty("player")
	rProp, err2 := conn.GetProperty("room")
	if err1 != nil || err2 != nil {
		return // 尚未加入房间的非法移动请求，忽略
	}

	p := pProp.(*room.Player)
	r := rProp.(*room.Room)

	// 将网络 I/O 协程收到的数据封装为事件，投递给房间独占的 Actor 事件信箱
	r.PostEvent(room.Event{
		Type:   room.EventMove,
		Player: p,
		X:      x,
		Y:      y,
		Z:      z,
	})
}
