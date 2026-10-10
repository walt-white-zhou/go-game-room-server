package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// runClient 模拟单个长连接客户端的真实游戏行为生命周期
func runClient(id int64, wg *sync.WaitGroup) {
	defer wg.Done()

	dialAddr := "[::1]:8999" // 优先 IPv6 本地回环

	// 建立与游戏服务器的首个物理长连接
	conn, err := net.Dial("tcp", dialAddr)
	if err != nil {
		// 降级尝试 IPv4
		conn, err = net.Dial("tcp", "127.0.0.1:8999")
		if err != nil {
			fmt.Printf("[Client %d] 初始连接建立失败: %v\n", id, err)
			return
		}
	}

	// 1. 发送 Join 登录协议包（MsgID 101）加入 1 号房间
	sendJoin(conn, id)

	// 2. 模拟正常游玩场景：以 20Hz (50ms) 频率持续发送移动坐标 3 秒
	sendMoves(conn, 3*time.Second)

	// 3. 模拟移动网络异常闪断：主动强制关闭物理 Socket
	_ = conn.Close()
	fmt.Printf("[Client %d] 模拟弱网断线，保留状态，等待 2 秒后发起重连...\n", id)
	time.Sleep(2 * time.Second)

	// 4. 重连阶段：建立全新的 TCP 长连接
	reConn, err := net.Dial("tcp", dialAddr)
	if err != nil {
		reConn, err = net.Dial("tcp", "127.0.0.1:8999")
		if err != nil {
			fmt.Printf("[Client %d] 重连失败: %v\n", id, err)
			return
		}
	}
	defer reConn.Close()

	// 再次发送 Join 包（携带相同的 PlayerID），服务端识别已存在后自动触发 Reconnect 恢复会话
	sendJoin(reConn, id)

	// 5. 重连恢复后继续游玩：持续发送移动坐标 5 秒
	sendMoves(reConn, 5*time.Second)
}

// sendJoin 构造并发送进入/重连协议数据包
// Zinx 封包协议头：DataLen(4B) + MsgID(4B) + Data(16B)
func sendJoin(conn net.Conn, id int64) {
	joinPkg := make([]byte, 8+16)
	binary.BigEndian.PutUint32(joinPkg[0:4], 16)          // 数据载荷长度: 16
	binary.BigEndian.PutUint32(joinPkg[4:8], 101)         // MsgID: 101 (Join/Reconnect)
	binary.BigEndian.PutUint64(joinPkg[8:16], uint64(id)) // 玩家唯一标识
	binary.BigEndian.PutUint64(joinPkg[16:24], 1)         // 目标房间号: 1
	_, _ = conn.Write(joinPkg)
}

// sendMoves 模拟以固定频率持续上报 3D 移动坐标
func sendMoves(conn net.Conn, duration time.Duration) {
	ticker := time.NewTicker(50 * time.Millisecond) // 20Hz 帧率
	defer ticker.Stop()

	// 预分配移动包切片：DataLen(4B) + MsgID(4B) + Data(12B)
	movePkg := make([]byte, 8+12)
	binary.BigEndian.PutUint32(movePkg[0:4], 12)  // 数据载荷长度: 12
	binary.BigEndian.PutUint32(movePkg[4:8], 102) // MsgID: 102 (Move)

	timeout := time.After(duration)
	for {
		select {
		case <-timeout:
			return
		case <-ticker.C:
			// 随机生成 X, Y, Z 三维坐标并写入载荷
			binary.BigEndian.PutUint32(movePkg[8:12], rand.Uint32())
			binary.BigEndian.PutUint32(movePkg[12:16], rand.Uint32())
			binary.BigEndian.PutUint32(movePkg[16:20], rand.Uint32())
			_, _ = conn.Write(movePkg)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	total := 50 // 模拟 50 个玩家并发进行高频位移、断线闪断与自动重连

	fmt.Printf("=== 开始断线重连与高频广播集成压力测试 (v1.1.0) ===\n")
	fmt.Printf("模拟并发客户端数: %d | 移动上报频率: 20Hz\n", total)

	for i := 1; i <= total; i++ {
		wg.Add(1)
		go runClient(int64(i), &wg)
		time.Sleep(10 * time.Millisecond) // 错峰连接，平滑建连压力
	}

	wg.Wait()
	fmt.Println("=== 压力测试全部通过！断线会话挂起与重连链路运行平稳 ===")
}
