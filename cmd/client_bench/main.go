package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// runClient 模拟单个长连接客户端的行为模型
func runClient(id int64, wg *sync.WaitGroup) {
	defer wg.Done()

	// 建立与游戏服务器的长连接
	conn, err := net.Dial("tcp", "[::1]:8999")
	if err != nil {
		fmt.Printf("[Client %d] 连接失败: %v\n", id, err)
		return
	}
	defer conn.Close()

	// 1. 组装并发送 Join 协议包 (Zinx 封包规范: Len(4B) + MsgID(4B) + Data(16B))
	joinPkg := make([]byte, 8+16)
	binary.BigEndian.PutUint32(joinPkg[0:4], 16)          // 数据长度: 16
	binary.BigEndian.PutUint32(joinPkg[4:8], 101)         // MsgID: 101
	binary.BigEndian.PutUint64(joinPkg[8:16], uint64(id)) // PlayerID
	binary.BigEndian.PutUint64(joinPkg[16:24], 1)         // 统一加入 1 号房间
	_, _ = conn.Write(joinPkg)

	// 2. 模拟高频移动：以 20Hz (50ms) 频率持续上报坐标
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// 预分配移动数据包 (Len(4B) + MsgID(4B) + Data(12B))
	movePkg := make([]byte, 8+12)
	binary.BigEndian.PutUint32(movePkg[0:4], 12)  // 数据长度: 12
	binary.BigEndian.PutUint32(movePkg[4:8], 102) // MsgID: 102

	// 压测持续时间：15 秒
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			return
		case <-ticker.C:
			// 随机生成三维坐标点
			binary.BigEndian.PutUint32(movePkg[8:12], rand.Uint32())
			binary.BigEndian.PutUint32(movePkg[12:16], rand.Uint32())
			binary.BigEndian.PutUint32(movePkg[16:20], rand.Uint32())
			_, _ = conn.Write(movePkg)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	total := 200 // 模拟 200 个玩家同时在 1 号房间高频移动广播

	fmt.Printf("=== 开始高频广播压力测试 ===\n")
	fmt.Printf("虚拟玩家并发数: %d | 上报频率: 20Hz (50ms)\n", total)

	for i := 1; i <= total; i++ {
		wg.Add(1)
		go runClient(int64(i), &wg)
		time.Sleep(2 * time.Millisecond) // 错峰建立连接，避免瞬时半连接队列溢出
	}

	wg.Wait()
	fmt.Println("=== 压力测试结束，服务器运行平稳 ===")
}
