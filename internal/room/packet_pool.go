// Package room 解决异步多播下的内存逃逸与生命周期错位（零拷贝 + 引用计数归还）。
package room

import (
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
)

// MovePacketSize 固定长度的移动广播二进制包大小（字节数）
// 协议布局：PlayerID(8B) + X(4B) + Y(4B) + Z(4B) = 20B
// 相比 JSON 格式（约 60+ 字节），节省 65% 以上的网络带宽，且无任何反射损耗
const MovePacketSize = 20

// bufPool 字节切片对象池，避免每次广播都执行 make([]byte, 20) 触发 GC 堆分配
var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, MovePacketSize)
		return &buf
	},
}

// RefPacket 带有原子引用计数的网络广播包包裹器
// 【解决痛点】：
// 在异步网络发送中，如果逻辑协程提前将切片 Put 回 pool，而某些慢连接的 socket.Write 尚未执行，
// 就会导致内存被其他协程覆写，引发数据污染。
// 引入原子引用计数：初始引用等于目标接收客户端数，每个写协程发送完毕递减 1，减至 0 时由最后一个协程负责回池。
type RefPacket struct {
	Data     *[]byte
	refCount int32
}

// refPool RefPacket 结构体本身的对象池，消除包装结构体引发的微小逃逸
var refPool = sync.Pool{
	New: func() interface{} {
		return &RefPacket{}
	},
}

// AcquirePacket 从内存池申请缓冲，完成零分配二进制编码，并初始化引用计数
// playerID: 发送移动事件的玩家 ID
// x, y, z: 玩家的三维空间坐标
// targets: 本次广播的目标接收端总数
func AcquirePacket(playerID int64, x, y, z float32, targets int32) *RefPacket {
	bufPtr := bufPool.Get().(*[]byte)
	buf := *bufPtr
	// 边界检查消除优化（BCE, Bounds Check Elimination）
	// 通过对下标 19 进行一次安全断言，Go 编译器后续所有的切片写入指令将彻底消除边界检测指令，提升 CPU 指令吞吐
	_ = buf[19] // 边界消除检查 (BCE)

	// 纯手写大端序（BigEndian）编码，内联执行，完全 0 堆分配、0 反射
	binary.BigEndian.PutUint64(buf[0:8], uint64(playerID))
	binary.BigEndian.PutUint32(buf[8:12], math.Float32bits(x))
	binary.BigEndian.PutUint32(buf[12:16], math.Float32bits(y))
	binary.BigEndian.PutUint32(buf[16:20], math.Float32bits(z))

	// 取出包裹对象并设定目标引用数
	pkt := refPool.Get().(*RefPacket)
	pkt.Data = bufPtr
	atomic.StoreInt32(&pkt.refCount, targets)
	return pkt
}

// Release 释放引用计数，当引用归零时自动将底层资源归还给对应的 sync.Pool
func (p *RefPacket) Release() {
	// 使用原子递减操作，保障多写协程并发访问安全
	if atomic.AddInt32(&p.refCount, -1) == 0 {
		// 最后一个消费者使用完毕，安全将底层字节切片归还缓冲池
		bufPool.Put(p.Data)
		p.Data = nil
		// 将 RefPacket 包装器归还包裹池
		refPool.Put(p)
	}
}
