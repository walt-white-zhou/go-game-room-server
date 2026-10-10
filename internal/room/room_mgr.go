// Package room 房间管理器
package room

import "sync"

// Manager 全局房间生命周期管理器
type Manager struct {
	mu    sync.RWMutex
	rooms map[int64]*Room // 房间映射表：RoomID -> *Room
}

// RoomMgr 全局单例
var RoomMgr = &Manager{
	rooms: make(map[int64]*Room),
}

// GetOrCreate 获取指定房间实例；若不存在则自动初始化一个全新的 Actor 房间
func (m *Manager) GetOrCreate(roomID int64) *Room {
	// 先尝试走读锁，提高已有房间查询的高并发吞吐
	m.mu.RLock()
	if r, ok := m.rooms[roomID]; ok {
		m.mu.RUnlock()
		return r
	}
	m.mu.RUnlock()

	// 不存在时获取写锁并创建
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-Check 二次检查，避免并发重入创建
	if r, ok := m.rooms[roomID]; ok {
		return r
	}

	r := NewRoom(roomID)
	m.rooms[roomID] = r
	return r
}
