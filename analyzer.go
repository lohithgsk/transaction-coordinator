/* package main

import (
	"log"
	"sync"
)

type DependencyAnalyzer struct {
	activeLocks map[string]string // Key -> TxnID
	mu          sync.Mutex
}

func NewDependencyAnalyzer() *DependencyAnalyzer {
	return &DependencyAnalyzer{
		activeLocks: make(map[string]string),
	}
}

func (da *DependencyAnalyzer) AnalyzeAndLock(txnID string, keys []string) bool {
	da.mu.Lock()
	defer da.mu.Unlock()

	// 1. Conflict Detection
	for _, key := range keys {
		if owner, exists := da.activeLocks[key]; exists {
			log.Printf("[Analyzer] CONFLICT: Key '%s' is locked by %s", key, owner)
			return false
		}
	}

	// 2. Lock Acquisition (Fast Path)
	for _, key := range keys {
		da.activeLocks[key] = txnID
	}
	log.Printf("[Analyzer] Locks acquired for %s: %v", txnID, keys)
	return true
}

func (da *DependencyAnalyzer) Release(keys []string) {
	da.mu.Lock()
	defer da.mu.Unlock()
	for _, key := range keys {
		delete(da.activeLocks, key)
	}
} */

package main

import (
	"hash/fnv"
	"sync"
	"time"
)

const NUM_SHARDS = 256

type LockShard struct {
	mu          sync.RWMutex
	activeLocks map[string]string
}

// Struct name remains the same so coordinator.go doesn't break
type DependencyAnalyzer struct {
	shards [NUM_SHARDS]*LockShard
}

func NewDependencyAnalyzer() *DependencyAnalyzer {
	da := &DependencyAnalyzer{}
	for i := 0; i < NUM_SHARDS; i++ {
		da.shards[i] = &LockShard{activeLocks: make(map[string]string)}
	}
	return da
}

// O(1) Shard Resolution
func getShardIndex(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % NUM_SHARDS
}

func (da *DependencyAnalyzer) IsIndependent(keys []string) bool {
	for _, key := range keys {
		shard := da.shards[getShardIndex(key)]
		shard.mu.RLock()
		_, exists := shard.activeLocks[key]
		shard.mu.RUnlock()
		if exists {
			return false // Conflict found
		}
	}
	return true
}

// TryLock implements targeted sharded locking and handles the wait boolean
func (da *DependencyAnalyzer) TryLock(txnID string, keys []string, wait bool) bool {
	timeout := time.After(30 * time.Second)
	for {
		if da.IsIndependent(keys) {
			for _, key := range keys {
				shard := da.shards[getShardIndex(key)]
				shard.mu.Lock()
				shard.activeLocks[key] = txnID
				shard.mu.Unlock()
			}
			return true
		}

		if !wait {
			return false // Fast Path drops immediately
		}

		select {
		case <-timeout:
			return false // Slow Path queues for 10s max
		case <-time.After(50 * time.Millisecond): // Poll
		}
	}
}

func (da *DependencyAnalyzer) Release(keys []string) {
	for _, key := range keys {
		shard := da.shards[getShardIndex(key)]
		shard.mu.Lock()
		delete(shard.activeLocks, key)
		shard.mu.Unlock()
	}
}