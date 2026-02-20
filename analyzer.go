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
	"sync"
	"time"
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

// IsIndependent checks if keys are free BEFORE locking.
func (da *DependencyAnalyzer) IsIndependent(keys []string) bool {
	da.mu.Lock()
	defer da.mu.Unlock()

	for _, key := range keys {
		if _, exists := da.activeLocks[key]; exists {
			return false // Conflict detected
		}
	}
	return true // No conflicts!
}

// TryLock attempts to lock the keys. If wait=true (Slow Path), it queues up.
func (da *DependencyAnalyzer) TryLock(txnID string, keys []string, wait bool) bool {
	retries := 0
	for {
		da.mu.Lock()
		conflict := false
		for _, key := range keys {
			if _, exists := da.activeLocks[key]; exists {
				conflict = true
				break
			}
		}

		if !conflict {
			// Success: Acquire locks
			for _, key := range keys {
				da.activeLocks[key] = txnID
			}
			da.mu.Unlock()
			return true
		}
		da.mu.Unlock()

		// If it's Fast Path, we don't wait. If it's Slow Path, we wait in queue.
		if !wait || retries > 10 {
			return false
		}
		retries++
		time.Sleep(1 * time.Second) // Wait in queue
	}
}

func (da *DependencyAnalyzer) Release(keys []string) {
	da.mu.Lock()
	defer da.mu.Unlock()
	for _, key := range keys {
		delete(da.activeLocks, key)
	}
}