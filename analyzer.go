package main

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
}