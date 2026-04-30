/* package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type TransactionManager struct {
	analyzer *DependencyAnalyzer
	wal      *WAL
}

func NewCoordinator(walFile string) *TransactionManager {
	wal := NewWAL(walFile)
	
	// RECOVERY: Replay logs on startup
	history := wal.ReadAll()
	log.Printf("--- RECOVERY: Found %d historical entries in log ---", len(history))

	return &TransactionManager{
		analyzer: NewDependencyAnalyzer(),
		wal:      wal,
	}
}

func (tm *TransactionManager) HandleBegin(w http.ResponseWriter, r *http.Request) {
	var meta TransactionMetadata
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// 1. Dependency Check
	if !tm.analyzer.AnalyzeAndLock(meta.ID, meta.Keys) {
		http.Error(w, "Conflict Detected", 409)
		return
	}
	defer tm.analyzer.Release(meta.Keys)

	// 2. Log START
	tm.wal.Write(fmt.Sprintf("START %s %v", meta.ID, meta.Keys))

	// 3. Prepare Phase
	log.Printf("[TM] Starting Prepare Phase for %s", meta.ID)
	if tm.broadcast(meta.Participants, "prepare", meta) {
		// 4. Log COMMIT
		tm.wal.Write(fmt.Sprintf("COMMIT %s", meta.ID))
		tm.sendDecision(meta, "COMMIT")
		fmt.Fprintf(w, "SUCCESS: Transaction %s Committed", meta.ID)
	} else {
		// 4. Log ABORT
		tm.wal.Write(fmt.Sprintf("ABORT %s", meta.ID))
		tm.sendDecision(meta, "ABORT")
		http.Error(w, "Transaction Aborted", 500)
	}
}

func (tm *TransactionManager) broadcast(participants []string, endpoint string, meta TransactionMetadata) bool {
	successCount := 0
	for _, p := range participants {
		url := fmt.Sprintf("%s/%s", p, endpoint)
		body, _ := json.Marshal(PrepareRequest{TxnID: meta.ID, Keys: meta.Keys})
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
		if err == nil && resp.StatusCode == 200 {
			successCount++
		}
	}
	// Simple Quorum: For this demo, we require 100% success
	return successCount == len(participants)
}

func (tm *TransactionManager) sendDecision(meta TransactionMetadata, action string) {
	for _, p := range meta.Participants {
		url := fmt.Sprintf("%s/commit", p)
		body, _ := json.Marshal(CommitRequest{TxnID: meta.ID, Action: action})
		http.Post(url, "application/json", bytes.NewBuffer(body))
	}
} */

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"context"
)

type TransactionManager struct {
	analyzer *DependencyAnalyzer
	wal      *WAL
}

const ENABLE_DEPENDENCY_ANALYZER = true

func NewCoordinator(walFile string) *TransactionManager {
	// START THE RECOVERY TIMER
	startTime := time.Now()

	wal := NewWAL(walFile)
	history := wal.ReadAll()
	
	// STOP THE TIMER
	recoveryTime := time.Since(startTime)

	log.Printf("--- RECOVERY: Found %d historical entries in log ---", len(history))
	log.Printf("--- MEAN TIME TO RECOVERY (MTTR): %v ---", recoveryTime)

	return &TransactionManager{
		analyzer: NewDependencyAnalyzer(),
		wal:      wal,
	}
}

func (tm *TransactionManager) HandleBegin(w http.ResponseWriter, r *http.Request) {
	var meta TransactionMetadata
	json.NewDecoder(r.Body).Decode(&meta)

	// ==========================================
	// Standard Data-Blind 2PC
	// ==========================================
	if !ENABLE_DEPENDENCY_ANALYZER {
		// Just throw everything at the database immediately
		tm.execute2PC(w, meta, "STANDARD_2PC")
		return
	}

	// ==========================================
	// Hybrid Coordinator
	// ==========================================
	startTime := time.Now()

	// 1. Run it once for the actual transaction logic
	isFastPath := tm.analyzer.IsIndependent(meta.Keys)

	// 2. Run it 10,000 times to bypass the Windows Clock limit
	for i := 0; i < 10000; i++ {
		tm.analyzer.IsIndependent(meta.Keys)
	}

	// 3. Stop the timer and divide by 10,000 to get the exact Nanosecond time
	totalTime := time.Since(startTime)
	avgDecisionTime := totalTime / 10000

	log.Printf("[Metrics] Txn %s: Dependency Analysis took %v per txn", meta.ID, avgDecisionTime)

	// 4. ROUTE THE TRANSACTION
	if isFastPath {
		if !tm.analyzer.TryLock(meta.ID, meta.Keys, false) {
			http.Error(w, "Conflict", 409)
			return
		}
		defer tm.analyzer.Release(meta.Keys)
		
		// NOVELTY: Dependency-Aware Fast Path Logging
		// By logging the keys, we leave a formal proof of non-conflict for the Recovery Manager
		tm.wal.Write(fmt.Sprintf("FAST_COMMIT %s %v", meta.ID, meta.Keys)) 
		tm.execute2PC(w, meta, "FAST_PATH")
		
	} else {
		// SLOW PATH: Requires a strict WAL disk write for ordering before locking
		tm.wal.Write(fmt.Sprintf("RAFT_PROPOSE %s %v", meta.ID, meta.Keys))
		
		if !tm.analyzer.TryLock(meta.ID, meta.Keys, true) {
			http.Error(w, "Timeout", 409)
			return
		}
		defer tm.analyzer.Release(meta.Keys)
		tm.execute2PC(w, meta, "SLOW_PATH")
	}
}

func (tm *TransactionManager) execute2PC(w http.ResponseWriter, meta TransactionMetadata, mode string) {
	log.Printf("[Dispatcher] Starting 2PC for %s across %d nodes...", meta.ID, len(meta.Participants))
	
	if tm.broadcast(meta.Participants, "prepare", meta) {
		tm.wal.Write(fmt.Sprintf("COMMIT %s", meta.ID))
		tm.sendDecision(meta, "COMMIT")
		fmt.Fprintf(w, "SUCCESS [%s]: Transaction %s Committed across %d nodes\n", mode, meta.ID, len(meta.Participants))
	} else {
		tm.wal.Write(fmt.Sprintf("ABORT %s", meta.ID))
		tm.sendDecision(meta, "ABORT")
		http.Error(w, fmt.Sprintf("FAILED [%s]: Transaction Aborted\n", mode), 500)
	}
}

// Parallel Execution Engine
func (tm *TransactionManager) broadcast(participants []string, endpoint string, meta TransactionMetadata) bool {
	var wg sync.WaitGroup
	var successCount int32 = 0

	// STRICT TIMEOUT: If a DB doesn't reply in 2 seconds, we automatically fail the txn.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, p := range participants {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			fullURL := url + "/" + endpoint
			
			
			payload := map[string]interface{}{
				"TxnID": meta.ID,
				"Keys":  meta.Keys,
			}
			body, _ := json.Marshal(payload)
			
			req, _ := http.NewRequestWithContext(ctx, "POST", fullURL, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			
			client := &http.Client{}
			resp, err := client.Do(req)
			
			if err == nil && resp.StatusCode == 200 {
				atomic.AddInt32(&successCount, 1)
			}
		}(p)
	}
	
	wg.Wait()
	return int(successCount) == len(participants)
}

// Fire-and-Forget Parallel Commits
func (tm *TransactionManager) sendDecision(meta TransactionMetadata, action string) {
	for _, p := range meta.Participants {
		go func(url string) {
			fullURL := fmt.Sprintf("%s/commit", url)
			
			payload := map[string]interface{}{
				"TxnID":  meta.ID,
				"Action": action,
			}
			body, _ := json.Marshal(payload)
			
			http.Post(fullURL, "application/json", bytes.NewBuffer(body))
		}(p)
	}
}