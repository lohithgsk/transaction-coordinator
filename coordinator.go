package main

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
}