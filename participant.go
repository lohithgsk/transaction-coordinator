package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type Participant struct {
	Port  string
	Locks map[string]string
	mu    sync.Mutex
}

func (p *Participant) HandlePrepare(w http.ResponseWriter, r *http.Request) {
	var req PrepareRequest
	json.NewDecoder(r.Body).Decode(&req)
	
	p.mu.Lock()
	defer p.mu.Unlock()

	log.Printf("[Participant] simulating slow database work...")
	time.Sleep(3 * time.Second) 

	// Check local locks
	for _, k := range req.Keys {
		if _, exists := p.Locks[k]; exists {
			http.Error(w, "Locked", 409)
			return
		}
	}
	// Lock
	for _, k := range req.Keys {
		p.Locks[k] = req.TxnID
	}
	log.Printf("[Participant] Locked keys %v for %s", req.Keys, req.TxnID)
	w.WriteHeader(200)
}

func (p *Participant) HandleCommit(w http.ResponseWriter, r *http.Request) {
	var req CommitRequest
	json.NewDecoder(r.Body).Decode(&req)

	p.mu.Lock()
	defer p.mu.Unlock()

	log.Printf("[Participant] Finished %s: %s", req.TxnID, req.Action)

	// Release Locks
	for k, v := range p.Locks {
		if v == req.TxnID {
			delete(p.Locks, k)
		}
	}
}