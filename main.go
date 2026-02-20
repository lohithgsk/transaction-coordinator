/* package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	mode := flag.String("mode", "coordinator", "Mode: coordinator | participant")
	port := flag.String("port", "8080", "Port to listen on")
	flag.Parse()

	if *mode == "coordinator" {
		log.Printf("Starting COORDINATOR on :%s", *port)
		tm := NewCoordinator("coordinator.log")
		http.HandleFunc("/txn", tm.HandleBegin)
		log.Fatal(http.ListenAndServe(":"+*port, nil))

	} else {
		log.Printf("Starting PARTICIPANT on :%s", *port)
		p := &Participant{Port: *port, Locks: make(map[string]string)}
		http.HandleFunc("/prepare", p.HandlePrepare)
		http.HandleFunc("/commit", p.HandleCommit)
		log.Fatal(http.ListenAndServe(":"+*port, nil))
	}
} */
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	// 1. Define all the flags here BEFORE flag.Parse()
	mode := flag.String("mode", "coordinator", "Modes: coordinator | participant | cluster | loadtest")
	port := flag.String("port", "8082", "Port to listen on")
	testType := flag.String("type", "high", "For loadtest: high | low")
	flag.Parse()

	// 2. Mode Routings
	if *mode == "coordinator" {
		log.Printf("Starting COORDINATOR on :%s", *port)
		tm := NewCoordinator("coordinator.log")
		http.HandleFunc("/txn", tm.HandleBegin)
		log.Fatal(http.ListenAndServe(":"+*port, nil))

	} else if *mode == "participant" {
		log.Printf("Starting PARTICIPANT on :%s", *port)
		p := &Participant{Port: *port, Locks: make(map[string]string)}
		http.HandleFunc("/prepare", p.HandlePrepare)
		http.HandleFunc("/commit", p.HandleCommit)
		log.Fatal(http.ListenAndServe(":"+*port, nil))

	} else if *mode == "cluster" {
		log.Println("Starting 100 PARTICIPANTS (Ports 8081 to 8180)...")
		for i := 0; i < 100; i++ {
			pPort := fmt.Sprintf("%d", 8081+i)
			p := &Participant{Port: pPort, Locks: make(map[string]string)}
			
			mux := http.NewServeMux()
			mux.HandleFunc("/prepare", p.HandlePrepare)
			mux.HandleFunc("/commit", p.HandleCommit)
			
			go http.ListenAndServe(":"+pPort, mux)
		}
		log.Println("Cluster of 100 Databases is running! Press Ctrl+C to stop.")
		select {} 

	} else if *mode == "loadtest" {
		log.Printf("Starting %s contention load test with 50 concurrent requests...", *testType)

		var wg sync.WaitGroup
		var successes, failures int32
		startTime := time.Now()

		// Replace the loadtest loop inside main.go with this:
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(reqID int) {
			defer wg.Done()

			var key string
			if *testType == "low" {
				key = fmt.Sprintf("user-%d", reqID)
			} else if *testType == "high" {
				key = "HOT_KEY"
			} else if *testType == "mixed" {
				// 80% independent (Fast Path), 20% conflicting (Slow Path)
				if reqID < 40 {
					key = fmt.Sprintf("user-%d", reqID)
				} else {
					key = "HOT_KEY"
				}
			}

			reqBody, _ := json.Marshal(TransactionMetadata{
				ID:           fmt.Sprintf("txn-%d", reqID),
				Keys:         []string{key},
				Participants: []string{"http://localhost:8081"},
			})

			resp, err := http.Post("http://localhost:8082/txn", "application/json", bytes.NewBuffer(reqBody))
			if err != nil {
				atomic.AddInt32(&failures, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == 200 {
				atomic.AddInt32(&successes, 1)
			} else {
				atomic.AddInt32(&failures, 1)
			}
		}(i)
	}

		wg.Wait() // Wait for all 50 requests to finish
		duration := time.Since(startTime)
		
		fmt.Printf("\n========================================\n")
		fmt.Printf("          LOAD TEST RESULTS             \n")
		fmt.Printf("========================================\n")
		fmt.Printf("Mode:             %s Contention\n", *testType)
		fmt.Printf("Total Time:       %v\n", duration)
		fmt.Printf("Successful Txns:  %d\n", successes)
		fmt.Printf("Failed Txns:      %d\n", failures)
		fmt.Printf("========================================\n\n")
	}
}