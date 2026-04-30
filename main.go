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
	"math/rand" // Required for Zipfian distribution
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"io"
	"strings"
	"os"
)

func main() {
	// 1. Define all the flags here BEFORE flag.Parse()
	mode := flag.String("mode", "coordinator", "Modes: coordinator | participant | cluster | loadtest")
	port := flag.String("port", "8082", "Port to listen on")
	testType := flag.String("type", "high", "For loadtest: high | low | mixed | zipf")
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
		log.Printf("Starting %s contention load test...", *testType)

		var wg sync.WaitGroup
		var successes, failures int32
		
		// Safe memory structures to record latencies
		var mu sync.Mutex
		fastLatencies := make([]time.Duration, 0)
		slowLatencies := make([]time.Duration, 0)
		
		startTime := time.Now()

		// ==========================================
		// TEST A: REALISTIC WORKLOAD (Zipfian Skew)
		// ==========================================
		if *testType == "zipf" {
			log.Println("Generating highly skewed 80/20 distribution for 200 txns...")
			zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.1, 1.0, 100)

			for i := 0; i < 400; i++ {
				wg.Add(1)
				go func(reqID int) {
					defer wg.Done()

					key1 := fmt.Sprintf("item-%d", zipf.Uint64())
					key2 := fmt.Sprintf("item-%d", zipf.Uint64())

					reqBody, _ := json.Marshal(TransactionMetadata{
						ID:           fmt.Sprintf("txn-%d", reqID),
						Keys:         []string{key1, key2},
						Participants: []string{"http://localhost:8081"},
					})

					reqStart := time.Now()
					resp, err := http.Post("http://localhost:8082/txn", "application/json", bytes.NewBuffer(reqBody))
					latency := time.Since(reqStart)

					if err != nil {
						atomic.AddInt32(&failures, 1)
						return
					}
					defer resp.Body.Close()

					bodyBytes, _ := io.ReadAll(resp.Body)
					responseString := string(bodyBytes)

					if resp.StatusCode == 200 {
						atomic.AddInt32(&successes, 1)
						mu.Lock()
						if strings.Contains(responseString, "FAST_PATH") {
							fastLatencies = append(fastLatencies, latency)
						} else if strings.Contains(responseString, "SLOW_PATH") {
							slowLatencies = append(slowLatencies, latency)
						}
						mu.Unlock()
					} else {
						atomic.AddInt32(&failures, 1)
					}
				}(i)
			}
		} else {
			// ==========================================
			// TEST B: STANDARD TESTS (Low, High, Mixed)
			// ==========================================
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

					reqStart := time.Now()
					resp, err := http.Post("http://localhost:8082/txn", "application/json", bytes.NewBuffer(reqBody))
					latency := time.Since(reqStart)

					if err != nil {
						atomic.AddInt32(&failures, 1)
						return
					}
					defer resp.Body.Close()

					bodyBytes, _ := io.ReadAll(resp.Body)
					responseString := string(bodyBytes)

					if resp.StatusCode == 200 {
						atomic.AddInt32(&successes, 1)
						mu.Lock()
						if strings.Contains(responseString, "FAST_PATH") {
							fastLatencies = append(fastLatencies, latency)
						} else if strings.Contains(responseString, "SLOW_PATH") {
							slowLatencies = append(slowLatencies, latency)
						}
						mu.Unlock()
					} else {
						atomic.AddInt32(&failures, 1)
					}
				}(i)
			}
		}

		// ==========================================
		// FINAL WAIT AND PRINT (Executes for ALL tests)
		// ==========================================
		wg.Wait()
		duration := time.Since(startTime)

		fmt.Printf("\n========================================\n")
		fmt.Printf("          LOAD TEST RESULTS             \n")
		fmt.Printf("========================================\n")
		fmt.Printf("Mode:             %s Contention\n", *testType)
		fmt.Printf("Total Time:       %v\n", duration)
		fmt.Printf("Successful Txns:  %d\n", successes)
		fmt.Printf("Failed Txns:      %d\n", failures)
		
		if len(fastLatencies) > 0 {
			var total time.Duration
			for _, l := range fastLatencies {
				total += l
			}
			fmt.Printf("Fast Path Avg:    %v (Leaderless Bypass)\n", total/time.Duration(len(fastLatencies)))
		}
		
		if len(slowLatencies) > 0 {
			var total time.Duration
			for _, l := range slowLatencies {
				total += l
			}
			fmt.Printf("Slow Path Avg:    %v (Consensus Queued)\n", total/time.Duration(len(slowLatencies)))
		}
		fmt.Printf("========================================\n\n")
		fileName := fmt.Sprintf("results_%s.csv", *testType)
		file, err := os.Create(fileName)
		if err == nil {
			defer file.Close()
			file.WriteString("Path,LatencyMS\n") // CSV Header

			// Write Fast Path Latencies
			for _, l := range fastLatencies {
				file.WriteString(fmt.Sprintf("FAST,%d\n", l.Milliseconds()))
			}
			
			// Write Slow Path Latencies
			for _, l := range slowLatencies {
				file.WriteString(fmt.Sprintf("SLOW,%d\n", l.Milliseconds()))
			}
			fmt.Printf("Data successfully exported to %s for graphing!\n", fileName)
		}
	}
}  