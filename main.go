package main

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
}