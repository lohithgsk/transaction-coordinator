package main

import (
	"bufio"
	"log"
	"os"
	"sync"
)

type WAL struct {
	filename string
	mu       sync.Mutex
	file     *os.File
}

func NewWAL(filename string) *WAL {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return &WAL{filename: filename, file: f}
}

// Write: Persist a decision to disk
func (w *WAL) Write(entry string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if _, err := w.file.WriteString(entry + "\n"); err != nil {
		log.Printf("CRITICAL: Failed to write to WAL: %v", err)
	}
	// Sync ensures data is physically on the disk (Durability)
	w.file.Sync() 
}

// ReadAll: Used on startup to recover state
func (w *WAL) ReadAll() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []string
	file, _ := os.Open(w.filename)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		entries = append(entries, scanner.Text())
	}
	return entries
}