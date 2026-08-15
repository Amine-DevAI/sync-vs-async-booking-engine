package main

import (
	"log"
	"net/http"

	"business-engine/api"
	"business-engine/db"
)

func main() {
	// 1. Connect to PostgreSQL
	if err := db.Connect(); err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	// 2. Initialize API Router
	// Phase 1 baseline: no message broker, no background jobs — every
	// handler does its DB work and responds immediately, nothing else.
	router := api.NewRouter()

	// 3. Start HTTP Server
	log.Println("Server running on port 8080...")
	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
