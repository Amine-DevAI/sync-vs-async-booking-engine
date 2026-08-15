package main

import (
	"log"
	"net/http"

	"business-engine/api"
	"business-engine/broker" // 1. Import your broker package
	"business-engine/db"
)

func main() {
	// 1. Connect to PostgreSQL
	if err := db.Connect(); err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	// 2. Connect to Redis Message Broker
	redisBroker, err := broker.NewRedisClient("localhost:6379")
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	// Note: go-redis manages its own connection pool, so an explicit close isn't strictly necessary on exit, 
	// but keeping the reference handy is essential for your handlers.

	// 3. Initialize API Router (pass the redis broker so your handlers can enqueue tasks)
	router := api.NewRouter(redisBroker)

	// 4. Start HTTP Server
	log.Println("Server running on port 8080...")
	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}