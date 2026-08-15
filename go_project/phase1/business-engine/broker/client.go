package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient wraps the Redis client instance
type RedisClient struct {
	Client *redis.Client
}

// NewRedisClient initializes and verifies the connection to the Redis broker container
func NewRedisClient(addr string) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
	Addr:         addr,
		Password:     "", // no password set in docker-compose by default
		DB:           0,  // default DB
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Perform a ping handshake to ensure Redis is reachable
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis broker at %s: %w", addr, err)
	}

	log.Println("Successfully connected to Redis message broker!")
	return &RedisClient{Client: rdb}, nil
}