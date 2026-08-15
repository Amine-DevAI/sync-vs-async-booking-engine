package connection

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConnectRedis initializes the Redis client and verifies it with a ping handshake
func ConnectRedis(addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // No password by default for local docker
		DB:       0,  // Default DB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Verify connection with PING
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping handshake failed: %w", err)
	}

	return client, nil
}