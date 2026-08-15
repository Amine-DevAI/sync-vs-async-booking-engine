package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "worker-service/workers"
)

const (
    auditQueueKey  = "background_jobs"
    workerPoolSize = 10
)

func main() {
    rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
    clickhouseDSN := getEnv("CLICKHOUSE_DSN", "clickhouse://default:@localhost:9000/default")

    worker, err := workers.NewWorker(workers.Config{
        QueueKey:      auditQueueKey,
        PoolSize:      workerPoolSize,
        Handler:       realTaskHandler,
        RedisAddr:     redisAddr,
        ClickHouseDSN: clickhouseDSN,
    })
    if err != nil {
        log.Fatalf("[main] failed to initialize worker: %v", err)
    }

    log.Printf("[main] worker service listening on queue %q (pool_size=%d). Press Ctrl+C to stop.", auditQueueKey, workerPoolSize)

    worker.Start(rootCtx)

    log.Println("[main] clean shutdown complete")
}

func getEnv(key, fallback string) string {
    if v, ok := os.LookupEnv(key); ok && v != "" {
        return v
    }
    return fallback
}

// realTaskHandler is your clean business logic endpoint. 
// It returns an error only if a real failure occurs during actual task execution.
// realTaskHandler processes tasks instantly without any artificial delay.
func realTaskHandler(ctx context.Context, task workers.Task) error {
    // Check if context is already cancelled before executing
    if err := ctx.Err(); err != nil {
        return err
    }

    log.Printf("[worker] successfully processed task: type=%s id=%d", task.EntityType, task.EntityID)
    return nil
}