// Package workers implements a bounded-concurrency background task processor.
//
// Architecture:
//   - A single dispatcher goroutine performs blocking pops (BRPop) against a Redis
//     list and fans raw payloads out into an internal channel.
//   - A fixed-size pool of worker goroutines consumes from that channel. This keeps
//     concurrency bounded (no goroutine-per-task explosion, no OS process spawning)
//     while still processing tasks in parallel.
//   - Every task execution is wrapped in a hard 5-second context.WithTimeout. If the
//     handler doesn't return before the deadline, the task is treated as TIMEOUT and
//     audited accordingly; the handler goroutine is abandoned (Go has no forced
//     preemption of goroutines) but is expected to cooperatively respect ctx.Done().
//   - Every outcome (SUCCESS / FAILED / TIMEOUT) is written to ClickHouse's
//     default.audit_logs table using a context independent from the task's context,
//     since the task context may already be cancelled/expired by the time we log.
package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"worker-service/connection"
)

const (
	// taskTimeout is the strict watchdog deadline for a single task execution.
	taskTimeout = 5 * time.Second

	// brPopBlockTimeout controls how long each BRPop call blocks before returning
	// redis.Nil, giving the dispatcher loop a chance to observe context cancellation
	// for graceful shutdown. Must be > 0 for go-redis BRPop.
	brPopBlockTimeout = 5 * time.Second

	// auditLogTimeout bounds how long a ClickHouse audit insert may take.
	auditLogTimeout = 3 * time.Second

	// dispatchChannelBuffer bounds how many pending payloads can sit between the
	// dispatcher and the worker pool before the dispatcher blocks on send (natural
	// backpressure into Redis itself).
	dispatchChannelBuffer = 64
)

// Audit event outcomes written to ClickHouse.
const (
	EventSuccess = "SUCCESS"
	EventFailed  = "FAILED"
	EventTimeout = "TIMEOUT"
)

// Task represents a unit of work popped off the Redis queue. Adjust the payload
// shape to whatever your producers actually push; Payload is left raw so handlers
// can decode their own domain-specific structure.
type Task struct {
	EntityType string          `json:"entity_type"`
	EntityID   uint64          `json:"entity_id"`
	Event      string          `json:"event"`
	Payload    json.RawMessage `json:"payload"`
}

// Handler is the business logic contract a caller supplies. Implementations SHOULD
// be context-aware (select on ctx.Done() during long-running work) so that a
// watchdog timeout can actually stop wasted work, not just stop waiting for it.
type Handler func(ctx context.Context, task Task) error

// Worker pulls tasks from a Redis list, executes them via Handler under a strict
// timeout, and audits every outcome to ClickHouse.
type Worker struct {
	redisClient *redis.Client
	chDB        *sql.DB

	queueKey    string
	poolSize    int
	handler     Handler

	taskCh chan string
	wg     sync.WaitGroup
}

// Config controls Worker construction.
type Config struct {
	QueueKey      string  // Redis list key to BRPOP from
	PoolSize      int     // number of concurrent worker goroutines (e.g. runtime.NumCPU()*2)
	Handler       Handler // business logic invoked per task
	RedisAddr     string  // e.g. "localhost:6379"
	ClickHouseDSN string  // e.g. "clickhouse://default:@localhost:9000/default"
}

// NewWorker wires up Redis and ClickHouse connections via the existing connection
// package and returns a ready-to-Start Worker. Both connections are verified with
// a ping handshake at construction time, so a misconfigured broker or sink fails
// fast here rather than surfacing later as a mysterious dispatch/audit error.
func NewWorker(cfg Config) (*Worker, error) {
	if cfg.QueueKey == "" {
		return nil, errors.New("workers: QueueKey must not be empty")
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 8
	}
	if cfg.Handler == nil {
		return nil, errors.New("workers: Handler must not be nil")
	}
	if cfg.RedisAddr == "" {
		return nil, errors.New("workers: RedisAddr must not be empty")
	}
	if cfg.ClickHouseDSN == "" {
		return nil, errors.New("workers: ClickHouseDSN must not be empty")
	}

	redisClient, err := connection.ConnectRedis(cfg.RedisAddr)
	if err != nil {
		return nil, fmt.Errorf("workers: redis connection failed: %w", err)
	}

	chDB, err := connection.ConnectClickHouse(cfg.ClickHouseDSN)
	if err != nil {
		return nil, fmt.Errorf("workers: clickhouse connection failed: %w", err)
	}

	return &Worker{
		redisClient: redisClient,
		chDB:        chDB,
		queueKey:    cfg.QueueKey,
		poolSize:    cfg.PoolSize,
		handler:     cfg.Handler,
		taskCh:      make(chan string, dispatchChannelBuffer),
	}, nil
}

// Start blocks and runs the dispatcher + worker pool until ctx is cancelled. It
// performs a graceful shutdown: the dispatcher stops popping new tasks, the task
// channel is closed, and Start waits for all in-flight workers to finish (bounded
// by their own 5s watchdog) before returning.
func (w *Worker) Start(ctx context.Context) {
	log.Printf("[workers] starting: queue=%q pool_size=%d timeout=%s", w.queueKey, w.poolSize, taskTimeout)

	// Launch the fixed worker pool.
	w.wg.Add(w.poolSize)
	for i := 0; i < w.poolSize; i++ {
		go func(id int) {
			defer w.wg.Done()
			w.runWorkerLoop(id)
		}(i)
	}

	// Dispatcher loop (runs on the calling goroutine).
	w.runDispatchLoop(ctx)

	// Dispatcher has stopped popping; close the channel so workers drain and exit.
	close(w.taskCh)
	w.wg.Wait()

	log.Printf("[workers] shutdown complete: queue=%q", w.queueKey)
}

// runDispatchLoop performs blocking pops against Redis and feeds raw payloads to
// the worker pool. It never spawns goroutines itself, keeping fan-in bounded.
func (w *Worker) runDispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[workers] dispatch loop stopping: %v", ctx.Err())
			return
		default:
		}

		// BRPop blocks up to brPopBlockTimeout, then returns redis.Nil on timeout,
		// which lets us re-check ctx.Done() periodically instead of blocking forever.
		result, err := w.redisClient.BRPop(ctx, brPopBlockTimeout, w.queueKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // no task within the block window; loop and re-check ctx
			}
			if ctx.Err() != nil {
				return // shutting down
			}
			// Transient Redis error (network blip, failover, etc). Back off briefly
			// rather than hot-looping and hammering a struggling broker.
			log.Printf("[workers] redis BRPop error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// result[0] == key name, result[1] == popped value
		if len(result) != 2 {
			continue
		}
		payload := result[1]

		select {
		case w.taskCh <- payload:
		case <-ctx.Done():
			return
		}
	}
}

// runWorkerLoop is executed by each pool goroutine; it consumes payloads and
// processes them one at a time until taskCh is closed.
func (w *Worker) runWorkerLoop(id int) {
	for payload := range w.taskCh {
		w.processPayload(payload)
	}
	log.Printf("[workers] worker #%d exiting", id)
}

// processPayload decodes the raw Redis payload into a Task and executes it under
// the watchdog. Decode failures are audited as FAILED so bad messages are never
// silently dropped.
func (w *Worker) processPayload(payload string) {
	var task Task
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		log.Printf("[workers] malformed task payload: %v", err)
		w.logAudit("unknown", 0, EventFailed, fmt.Sprintf("payload decode error: %v", err))
		return
	}
	w.executeWithWatchdog(task)
}

// executeWithWatchdog runs the handler under a strict 5-second deadline and audits
// the outcome. The handler runs in its own goroutine so that a timeout can be
// observed immediately even if the handler is still executing; the handler
// goroutine is left to finish on its own (Go cannot forcibly kill it) but its
// result is discarded once the deadline has already been reported as TIMEOUT.
func (w *Worker) executeWithWatchdog(task Task) {
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()

	done := make(chan error, 1) // buffered so a late handler write never leaks the goroutine

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic in task handler: %v", r)
			}
		}()
		done <- w.handler(ctx, task)
	}()

	select {
	case err := <-done:
		if err != nil {
			w.logAudit(task.EntityType, task.EntityID, EventFailed, err.Error())
			return
		}
		w.logAudit(task.EntityType, task.EntityID, EventSuccess, "")

	case <-ctx.Done():
		// Deadline exceeded (or, in principle, some other cancellation cause).
		w.logAudit(task.EntityType, task.EntityID, EventTimeout,
			fmt.Sprintf("task exceeded %s watchdog limit", taskTimeout))
	}
}

// logAudit writes a single audit row to ClickHouse's default.audit_logs table.
// It intentionally uses a fresh, independent context rather than the task's
// context, since the task context may already be cancelled/expired (that's often
// exactly why we're logging).
func (w *Worker) logAudit(entityType string, entityID uint64, event, details string) {
	ctx, cancel := context.WithTimeout(context.Background(), auditLogTimeout)
	defer cancel()

	const stmt = `
		INSERT INTO default.audit_logs (timestamp, entity_type, entity_id, event, details)
		VALUES (?, ?, ?, ?, ?)
	`

	if _, err := w.chDB.ExecContext(ctx, stmt, time.Now().UTC(), entityType, entityID, event, details); err != nil {
		// Audit logging must never crash the worker; log locally and move on.
		log.Printf("[workers] CRITICAL: failed to write audit log (event=%s entity=%s/%d): %v",
			event, entityType, entityID, err)
	}
}