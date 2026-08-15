package connection

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ConnectClickHouse initializes a connection pool to ClickHouse and verifies it with a ping
func ConnectClickHouse(dsn string) (*sql.DB, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping handshake failed: %w", err)
	}

	return db, nil
}