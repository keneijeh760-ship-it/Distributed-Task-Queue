package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const databaseURL = "postgres://taskqueue:taskqueue@localhost:5444/taskqueue"

func connectDB() (*pgxpool.Pool, error) {
	conn, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, err
	}
	if err := ensureSchema(context.Background(), conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			idempotency_key TEXT UNIQUE,
			payload TEXT NOT NULL,
			status TEXT NOT NULL,
			attempts INT NOT NULL DEFAULT 0,
			max_attempts INT NOT NULL DEFAULT 5,
			visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			lease_expires_at TIMESTAMPTZ,
			last_error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS idempotency_key TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 5`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
	}
	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}
