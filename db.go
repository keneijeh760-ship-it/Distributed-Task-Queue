package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func connectDB() (*pgxpool.Pool, error) {
	conn, err := pgxpool.New(context.Background(), "postgres://taskqueue:taskqueue@localhost:5444/taskqueue")
	return conn, err
}
