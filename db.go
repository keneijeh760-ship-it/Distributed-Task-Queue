package main

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func connectDB() (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), "postgres://taskqueue:taskqueue@localhost:5444/taskqueue")
	return conn, err
}
