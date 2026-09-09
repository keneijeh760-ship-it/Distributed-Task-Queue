package main

import (
	"errors"

	"context"

	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusInProgress TaskStatus = "in_progress"
	StatusCompleted  TaskStatus = "completed"
	StatusDead       TaskStatus = "dead"
)

type Task struct {
	ID             string
	IdempotencyKey string
	Payload        string
	Status         TaskStatus
	Attempts       int
	MaxAttempts    int
	VisibleAt      time.Time
	LeaseExpiresAt *time.Time
	LastError      string
}

type Queue struct {
	conn        *pgxpool.Pool
	lease       time.Duration
	maxAttempts int
}

func NewQueue(conn *pgxpool.Pool) *Queue {
	return &Queue{
		conn:        conn,
		lease:       30 * time.Second,
		maxAttempts: 5,
	}
}

func (q *Queue) AddTask(id, payload string) error {

	_, err := q.conn.Exec(context.Background(), "INSERT INTO tasks (id, payload, status) VALUES ($1, $2, $3)", id, payload, StatusPending)
	if err != nil {
		return err
	}

	return nil
}

func (q *Queue) DequeueTask() (*Task, error) {
	ctx := context.Background()

	tx, err := q.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id, payload string
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx,
		"SELECT id, payload, lease_expires_at FROM tasks WHERE status = $1 OR (status = $2 AND lease_expires_at < NOW()) ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
		StatusPending, StatusInProgress,
	).Scan(&id, &payload, &leaseExpiresAt)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.New("queue is empty")
		}
		return nil, err
	}

	seconds := q.lease.Seconds()
	_, err = tx.Exec(ctx,
		"UPDATE tasks SET status = $1, lease_expires_at = NOW() + ($3 * INTERVAL '1 second') WHERE id = $2",
		StatusInProgress, id, seconds,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	leaseExpiresAt = nil
	return &Task{
		ID:             id,
		Payload:        payload,
		Status:         StatusInProgress,
		LeaseExpiresAt: leaseExpiresAt,
	}, nil
}

func (q *Queue) Acknowledge(id string) error {
	tag, err := q.conn.Exec(context.Background(),
		"UPDATE tasks SET status = $1 WHERE id = $2 AND status = $3",
		StatusCompleted, id, StatusInProgress,
	)

	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return errors.New("task not found or not in progress")
	}

	return nil
}
