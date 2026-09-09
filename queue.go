package main

import (
	"context"
	"errors"
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

type rowScanner interface {
	Scan(dest ...any) error
}

const taskSelectColumns = `id, idempotency_key, payload, status, attempts, max_attempts, visible_at, lease_expires_at, last_error`

func scanTask(row rowScanner) (*Task, error) {
	var task Task
	var status string
	var key *string
	var lastError *string
	err := row.Scan(
		&task.ID,
		&key,
		&task.Payload,
		&status,
		&task.Attempts,
		&task.MaxAttempts,
		&task.VisibleAt,
		&task.LeaseExpiresAt,
		&lastError,
	)
	if err != nil {
		return nil, err
	}
	task.Status = TaskStatus(status)
	if key != nil {
		task.IdempotencyKey = *key
	}
	if lastError != nil {
		task.LastError = *lastError
	}
	return &task, nil
}

func (q *Queue) AddTask(idempotencyKey, payload string) (*Task, error) {
	ctx := context.Background()
	task, err := scanTask(q.conn.QueryRow(ctx, `
		INSERT INTO tasks (id, idempotency_key, payload, status, max_attempts)
		VALUES ($1, $1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING
		RETURNING `+taskSelectColumns,
		idempotencyKey, payload, StatusPending, q.maxAttempts,
	))
	if err == nil {
		return task, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	existing, err := scanTask(q.conn.QueryRow(ctx, `
		SELECT `+taskSelectColumns+` FROM tasks WHERE id = $1 OR idempotency_key = $1`,
		idempotencyKey,
	))
	if err != nil {
		return nil, err
	}
	return existing, nil
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
