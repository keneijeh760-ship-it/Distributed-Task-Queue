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
	for {
		tx, err := q.conn.Begin(ctx)
		if err != nil {
			return nil, err
		}

		task, err := scanTask(tx.QueryRow(ctx, `
			SELECT `+taskSelectColumns+`
			FROM tasks
			WHERE (status = $1 AND visible_at <= NOW())
			   OR (status = $2 AND lease_expires_at < NOW())
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED`,
			StatusPending, StatusInProgress,
		))
		if err != nil {
			_ = tx.Rollback(ctx)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.New("queue is empty")
			}
			return nil, err
		}


		claimed, err := scanTask(tx.QueryRow(ctx, `
			UPDATE tasks
			SET status = $1,
			    lease_expires_at = NOW() + ($2 * INTERVAL '1 second'),
			    attempts = attempts + 1
			WHERE id = $3
			RETURNING `+taskSelectColumns,
			StatusInProgress, q.lease.Seconds(), task.ID,
		))
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return claimed, nil
	}
}

func (q *Queue) RenewLease(id string) error {
	tag, err := q.conn.Exec(context.Background(), `
		UPDATE tasks
		SET lease_expires_at = NOW() + ($2 * INTERVAL '1 second')
		WHERE id = $1 AND status = $3`,
		id, q.lease.Seconds(), StatusInProgress,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errors.New("task not found or not in progress")
	}
	return nil
}

func (q *Queue) Acknowledge(id string) error {
	tag, err := q.conn.Exec(context.Background(), `
		UPDATE tasks
		SET status = $1, lease_expires_at = NULL
		WHERE id = $2 AND status = $3`,
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
