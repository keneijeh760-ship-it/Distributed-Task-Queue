package main

import (
	"errors"

	"context"

	"time"

	"github.com/jackc/pgx/v5"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusInProgress TaskStatus = "in_progress"
	StatusCompleted  TaskStatus = "completed"
)

type Task struct {
	ID             string
	Payload        string
	Status         TaskStatus
	LeaseExpiresAt *time.Time
}

type Queue struct {
	conn *pgx.Conn
}

func NewQueue(conn *pgx.Conn) *Queue {
	return &Queue{
		conn: conn,
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

	newLease := time.Now().Add(30 * time.Second)
	leaseExpiresAt = &newLease

	_, err = tx.Exec(ctx,
		"UPDATE tasks SET status = $1, lease_expires_at = $3 WHERE id = $2",
		StatusInProgress, id, leaseExpiresAt,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

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
