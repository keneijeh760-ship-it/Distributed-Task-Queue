package main

import (
	"errors"

	"context"

	"github.com/jackc/pgx/v5"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusInProgress TaskStatus = "in_progress"
	StatusCompleted  TaskStatus = "completed"
)

type Task struct {
	ID      string
	Payload string
	Status  TaskStatus
}

type Queue struct {
	conn *pgx.Conn
}

func NewQueue(conn *pgx.Conn) *Queue {
	return &Queue{
		conn: conn,
	}
}

func (q *Queue) DequeueTask() (*Task, error) {
	ctx := context.Background()

	tx, err := q.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id, payload string
	err = tx.QueryRow(ctx,
		"SELECT id, payload FROM tasks WHERE status = $1 ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
		StatusPending,
	).Scan(&id, &payload)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.New("queue is empty")
		}
		return nil, err
	}

	_, err = tx.Exec(ctx,
		"UPDATE tasks SET status = $1 WHERE id = $2",
		StatusInProgress, id,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &Task{
		ID:      id,
		Payload: payload,
		Status:  StatusInProgress,
	}, nil
}

func (q *Queue) DequeueTask() (*Task, error) {

	ctx := context.Background()

	tx, err := q.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var id, payload string

}

func (q *Queue) Acknowledge(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[id]

	if !exists {
		return errors.New("task not found")
	}

	if task.Status != StatusInProgress {
		return errors.New("you shouldn't be able to acknowledge a task that was never leased.")
	}

	task.Status = StatusCompleted
	return nil
}
