package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAddTask_Success(t *testing.T) {
	q := setupTestQueue(t)

	task, err := q.AddTask("1", "Task 1 payload")
	if err != nil {
		t.Fatalf("failed to add task: %v", err)
	}
	if task.ID != "1" || task.Payload != "Task 1 payload" || task.Status != StatusPending {
		t.Fatalf("unexpected task: %+v", task)
	}
	if task.IdempotencyKey != "1" {
		t.Fatalf("expected idempotency key 1, got %q", task.IdempotencyKey)
	}
}

func TestAddTask_DuplicateID(t *testing.T) {
	q := setupTestQueue(t)

	first, err := q.AddTask("1", "original payload")
	if err != nil {
		t.Fatalf("failed to add task: %v", err)
	}

	second, err := q.AddTask("1", "changed payload")
	if err != nil {
		t.Fatalf("duplicate add should return the original task: %v", err)
	}
	if second.ID != first.ID || second.Payload != "original payload" {
		t.Fatalf("duplicate overwrote the task: %+v", second)
	}
}

func setupTestQueue(t *testing.T) *Queue {
	t.Helper()
	conn, err := connectDB()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(conn.Close)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "DELETE FROM dead_letters"); err != nil {
		t.Fatalf("failed to clean dead letters: %v", err)
	}
	if _, err := conn.Exec(ctx, "DELETE FROM tasks"); err != nil {
		t.Fatalf("failed to clean table: %v", err)
	}
	return NewQueue(conn)
}
