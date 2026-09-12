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

func TestDequeueTask_FIFO(t *testing.T) {
	q := setupTestQueue(t)

	if _, err := q.AddTask("1", "Task 1 payload"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.AddTask("2", "Task 2 payload"); err != nil {
		t.Fatal(err)
	}

	first, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue task: %v", err)
	}
	if first.ID != "1" || first.Payload != "Task 1 payload" || first.Status != StatusInProgress {
		t.Fatalf("unexpected task: %+v", first)
	}
	if first.Attempts != 1 {
		t.Fatalf("expected attempts 1, got %d", first.Attempts)
	}

	second, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue second task: %v", err)
	}
	if second.ID != "2" {
		t.Fatalf("expected task 2 next, got %s", second.ID)
	}
}

func TestDequeueTask_EmptyQueue(t *testing.T) {
	q := setupTestQueue(t)

	_, err := q.DequeueTask()
	if err == nil {
		t.Fatal("expected error for empty queue, got nil")
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
