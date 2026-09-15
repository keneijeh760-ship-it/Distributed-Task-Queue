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

func TestAcknowledge_Success(t *testing.T) {
	q := setupTestQueue(t)

	if _, err := q.AddTask("1", "Task 1 payload"); err != nil {
		t.Fatal(err)
	}
	task, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue task: %v", err)
	}

	err = q.Acknowledge(task.ID)
	if err != nil {
		t.Fatalf("failed to acknowledge task: %v", err)
	}
}

func TestAcknowledge_DoubleAcknowledge(t *testing.T) {
	q := setupTestQueue(t)

	if _, err := q.AddTask("1", "Task 1 payload"); err != nil {
		t.Fatal(err)
	}
	task, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue task: %v", err)
	}

	err = q.Acknowledge(task.ID)
	if err != nil {
		t.Fatalf("failed to acknowledge task: %v", err)
	}

	err = q.Acknowledge(task.ID)
	if err == nil {
		t.Fatal("expected error for double acknowledge, got nil")
	}
}

func TestDequeueTask_UnexpiredLeaseNotStolen(t *testing.T) {
	q := setupTestQueue(t)
	if _, err := q.AddTask("1", "Task 1 payload"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.AddTask("2", "Task 2 payload"); err != nil {
		t.Fatal(err)
	}

	first, err := q.DequeueTask()
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.DequeueTask()
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID {
		t.Fatal("unexpired lease was stolen")
	}
	if second.ID != "2" {
		t.Fatalf("expected task 2, got %s", second.ID)
	}
}

func TestDequeueTask_LeaseExpiration(t *testing.T) {
	q := setupTestQueue(t)

	if _, err := q.AddTask("1", "Task 1 payload"); err != nil {
		t.Fatal(err)
	}

	task, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue task: %v", err)
	}

	_, err = q.conn.Exec(context.Background(),
		"UPDATE tasks SET lease_expires_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
		task.ID,
	)
	if err != nil {
		t.Fatalf("failed to force lease expiry: %v", err)
	}

	task2, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("expected expired lease to be reclaimed, got error: %v", err)
	}
	if task2.ID != task.ID {
		t.Fatal("expected the same task to be reclaimed")
	}
	if task2.Status != StatusInProgress {
		t.Fatal("expected reclaimed task status to be in_progress")
	}
	if task2.Attempts != 2 {
		t.Fatalf("expected reclaim to count as a second attempt, got %d", task2.Attempts)
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
