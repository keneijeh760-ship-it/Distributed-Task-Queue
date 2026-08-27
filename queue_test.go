package main

import (
	"context"
	"testing"
	"time"
)

func TestAddTask_Success(t *testing.T) {
	q := setupTestQueue(t)

	err := q.AddTask("1", "Task 1 payload")
	if err != nil {
		t.Fatalf("failed to add task: %v", err)
	}
}

func TestAddTask_DuplicateID(t *testing.T) {
	q := setupTestQueue(t)

	err := q.AddTask("1", "Task 1 payload")
	if err != nil {
		t.Fatalf("failed to add task: %v", err)
	}

	err = q.AddTask("1", "Task 1 payload")
	if err == nil {
		t.Fatal("expected error for duplicate task ID, got nil")
	}

}

func TestDequeueTask_FIFO(t *testing.T) {
	q := setupTestQueue(t)

	q.AddTask("1", "Task 1 payload")

	task, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue task: %v", err)
	}
	if task.ID != "1" || task.Payload != "Task 1 payload" || task.Status != StatusInProgress {
		t.Fatal("unexpected task")
	}
}

func TestDequeueTask_LeaseExpiration(t *testing.T) {
	q := setupTestQueue(t)

	q.AddTask("1", "Task 1 payload")

	task, err := q.DequeueTask()
	if err != nil {
		t.Fatalf("failed to dequeue task: %v", err)
	}

	// Simulate lease expiration
	_, err = q.conn.Exec(context.Background(),
		"UPDATE tasks SET lease_expires_at = $1 WHERE id = $2",
		time.Now().Add(-1*time.Minute), task.ID,
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

	q.AddTask("1", "Task 1 payload")
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

	q.AddTask("1", "Task 1 payload")
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

	err = q.Acknowledge(task.ID)
	if err == nil {
		t.Fatal("expected error for double acknowledge, got nil")
	}
}

func setupTestQueue(t *testing.T) *Queue {
	conn, err := connectDB()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	_, err = conn.Exec(context.Background(), "DELETE FROM tasks")
	if err != nil {
		t.Fatalf("failed to clean table: %v", err)
	}
	return NewQueue(conn)
}
