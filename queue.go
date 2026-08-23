package main

import (
	"errors"
	"sync"
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
	mu    sync.Mutex
	tasks map[string]*Task
	order []string
}

func newQueue() *Queue {
	return &Queue{
		tasks: make(map[string]*Task),
		order: []string{},
	}
}

func (q *Queue) AddTask(id, payload string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.tasks[id]; exists {
		return errors.New("task already exists")
	}

	task := &Task{
		ID:      id,
		Payload: payload,
		Status:  StatusPending,
	}
	q.tasks[id] = task
	q.order = append(q.order, id)
	return nil
}

func (q *Queue) DequeueTask() (*Task, error) {

	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.order) == 0 {
		return nil, errors.New("queue is empty")
	}

	id := q.order[0]

	q.order = q.order[1:]
	task, exists := q.tasks[id]

	if !exists {
		return nil, errors.New("task not found")
	}

	task.Status = StatusInProgress
	return task, nil

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
