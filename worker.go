package main

import (
	"fmt"
	"time"
)

func worker(id int, q *Queue, stop <-chan struct{}) {
	if stop == nil {
		stop = make(chan struct{})
	}
	heartbeatEvery := q.lease / 3
	if heartbeatEvery < 50*time.Millisecond {
		heartbeatEvery = 50 * time.Millisecond
	}

	for {
		if stopped(stop) {
			return
		}

		task, err := q.DequeueTask()
		if err != nil {
			if err.Error() != "queue is empty" {
				fmt.Printf("worker %d dequeue error: %v\n", id, err)
			}
			if !sleepOrStop(stop, 200*time.Millisecond) {
				return
			}
			continue
		}

		fmt.Printf("worker %d processing task %s\n", id, task.ID)
		if !workThenAck(id, q, task, heartbeatEvery, time.Second, stop) {
			return
		}
	}
}

func workThenAck(id int, q *Queue, task *Task, heartbeatEvery, work time.Duration, stop <-chan struct{}) bool {
	workTimer := time.NewTimer(work)
	defer workTimer.Stop()
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return false
		case <-workTimer.C:
			if err := q.Acknowledge(task.ID); err != nil {
				fmt.Printf("worker %d acknowledge error for task %s: %v\n", id, task.ID, err)
				return true
			}
			fmt.Printf("worker %d finished task %s\n", id, task.ID)
			return true
		case <-ticker.C:
			if err := q.RenewLease(task.ID); err != nil {
				fmt.Printf("worker %d renew error for task %s: %v\n", id, task.ID, err)
				return true
			}
		}
	}
}

func stopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func sleepOrStop(stop <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}
