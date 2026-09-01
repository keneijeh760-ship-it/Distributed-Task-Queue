package main

import (
	"fmt"
	"time"
)

func worker(id int, q *Queue, stop <-chan struct{}) {
	if stop == nil {
		stop = make(chan struct{})
	}
	for {
		select {
		case <-stop:
			return
		default:
		}
		task, err := q.DequeueTask()
		if err != nil {
			if err.Error() != "queue is empty" {
				fmt.Printf("worker %d dequeue error: %v\n", id, err)
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		fmt.Printf("worker %d processing task %s\n", id, task.ID)
		time.Sleep(1 * time.Second)
		if err := q.Acknowledge(task.ID); err != nil {
			fmt.Printf("worker %d acknowledge error for task %s: %v\n", id, task.ID, err)
			continue
		}
		fmt.Printf("worker %d finished task %s\n", id, task.ID)
	}
}
