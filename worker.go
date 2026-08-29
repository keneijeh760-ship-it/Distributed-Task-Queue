package main

import (
	"fmt"
	"time"
)

func worker(id int, q *Queue) {
	for {
		task, err := q.DequeueTask()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		fmt.Printf("worker %d processing task %s\n", id, task.ID)
		time.Sleep(1 * time.Second) // simulate doing work
		if err := q.Acknowledge(task.ID); err != nil {
			fmt.Printf("worker %d acknowledge error for task %s: %v\n", id, task.ID, err)
			continue
		}
		fmt.Printf("worker %d finished task %s\n", id, task.ID)
	}
}
