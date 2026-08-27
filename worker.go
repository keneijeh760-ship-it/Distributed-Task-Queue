package main

import (
	"fmt"
	"time"
)

func worker(id int, q *Queue) {
	for {
		task, err := q.DequeueTask()
		if err != nil {
			time.Sleep()
			continue
		}
		fmt.Printf("worker %d processing task %s\n", id, task.ID)
		time.Sleep(1 * time.Second) // simulate doing work
		q.Acknowledge(task.ID)
		fmt.Printf("worker %d finished task %s\n", id, task.ID)
	}
}
