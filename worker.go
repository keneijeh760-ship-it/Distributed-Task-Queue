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
		q.Acknowledge(task.ID)
		fmt.Printf("worker %d finished task %s\n", id, task.ID)
		if err := q.AddTask("1", "Task 1 payload"); err != nil {
			fmt.Println("AddTask 1 error:", err)
		}
		if err := q.AddTask("2", "Task 2 payload"); err != nil {
			fmt.Println("AddTask 2 error:", err)
		}
	}
}
