package main

import (
	"fmt"
)

func main() {
	q := newQueue()
	q.AddTask("1", "Task 1 payload")
	q.AddTask("2", "Task 2 payload")

	task, err := q.DequeueTask()
	fmt.Println(task, err)
	first := q.Acknowledge("1")
	fmt.Println(first)
	fmt.Println(q.Acknowledge("1"))
	task, err = q.DequeueTask()
	fmt.Println(task, err)
	task, err = q.DequeueTask()
	fmt.Println(task, err)

}
