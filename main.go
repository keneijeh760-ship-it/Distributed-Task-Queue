package main

import (
	"fmt"
	"log"

	"context"
)

func main() {

	conn, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	q := NewQueue(conn)

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
