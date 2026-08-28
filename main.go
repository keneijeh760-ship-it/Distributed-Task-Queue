package main

import (
	"log"
	"time"
)

func main() {

	conn, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	q := NewQueue(conn)

	q.AddTask("1", "Task 1 payload")
	q.AddTask("2", "Task 2 payload")

	go worker(1, q)
	go worker(2, q)
	time.Sleep(10 * time.Second)
}
