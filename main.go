package main

import (
	"context"
	"log"
	"time"
)

func main() {
	conn, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Exec(context.Background(), "DELETE FROM tasks"); err != nil {
		log.Fatal(err)
	}

	q := NewQueue(conn)
	if err := q.AddTask("1", "Task 1 payload"); err != nil {
		log.Fatal(err)
	}
	if err := q.AddTask("2", "Task 2 payload"); err != nil {
		log.Fatal(err)
	}

	go worker(1, q)
	go worker(2, q)
	time.Sleep(10 * time.Second)
}
