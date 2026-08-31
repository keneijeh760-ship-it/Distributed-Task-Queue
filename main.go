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
	for _, id := range []string{"1", "2", "3", "4"} {
		if err := q.AddTask(id, "Task "+id+" payload"); err != nil {
			log.Fatal(err)
		}
	}

	go worker(1, q)
	go worker(2, q)
	time.Sleep(10 * time.Second)
}
