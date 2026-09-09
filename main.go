package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

func main() {
	conn, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx := context.Background()
	if _, err := conn.Exec(ctx, "DELETE FROM tasks"); err != nil {
		log.Fatal(err)
	}

	q := NewQueue(conn)
	for _, id := range []string{"1", "2", "3", "4"} {
		if _, err := q.AddTask(id, "Task "+id+" payload"); err != nil {
			log.Fatal(err)
		}
	}

	go worker(1, q, nil)
	go worker(2, q, nil)

	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			log.Fatal("timed out waiting for tasks to complete")
		case <-tick.C:
			var n int
			err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM tasks WHERE status = $1", StatusCompleted).Scan(&n)
			if err != nil {
				log.Fatal(err)
			}
			if n == 4 {
				fmt.Println("all tasks completed")
				return
			}
		}
	}
}
