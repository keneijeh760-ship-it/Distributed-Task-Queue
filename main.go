package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	conn, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if len(os.Args) > 1 && os.Args[1] == "worker" {
		id := 1
		if len(os.Args) > 2 {
			parsed, err := strconv.Atoi(os.Args[2])
			if err != nil {
				log.Fatal(err)
			}
			id = parsed
		}
		q := NewQueue(conn)
		fmt.Printf("worker %d waiting for tasks\n", id)
		worker(id, q, nil)
		return
	}

	runDemo(conn)
}

func runDemo(conn *pgxpool.Pool) {
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "DELETE FROM dead_letters"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "DELETE FROM tasks"); err != nil {
		log.Fatal(err)
	}

	q := NewQueue(conn)
	q.lease = 2 * time.Second

	for i := 1; i <= 4; i++ {
		id := strconv.Itoa(i)
		if _, err := q.AddTask(id, "Task "+id+" payload"); err != nil {
			log.Fatal(err)
		}
	}

	abandoned := make(chan string, 1)
	go func() {
		task, err := q.DequeueTask()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("crash worker abandoned task %s\n", task.ID)
		abandoned <- task.ID
	}()

	leftBehind := <-abandoned
	fmt.Printf("task %s is leased with no heartbeat\n", leftBehind)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for id := 1; id <= 2; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(id, q, stop)
		}(id)
	}

	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			close(stop)
			log.Fatal("timed out waiting for tasks to complete")
		case <-tick.C:
			var n int
			err := conn.QueryRow(ctx, `
				SELECT COUNT(*) FROM tasks
				WHERE status = $1 AND id IN ('1', '2', '3', '4')`,
				StatusCompleted,
			).Scan(&n)
			if err != nil {
				close(stop)
				log.Fatal(err)
			}
			if n == 4 {
				close(stop)
				wg.Wait()
				fmt.Println("all tasks completed")
				return
			}
		}
	}
}
