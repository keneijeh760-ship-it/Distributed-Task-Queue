package main

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkDequeueAcknowledge(b *testing.B) {
	conn, err := connectDB()
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	if _, err := conn.Exec(ctx, "DELETE FROM dead_letters"); err != nil {
		b.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "DELETE FROM tasks"); err != nil {
		b.Fatal(err)
	}

	q := NewQueue(conn)
	const batch = 500
	seed := func(start int) {
		b.Helper()
		for i := 0; i < batch; i++ {
			id := fmt.Sprintf("bench-%d", start+i)
			if _, err := q.AddTask(id, "payload"); err != nil {
				b.Fatal(err)
			}
		}
	}
	seed(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task, err := q.DequeueTask()
		if err != nil {
			b.StopTimer()
			seed((i/batch + 1) * batch)
			b.StartTimer()
			task, err = q.DequeueTask()
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := q.Acknowledge(task.ID); err != nil {
			b.Fatal(err)
		}
	}
}
