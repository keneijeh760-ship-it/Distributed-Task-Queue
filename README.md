# Distributed Task Queue

![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![Postgres](https://img.shields.io/badge/Postgres-16-336791?logo=postgresql&logoColor=white)
![CI](https://github.com/keneijeh760-ship-it/Distributed-Task-Queue/actions/workflows/test.yml/badge.svg)

A small durable queue. Producers insert a task once. Any number of workers compete to claim it. A claim is a lease, not a permanent lock, so a worker that dies does not take the task with it.

```bash
docker compose up -d
go test -count=1 -v
go run .
```

Postgres listens on `localhost:5444` (`taskqueue` / `taskqueue`, database `taskqueue`).

## What it guarantees

Delivery is at least once. A crash after the work and before `Acknowledge` runs the handler again. The caller-supplied id is the idempotency key, so a retried `AddTask` returns the original row and does not change the payload. Consumers should treat that id as their own dedupe key.

A row can be claimed when:

- it is `pending` and `visible_at` has passed, or
- it is `in_progress` and `lease_expires_at` is already in the past

`DequeueTask` locks one row with `FOR UPDATE SKIP LOCKED`, ordered by `created_at`. Two workers cannot claim the same row. FIFO applies to rows that are visible now. A task waiting out its backoff can let a newer pending task run first, so one failure does not block the queue.
