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
