.PHONY: up test bench demo

up:
	docker compose up -d

test:
	go test -count=1 -v

bench:
	go test -bench=BenchmarkDequeueAcknowledge -benchtime=1s -run=^$$

demo:
	go run .
