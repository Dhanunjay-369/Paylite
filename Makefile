.PHONY: up down test run stack demo

up: ## start Postgres and Redis only (for local dev and tests)
	docker compose up -d db redis

down: ## stop everything and delete data
	docker compose down -v

test: ## run all tests with the race detector (needs `make up`)
	REQUIRE_INTEGRATION=1 go test -race -count=1 ./...

run: ## run the API locally against `make up`
	ENABLE_DEBUG=1 PROCESSOR_LATENCY_MS=200 CHARGE_TIMEOUT_MS=1000 go run ./cmd/api

stack: ## build and run API + Postgres + Redis in Docker
	docker compose up --build -d

demo: ## replay the three failure scenarios against a running API
	bash ./scripts/demo.sh
