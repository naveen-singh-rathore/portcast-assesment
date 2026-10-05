.PHONY: install-dev format lint typecheck test check redis-up redis-down up down load bench

install-dev:
	pip install -r requirements-dev.txt
	pre-commit install

format:
	ruff check --fix .
	black .

lint:
	black --check .
	ruff check .

typecheck:
	mypy .

test:
	pytest

# Same checks CI runs.
check: lint typecheck test

redis-up:
	docker compose up -d --wait redis

redis-down:
	docker compose down -v

# Redis + 3 API replicas + nginx on :8080
up:
	docker compose up --build -d --wait

down:
	docker compose down -v

# Full stack + 60 s HTTP load test; exits non-zero if the audit fails
load:
	docker compose up --build -d --wait
	docker compose --profile load build loadgen
	docker compose --profile load run --rm --no-deps loadgen

# Library-only latency/throughput + invariant audit
bench:
	docker compose up -d --wait redis
	docker compose --profile bench run --rm --build bench
