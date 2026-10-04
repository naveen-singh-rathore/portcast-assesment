.PHONY: install-dev format lint typecheck test check redis-up redis-down

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
