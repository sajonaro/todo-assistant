# App data lives in `todo`; DBOS workflow state lives in a SEPARATE `todo_dbos_sys`
# database (so reset_system_database in tests never touches todo.tasks).
# These URLs are for LOCAL dev (uv); the containerized app uses db:5432 (see compose).
APP_DB_URL ?= postgresql://postgres:postgres@localhost:5433/todo
SYS_DB_URL ?= postgresql://postgres:postgres@localhost:5433/todo_dbos_sys
export APP_DATABASE_URL = $(APP_DB_URL)
export DBOS_SYSTEM_DATABASE_URL = $(SYS_DB_URL)

.PHONY: install db up down migrate run test lint

install:                ## install deps into a uv-managed venv
	uv sync --extra dev

db:                     ## start ONLY postgres (for local dev with `make run`)
	docker compose up -d db
	@echo "waiting for postgres..."
	@until docker compose exec -T db pg_isready -U postgres -d todo >/dev/null 2>&1; do sleep 1; done
	@echo "postgres ready"

up-local:                     ## build + run the WHOLE stack (postgres + app) in containers
	docker compose up -d --build
	@echo "app: http://localhost:8000"

up:	
	docker compose pull && docker compose up
	@echo "app: http://localhost:8000"
	
down:
	docker compose down

migrate:                ## optional: the app also self-migrates on startup
	docker compose exec -T db psql -U postgres -d todo < migrations/001_init.sql

run:                    ## run the app locally via uv (needs: make db)
	uv run python -m app.main

test:                   ## run the test suite via uv (needs: make db)
	uv run pytest

lint:                   ## lint with ruff (add --fix to auto-fix)
	uv run ruff check app tests
