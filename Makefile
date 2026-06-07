.DEFAULT_GOAL := help
.PHONY: help install setup dev lint format type-check test test-unit test-cov \
        docker-up docker-down docker-logs migrate clean

help:
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

install: ## Install project
	pip install -e .

setup: ## Install with dev deps and copy .env
	pip install -e ".[dev]"
	cp -n .env.example .env || true

dev: ## Start DB, runtime API, and web dashboard (Ctrl-C stops all)
	docker compose up -d
	@ALPHA_PID=""; \
	alpha run & ALPHA_PID=$$!; \
	trap "kill $$ALPHA_PID 2>/dev/null; docker compose down" EXIT INT TERM; \
	cd web && pnpm dev

lint: ## Run ruff linter
	ruff check src/ tests/
	ruff format --check src/ tests/

format: ## Auto-format code
	ruff format src/ tests/
	ruff check --fix src/ tests/

type-check: ## Run mypy
	mypy src/

test: ## Run all tests
	pytest tests/ -v --cov=alpha --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

test-cov: ## Generate HTML coverage report
	pytest tests/ --cov=alpha --cov-report=html
	open htmlcov/index.html

docker-up: ## Start all Docker services
	docker compose up -d

docker-down: ## Stop all Docker services
	docker compose down

docker-logs: ## Tail Docker logs
	docker compose logs -f

migrate: ## Run DB migrations (requires alembic.ini)
	@test -f alembic.ini || { echo "No alembic.ini found — schema is managed by Docker init SQL"; exit 0; }
	alembic upgrade head

clean: ## Remove build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name "*.pyc" -delete 2>/dev/null; true
	rm -rf .coverage htmlcov/ dist/ .mypy_cache/ .ruff_cache/ .pytest_cache/
