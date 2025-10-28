.PHONY: help setup install start stop restart status logs clean test validate lint format type-check build

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initial project setup (install dependencies, create .env)
	@echo "Setting up CDC demo project..."
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env file"; fi
	@poetry install
	@echo "Setup complete! Run 'make start' to launch services."

install: ## Install Python dependencies
	@poetry install

start: ## Start all services via docker-compose
	@echo "Starting CDC demo services..."
	@docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@docker compose ps

stop: ## Stop all services
	@echo "Stopping CDC demo services..."
	@docker compose down

restart: ## Restart all services
	@make stop
	@make start

status: ## Show status of all services
	@docker compose ps

logs: ## Show logs from all services (use SERVICE=<name> for specific service)
ifdef SERVICE
	@docker compose logs -f $(SERVICE)
else
	@docker compose logs -f
endif

clean: ## Stop services and remove volumes
	@echo "Cleaning up CDC demo..."
	@docker compose down -v
	@rm -rf .pytest_cache .coverage htmlcov
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup complete"

test: ## Run all tests
	@echo "Running tests..."
	@poetry run pytest

test-unit: ## Run unit tests only
	@poetry run pytest tests/unit -m unit

test-integration: ## Run integration tests only
	@poetry run pytest tests/integration -m integration

test-e2e: ## Run end-to-end tests only
	@poetry run pytest tests/e2e -m e2e

test-dq: ## Run data quality tests only
	@poetry run pytest tests/data_quality -m data_quality

test-coverage: ## Run tests with coverage report
	@poetry run pytest --cov=src --cov-report=html --cov-report=term-missing

validate: ## Run validation checks (data integrity, CDC lag)
	@poetry run cdc-demo validate all

lint: ## Run linter (ruff)
	@poetry run ruff check src tests

format: ## Format code with black
	@poetry run black src tests

format-check: ## Check code formatting without making changes
	@poetry run black --check src tests

type-check: ## Run type checker (mypy)
	@poetry run mypy src

quality: lint type-check format-check ## Run all code quality checks

build: ## Build Docker images
	@docker compose build

quickstart: setup build start ## One-command quickstart (setup + build + start)
	@echo "CDC demo is ready!"
	@echo "Services available at:"
	@echo "  - Grafana: http://localhost:3000 (admin/admin)"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  - Debezium: http://localhost:8083"
	@echo ""
	@echo "Run 'make test' to execute tests"
	@echo "Run 'cdc-demo --help' for CLI commands"

demo: ## Run a demo scenario
	@poetry run cdc-demo demo run

generate: ## Generate mock data
	@poetry run cdc-demo generate customers --count 1000
	@poetry run cdc-demo generate orders --count 5000
	@poetry run cdc-demo generate products --count 500

monitor: ## Open monitoring dashboard
	@echo "Opening Grafana dashboard..."
	@open http://localhost:3000 || xdg-open http://localhost:3000 || echo "Open http://localhost:3000 in your browser"
