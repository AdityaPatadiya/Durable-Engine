.PHONY: install dev test lint format run clean docker-build docker-up

# Auto-detect docker compose command
DOCKER_COMPOSE := $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; elif command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; else echo ""; fi)

install:
	pip install -e .

install-kafka:
	pip install -e ".[kafka]"

install-rabbitmq:
	pip install -e ".[rabbitmq]"

install-cassandra:
	pip install -e ".[cassandra]"

install-dynamodb:
	pip install -e ".[dynamodb]"

install-all-sinks:
	pip install -e ".[all-sinks]"

dev:
	pip install -e ".[dev]"

test:
	pytest tests/

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-perf:
	pytest tests/performance/ -v

test-cov:
	pytest --cov=durable_engine --cov-report=html tests/

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

run:
	python -m durable_engine --config config/default.yaml

run-live:
	python -m durable_engine --config config/live.yaml

run-docker:
	python -m durable_engine --config config/docker.yaml

run-webhook:
	python -m durable_engine --config config/source-webhook.yaml

run-kafka-source:
	python -m durable_engine --config config/source-kafka.yaml

run-websocket:
	python -m durable_engine --config config/source-websocket.yaml

run-postgres-cdc:
	python -m durable_engine --config config/source-postgres-cdc.yaml

run-prod:
	python -m durable_engine --config config/production.yaml

infra-up:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "Error: Neither 'docker compose' nor 'docker-compose' found. Install with: sudo apt install docker-compose-v2"; exit 1; fi
	$(DOCKER_COMPOSE) up kafka cassandra rabbitmq rest-api
	@echo "Waiting for services to be healthy..."
	$(DOCKER_COMPOSE) up cassandra-init

infra-down:
	$(DOCKER_COMPOSE) down

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +

docker-build:
	docker build -t durable-engine:latest .

docker-up:
	$(DOCKER_COMPOSE) up

docker-down:
	$(DOCKER_COMPOSE) down

proto:
	python -m grpc_tools.protoc -I proto --python_out=src/durable_engine/transformation --grpc_python_out=src/durable_engine/transformation proto/record.proto
