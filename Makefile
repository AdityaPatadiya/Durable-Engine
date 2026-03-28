.PHONY: install dev test lint format run clean docker-build docker-up

install:
	pip install -e .

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

run-prod:
	python -m durable_engine --config config/production.yaml

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +

docker-build:
	docker build -t durable-engine:latest .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

proto:
	python -m grpc_tools.protoc -I proto --python_out=src/durable_engine/transformation --grpc_python_out=src/durable_engine/transformation proto/record.proto
