.PHONY: install dev test lint format run clean docker-build docker-up \
	helm-template helm-lint helm-install helm-upgrade helm-uninstall \
	k8s-staging k8s-production

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

dlq-replay:
	python -m durable_engine dlq-replay --dlq-dir dlq/ --config config/default.yaml

dlq-replay-dry:
	python -m durable_engine dlq-replay --dlq-dir dlq/ --dry-run

infra-up:
	@if [ -z "$(DOCKER_COMPOSE)" ]; then echo "Error: Neither 'docker compose' nor 'docker-compose' found. Install with: sudo apt install docker-compose-v2"; exit 1; fi
	$(DOCKER_COMPOSE) up -d kafka cassandra rabbitmq rest-api
	@echo "Waiting for services to be healthy..."
	$(DOCKER_COMPOSE) up cassandra-init
	@echo "Infrastructure is ready. Use 'make infra-down' to stop."

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

# ── Helm ──────────────────────────────────────────────────
HELM_RELEASE ?= durable-engine
HELM_NAMESPACE ?= durable-engine
HELM_CHART := helm/durable-engine

helm-template:
	helm template $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)

helm-template-staging:
	helm template $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)-staging -f $(HELM_CHART)/values-staging.yaml

helm-template-production:
	helm template $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)-production -f $(HELM_CHART)/values-production.yaml

helm-lint:
	helm lint $(HELM_CHART)
	helm lint $(HELM_CHART) -f $(HELM_CHART)/values-staging.yaml
	helm lint $(HELM_CHART) -f $(HELM_CHART)/values-production.yaml

helm-install:
	helm install $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE) --create-namespace

helm-install-staging:
	helm install $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)-staging --create-namespace -f $(HELM_CHART)/values-staging.yaml

helm-install-production:
	helm install $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)-production --create-namespace -f $(HELM_CHART)/values-production.yaml

helm-upgrade:
	helm upgrade $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)

helm-uninstall:
	helm uninstall $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

# ── Kustomize ─────────────────────────────────────────────
k8s-base:
	kubectl kustomize k8s/base

k8s-staging:
	kubectl kustomize k8s/overlays/staging

k8s-production:
	kubectl kustomize k8s/overlays/production

k8s-apply-staging:
	kubectl apply -k k8s/overlays/staging

k8s-apply-production:
	kubectl apply -k k8s/overlays/production
