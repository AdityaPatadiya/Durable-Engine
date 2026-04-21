.PHONY: install dev test lint format run clean docker-build docker-up \
	helm-template helm-lint helm-install helm-upgrade helm-uninstall \
	k8s-staging k8s-production \
	k8s-apply-stable-service k8s-apply-stable-ingress k8s-apply-stable-switch \
	k8s-deploy-blue k8s-deploy-green \
	k8s-deploy-blue-standby k8s-deploy-green-standby \
	k8s-revert-to-standby \
	k8s-switch-blue k8s-switch-green k8s-switch-active-color k8s-rollback-switch \
	k8s-show-active-status k8s-verify-persistence-isolation k8s-stable-status

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
K8S_PROD_NAMESPACE ?= durable-engine-production
STABLE_SERVICE_NAME ?= durable-engine-stable
BLUE_RELEASE ?= durable-engine-blue
GREEN_RELEASE ?= durable-engine-green
BASE_PROD_VALUES_FILE ?= $(HELM_CHART)/values-production.yaml
BLUE_VALUES_FILE ?= $(HELM_CHART)/values-production-blue.yaml
GREEN_VALUES_FILE ?= $(HELM_CHART)/values-production-green.yaml
STANDBY_VALUES_FILE ?= $(HELM_CHART)/values-production-standby.yaml
BG_IMAGE_TAG ?=
ACTIVE_COLOR ?= blue
BG_HOST ?= durable-engine.example.com
BG_INGRESS_CLASS ?= nginx

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

# ── Blue-Green Stable Switch ─────────────────────────────
k8s-deploy-blue:
	@set -eu; \
	deploy_cmd="helm upgrade --install $(BLUE_RELEASE) $(HELM_CHART) --namespace $(K8S_PROD_NAMESPACE) --create-namespace -f $(BASE_PROD_VALUES_FILE) -f $(BLUE_VALUES_FILE)"; \
	if [ -n "$(BG_IMAGE_TAG)" ]; then \
		deploy_cmd="$$deploy_cmd --set image.tag=$(BG_IMAGE_TAG)"; \
	fi; \
	echo "$$deploy_cmd"; \
	eval "$$deploy_cmd"

k8s-deploy-green:
	@set -eu; \
	deploy_cmd="helm upgrade --install $(GREEN_RELEASE) $(HELM_CHART) --namespace $(K8S_PROD_NAMESPACE) --create-namespace -f $(BASE_PROD_VALUES_FILE) -f $(GREEN_VALUES_FILE)"; \
	if [ -n "$(BG_IMAGE_TAG)" ]; then \
		deploy_cmd="$$deploy_cmd --set image.tag=$(BG_IMAGE_TAG)"; \
	fi; \
	echo "$$deploy_cmd"; \
	eval "$$deploy_cmd"

k8s-deploy-blue-standby:
	@set -eu; \
	deploy_cmd="helm upgrade --install $(BLUE_RELEASE) $(HELM_CHART) --namespace $(K8S_PROD_NAMESPACE) --create-namespace -f $(BASE_PROD_VALUES_FILE) -f $(BLUE_VALUES_FILE) -f $(STANDBY_VALUES_FILE)"; \
	if [ -n "$(BG_IMAGE_TAG)" ]; then \
		deploy_cmd="$$deploy_cmd --set image.tag=$(BG_IMAGE_TAG)"; \
	fi; \
	echo "$$deploy_cmd"; \
	eval "$$deploy_cmd"

k8s-deploy-green-standby:
	@set -eu; \
	deploy_cmd="helm upgrade --install $(GREEN_RELEASE) $(HELM_CHART) --namespace $(K8S_PROD_NAMESPACE) --create-namespace -f $(BASE_PROD_VALUES_FILE) -f $(GREEN_VALUES_FILE) -f $(STANDBY_VALUES_FILE)"; \
	if [ -n "$(BG_IMAGE_TAG)" ]; then \
		deploy_cmd="$$deploy_cmd --set image.tag=$(BG_IMAGE_TAG)"; \
	fi; \
	echo "$$deploy_cmd"; \
	eval "$$deploy_cmd"

# Reconfigure whichever release is NOT currently selected by the stable service
# to the standby profile. Use after a switch to stop the old-active from
# continuing to consume the live source.
k8s-revert-to-standby:
	@set -eu; \
	current="$$(kubectl get service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) -o jsonpath='{.spec.selector.app\\.kubernetes\\.io/instance}')"; \
	if [ "$$current" = "$(BLUE_RELEASE)" ]; then \
		inactive="$(GREEN_RELEASE)"; \
		inactive_color_values="$(GREEN_VALUES_FILE)"; \
	elif [ "$$current" = "$(GREEN_RELEASE)" ]; then \
		inactive="$(BLUE_RELEASE)"; \
		inactive_color_values="$(BLUE_VALUES_FILE)"; \
	else \
		echo "Unexpected active release '$$current'. Expected $(BLUE_RELEASE) or $(GREEN_RELEASE)." >&2; \
		exit 1; \
	fi; \
	if ! helm status "$$inactive" -n $(K8S_PROD_NAMESPACE) >/dev/null 2>&1; then \
		echo "Inactive release '$$inactive' is not installed; nothing to revert."; \
		exit 0; \
	fi; \
	echo "Reverting $$inactive to standby profile"; \
	helm upgrade "$$inactive" $(HELM_CHART) --namespace $(K8S_PROD_NAMESPACE) \
		-f $(BASE_PROD_VALUES_FILE) -f $$inactive_color_values -f $(STANDBY_VALUES_FILE)

k8s-apply-stable-service:
	kubectl apply -n $(K8S_PROD_NAMESPACE) -f k8s/blue-green/stable-service.yaml

k8s-apply-stable-ingress:
	@set -eu; \
	if ! command -v envsubst >/dev/null 2>&1; then \
		echo "envsubst is required (install with 'apt install gettext-base')." >&2; \
		exit 1; \
	fi; \
	BG_HOST='$(BG_HOST)' BG_INGRESS_CLASS='$(BG_INGRESS_CLASS)' \
		envsubst '$$BG_HOST $$BG_INGRESS_CLASS' \
		< k8s/blue-green/stable-ingress.yaml \
		| kubectl apply -n $(K8S_PROD_NAMESPACE) -f -

k8s-apply-stable-switch: k8s-apply-stable-service k8s-apply-stable-ingress

k8s-switch-blue:
	kubectl patch service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) --type=merge -p '{"spec":{"selector":{"app.kubernetes.io/name":"durable-engine","app.kubernetes.io/instance":"$(BLUE_RELEASE)"}}}'

k8s-switch-green:
	kubectl patch service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) --type=merge -p '{"spec":{"selector":{"app.kubernetes.io/name":"durable-engine","app.kubernetes.io/instance":"$(GREEN_RELEASE)"}}}'

k8s-switch-active-color:
	@if [ "$(ACTIVE_COLOR)" = "blue" ]; then \
		$(MAKE) k8s-switch-blue; \
	elif [ "$(ACTIVE_COLOR)" = "green" ]; then \
		$(MAKE) k8s-switch-green; \
	else \
		echo "ACTIVE_COLOR must be 'blue' or 'green'" >&2; \
		exit 1; \
	fi

k8s-rollback-switch:
	@set -eu; \
	current="$$(kubectl get service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) -o jsonpath='{.spec.selector.app\\.kubernetes\\.io/instance}')"; \
	if [ "$$current" = "$(BLUE_RELEASE)" ]; then \
		target="$(GREEN_RELEASE)"; \
	elif [ "$$current" = "$(GREEN_RELEASE)" ]; then \
		target="$(BLUE_RELEASE)"; \
	else \
		echo "Unexpected active release '$$current'. Expected $(BLUE_RELEASE) or $(GREEN_RELEASE)." >&2; \
		exit 1; \
	fi; \
	patch_payload="$$(printf '{"spec":{"selector":{"app.kubernetes.io/name":"durable-engine","app.kubernetes.io/instance":"%s"}}}' "$$target")"; \
	kubectl patch service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) --type=merge -p "$$patch_payload"; \
	echo "Rolled back stable selector to $$target"

k8s-show-active-status:
	@set -eu; \
	current="$$(kubectl get service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) -o jsonpath='{.spec.selector.app\\.kubernetes\\.io/instance}')"; \
	if [ "$$current" = "$(BLUE_RELEASE)" ]; then \
		color="blue"; \
	elif [ "$$current" = "$(GREEN_RELEASE)" ]; then \
		color="green"; \
	else \
		color="unknown"; \
	fi; \
	echo "active_release=$$current"; \
	echo "active_color=$$color"; \
	kubectl get service $(STABLE_SERVICE_NAME) -n $(K8S_PROD_NAMESPACE) -o wide

k8s-verify-persistence-isolation:
	@set -eu; \
	blue_pvcs="$$(kubectl get pvc -n $(K8S_PROD_NAMESPACE) -l app.kubernetes.io/instance=$(BLUE_RELEASE) -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"; \
	green_pvcs="$$(kubectl get pvc -n $(K8S_PROD_NAMESPACE) -l app.kubernetes.io/instance=$(GREEN_RELEASE) -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"; \
	if [ -z "$$blue_pvcs" ] || [ -z "$$green_pvcs" ]; then \
		echo "Could not find PVCs for one or both releases in namespace $(K8S_PROD_NAMESPACE)." >&2; \
		echo "blue_release=$(BLUE_RELEASE) pvcs=$$blue_pvcs" >&2; \
		echo "green_release=$(GREEN_RELEASE) pvcs=$$green_pvcs" >&2; \
		exit 1; \
	fi; \
	overlap=""; \
	for pvc in $$blue_pvcs; do \
		if printf '%s\n' "$$green_pvcs" | grep -qx "$$pvc"; then \
			overlap="$$overlap $$pvc"; \
		fi; \
	done; \
	if [ -n "$$overlap" ]; then \
		echo "Found shared PVC names across blue and green releases:$$overlap" >&2; \
		exit 1; \
	fi; \
	for pvc in $$blue_pvcs $$green_pvcs; do \
		mode="$$(kubectl get pvc "$$pvc" -n $(K8S_PROD_NAMESPACE) -o jsonpath='{.spec.accessModes[0]}')"; \
		if [ "$$mode" != "ReadWriteOnce" ]; then \
			echo "PVC $$pvc is not ReadWriteOnce (found: $$mode)." >&2; \
			exit 1; \
		fi; \
	done; \
	echo "Persistence isolation verified."; \
	echo "blue_pvcs=$$(printf '%s' "$$blue_pvcs" | tr '\n' ' ')"; \
	echo "green_pvcs=$$(printf '%s' "$$green_pvcs" | tr '\n' ' ')"

k8s-stable-status: k8s-show-active-status
