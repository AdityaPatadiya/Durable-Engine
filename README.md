# Durable Engine

**High-Throughput Distributed Data Fan-Out & Transformation Engine**

A production-grade Python engine that reads records from any data source and fans them out to multiple downstream systems simultaneously — each in the format it expects — with full resilience, backpressure, and observability.

```
ONE Source  ──►  Durable Engine  ──►  MANY Destinations
(File/Kafka/     (transform +        (REST, gRPC, Kafka,
 Webhook/CDC/     resilience)          Cassandra, RabbitMQ,
 WebSocket)                            DynamoDB...)
```

---

## Quick Start

```bash
# Clone and install
git clone <repo-url> && cd durable-engine
python -m venv venv && source venv/bin/activate
pip install -e ".[dev]"

# Run with sample CSV data (mock sinks, no external deps needed)
make run
```

**Output:**
```
============================================================
  FINAL SUMMARY
============================================================
  Duration:           0.90s
  Records Ingested:   10
  Total Dispatched:   40
  Total Success:      40
  Total Failures:     0
  Avg Throughput:     44.6 rec/s
────────────────────────────────────────────────────────────
  rest_api              OK:       10  FAIL:      0
  grpc                  OK:       10  FAIL:      0
  message_queue         OK:       10  FAIL:      0
  wide_column_db        OK:       10  FAIL:      0
============================================================
```

---

## Architecture

```
┌──────────────────┐    ┌──────────────┐    ┌─────────────────┐
│ CSV / JSONL /    │    │              │    │  Backpressure   │
│ Kafka / Webhook /│───►│  Ingestion   │───►│  Queue (bounded)│
│ CDC / WebSocket  │    │  (async)     │    │  per sink       │
└──────────────────┘    └──────────────┘    └────────┬────────┘
                                                     │
                ┌────────────────────────────────────┤
                │              │               │              │
         ┌──────▼──────┐┌─────▼───────┐┌─────▼───────┐┌─────▼──────┐
         │ → JSON      ││ → Protobuf  ││ → XML       ││ → Avro     │
         │ Rate Limit  ││ Rate Limit  ││ Rate Limit  ││ Rate Limit │
         │ Retry + CB  ││ Retry + CB  ││ Retry + CB  ││ Retry + CB │
         ├─────────────┤├─────────────┤├─────────────┤├────────────┤
         │  REST Sink  ││  gRPC Sink  ││   MQ Sink   ││  DB Sink   │
         └──────┬──────┘└──────┬──────┘└──────┬──────┘└──────┬─────┘
                └──────────────┴───────┬──────┴──────────────┘
                                       │
                            ┌──────────▼──────────┐
                            │  DLQ + Metrics      │
                            └─────────────────────┘
```

### Data Flow

1. **Ingestion** — A streaming source reads records one at a time (files) or continuously (Kafka/webhook/CDC/WebSocket). Records are normalized into a canonical internal `Record`.
2. **Fan-Out** — Each record is placed into bounded async queues, one per sink. Backpressure prevents memory overflow if any sink is slow.
3. **Transformation** — Per-sink transformers convert records to the required wire format (JSON, Protobuf, XML, Avro) using the Strategy pattern.
4. **Dispatch** — Async worker tasks send transformed records to each sink concurrently, governed by per-sink rate limiters, retry policies, and circuit breakers.
5. **Dead Letter Queue** — Records that exhaust all retries are persisted to JSONL files with full error metadata for later reprocessing.

---

## Data Sources

| Source | Type | Command | External Dependency |
|---|---|---|---|
| CSV / JSONL / Fixed-width | File (finite) | `make run` | None |
| HTTP Webhook | Live (infinite) | `make run-webhook` | None |
| WebSocket | Live (infinite) | `make run-websocket` | None |
| Kafka Consumer | Live (infinite) | `make run-kafka-source` | Kafka broker |
| PostgreSQL CDC | Live (infinite) | `make run-postgres-cdc` | PostgreSQL (wal_level=logical) |

### Webhook Example (zero setup)

```bash
# Terminal 1: Start engine
make run-webhook

# Terminal 2: Send data
curl -X POST http://localhost:8082/ingest \
  -H "Content-Type: application/json" \
  -d '[{"name":"Alice","age":32},{"name":"Bob","age":28}]'

# Check health
curl http://localhost:8082/health
```

---

## Sinks (Destinations)

Every sink supports `mode: "mock"` (simulated) and `mode: "live"` (real connections):

| Sink | Mock | Live | Library |
|---|---|---|---|
| REST API | Simulated HTTP | Real HTTP/2 with auth/TLS | `httpx[http2]` |
| gRPC | Simulated RPC | Real `grpc.aio` channel | `grpcio` |
| Kafka | Simulated publish | Real Kafka producer | `aiokafka` |
| RabbitMQ | Simulated publish | Real AMQP publisher | `aio-pika` |
| Cassandra/ScyllaDB | Simulated UPSERT | Real CQL driver | `cassandra-driver` |
| DynamoDB | Simulated PutItem | Real AWS client | `aiobotocore` |

### Switching to Live Mode

```yaml
sinks:
  my_api:
    mode: "live"
    type: "rest"
    endpoint: "https://api.example.com/v1/records"
    auth:
      type: "bearer"
      token: "${API_TOKEN}"
    tls:
      enabled: true
```

### Default Sink Configuration

| Sink | Format | Rate Limit | Retry | Concurrency |
|---|---|---|---|---|
| REST API | JSON | 50 req/s | 3 retries, 100ms base | 10 workers |
| gRPC | Protobuf | 200 req/s | 3 retries, 50ms base | 20 workers |
| Message Queue | XML | 500 req/s | 3 retries, 200ms base | 15 workers |
| Wide-Column DB | Avro | 1000 req/s | 3 retries, 50ms base | 30 workers |

---

## Resilience

Every record goes through a full resilience pipeline per sink:

| Layer | What It Does |
|---|---|
| **Rate Limiter** | Token bucket — configurable req/sec with burst allowance and adaptive adjustment |
| **Circuit Breaker** | Opens after N failures, half-open probe after timeout, auto-recovers |
| **Retry** | Exponential backoff + jitter, max 3 attempts per record |
| **Dead Letter Queue** | Failed records saved to `dlq/<sink>.jsonl` — zero data loss |
| **Backpressure** | Bounded queues — producer pauses when sinks are slow |
| **Graceful Shutdown** | SIGINT/SIGTERM → drain queues, flush sinks, exit cleanly |

---

## Configuration

All behavior is controlled via YAML. No code changes needed.

| Config File | Purpose |
|---|---|
| `config/default.yaml` | Mock mode, reads sample CSV |
| `config/docker.yaml` | Connects to docker-compose local infra |
| `config/live.yaml` | Production template with auth + TLS |
| `config/source-kafka.yaml` | Kafka consumer source |
| `config/source-webhook.yaml` | HTTP webhook source |
| `config/source-postgres-cdc.yaml` | PostgreSQL CDC source |
| `config/source-websocket.yaml` | WebSocket source |

Key config sections:

| Section | Description |
|---|---|
| `engine` | Batch size, queue size, worker count, shutdown timeout |
| `ingestion` | Source type, file path/format, Kafka/webhook/CDC/WebSocket settings |
| `sinks.*` | Per-sink: mode, endpoint, transformer, rate limit, retry, circuit breaker, auth, TLS |
| `dlq` | Dead letter queue output directory and size limits |
| `observability` | Reporter interval/format, logging, health check, Prometheus metrics |

Environment variable interpolation: `${ENV_VAR}` or `${ENV_VAR:default}`

---

## Project Structure

```
.
├── config/                         # YAML config files (9 files)
├── proto/                          # Protocol Buffer definitions
├── samples/                        # Sample input files (CSV, JSONL, fixed-width)
├── src/durable_engine/
│   ├── __main__.py                 # CLI entrypoint (Click)
│   ├── app.py                      # Application bootstrap
│   ├── config/                     # Config loader & Pydantic models
│   ├── ingestion/                  # Sources: file readers + live (Kafka, webhook, CDC, WS)
│   ├── transformation/             # Format transformers (JSON, Protobuf, XML, Avro)
│   ├── sinks/                      # Sinks: mock + live (REST, gRPC, MQ, DB)
│   ├── orchestrator/               # Fan-out engine, dispatchers, pipeline
│   ├── resilience/                 # Rate limiter, retry, circuit breaker, backpressure, DLQ
│   └── observability/              # Metrics, health checks, structured logging
├── tests/
│   ├── unit/                       # 11 unit test files
│   ├── integration/                # 3 integration test files
│   └── performance/                # 2 performance test files
├── Dockerfile                      # Multi-stage production build
├── docker-compose.yml              # Kafka + Cassandra + RabbitMQ + REST API
├── Makefile                        # Build, run, test commands
└── pyproject.toml                  # Python package config (PEP 621)
```

---

## Installation

```bash
# Basic (mock mode, file sources only)
pip install -e .

# With specific live integrations
pip install -e ".[kafka]"           # Kafka producer/consumer
pip install -e ".[rabbitmq]"        # RabbitMQ
pip install -e ".[cassandra]"       # Cassandra/ScyllaDB
pip install -e ".[dynamodb]"        # AWS DynamoDB
pip install -e ".[postgres-cdc]"    # PostgreSQL CDC
pip install -e ".[all]"             # Everything

# Dev (tests + linting + all integrations)
pip install -e ".[dev]"
```

### Docker

```bash
make docker-build       # Build image
make infra-up           # Start Kafka, Cassandra, RabbitMQ, REST API
make run-docker          # Run engine connected to local infra
make infra-down          # Stop infrastructure
```

The container exposes:
- **Port 8080** — Health check endpoint (`/health`)
- **Port 9090** — Prometheus metrics endpoint (`/metrics`)

---

## Commands

| Command | Description |
|---|---|
| `make run` | Process sample CSV through all 4 mock sinks |
| `make run-webhook` | Start webhook server on port 8082 |
| `make run-kafka-source` | Consume from Kafka topic |
| `make run-websocket` | Connect to WebSocket stream |
| `make run-postgres-cdc` | Stream PostgreSQL changes |
| `make run-docker` | Connect to docker-compose infrastructure |
| `make run-live` | Connect to real production systems |
| `make test` | Run all tests |
| `make test-unit` | Unit tests only |
| `make test-cov` | Tests with HTML coverage report |
| `make lint` | ruff + mypy |
| `make infra-up` | Start Kafka, Cassandra, RabbitMQ via Docker |
| `make infra-down` | Stop infrastructure |

---

## Testing

```bash
make test           # All tests (unit + integration + performance)
make test-unit      # Unit tests only
make test-cov       # With HTML coverage report
```

| Type | Count | What It Tests |
|---|---|---|
| Unit | 11 files | Readers, transformers, rate limiter, retry, circuit breaker, DLQ |
| Integration | 3 files | Full pipeline, multi-sink orchestrator, backpressure |
| Performance | 2 files | Throughput benchmarks, constant memory under large files |

---

## Design Decisions

### Concurrency Model — Python asyncio

Python's `asyncio` with bounded semaphores and async queues is ideal for this I/O-bound fan-out workload:
- Async I/O naturally models network-bound sink dispatch without thread-per-connection overhead.
- Bounded `asyncio.Queue` provides built-in backpressure — producers block when the queue is full, preventing OOM.
- Semaphores control per-sink concurrency without thread pool tuning.

### Backpressure

Enforced at two levels:
1. **Bounded async queues** (`max_queue_size: 10,000`) between ingestion and dispatch — if any sink falls behind, the ingestion pipeline pauses automatically.
2. **Per-sink concurrency semaphores** limit in-flight requests, preventing downstream overload.

### Resilience Stack

Each sink has a layered resilience pipeline:
- **Token-bucket rate limiter** with adaptive adjustment (reduce on failures, increase on success).
- **Exponential backoff retry** with jitter to avoid thundering herd.
- **Circuit breaker** (closed → open → half-open) to protect failing downstream services.
- **Dead Letter Queue** to guarantee zero data loss.

### Mock + Live Modes

Mock mode lets you develop, test, and demo the entire pipeline without any external infrastructure. Live mode connects to real systems with the same resilience guarantees. Switching is a single config field change (`mode: "mock"` → `mode: "live"`).

### Extensibility

Adding a new sink (e.g., Elasticsearch) requires:
1. Implement `BaseSink` (one class).
2. Register the sink type in `SinkFactory`.
3. Add a transformer if a new format is needed.

No changes to the core orchestrator, pipeline, or dispatcher are required.

---

## Design Patterns

| Pattern | Where | Purpose |
|---|---|---|
| Strategy | Transformers | Pluggable serialization (JSON/Protobuf/XML/Avro) |
| Factory | ReaderFactory, SinkFactory | Create sources/sinks from config |
| Registry | TransformerRegistry | Lookup transformers by name |
| Template Method | BaseSink | Lifecycle: connect → send → flush → close |
| Observer | MetricsCollector | Track events from sinks |
| Pipeline | Orchestrator | Ingestion → Transform → Dispatch |
| Circuit Breaker | Per-sink | Protect failing services |

---

## Observability

- **Status Reporter** — Rich-formatted table every 5 seconds: records processed, throughput (rec/s), success/failure per sink.
- **Structured Logging** — JSON logs via `structlog`, written to `output/engine.log`.
- **Prometheus Metrics** — Exposed on port 9090 for scraping.
- **Health Check** — HTTP endpoint on port 8080 at `/health`.
- **Final Summary** — Printed on completion with total records, duration, per-sink breakdown, DLQ counts.

---

## Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.11+ |
| Async I/O | asyncio, aiohttp, aiofiles |
| Config & Validation | Pydantic v2, PyYAML |
| Serialization | fastavro, protobuf, lxml |
| HTTP Client | httpx (HTTP/2) |
| Kafka | aiokafka |
| RabbitMQ | aio-pika |
| Cassandra | cassandra-driver |
| DynamoDB | aiobotocore |
| PostgreSQL CDC | psycopg3 (test_decoding) |
| Observability | structlog, prometheus-client, rich |
| CLI | Click |
| Testing | pytest, pytest-asyncio, pytest-cov |
| Code Quality | ruff, mypy (strict mode) |
| Containerization | Docker, Docker Compose |

---

## Assumptions

1. Input records are independent (no ordering dependency between records).
2. Each record is self-contained and can be transformed/sent independently.
3. Network conditions may be unreliable (hence retry, circuit breaker, DLQ).
4. Downstream services may have rate limits (hence token bucket).
5. File sources are UTF-8 encoded by default (configurable).
6. For CDC: PostgreSQL has `wal_level = logical` enabled.
7. Single-node execution — horizontal scaling across nodes is out of scope.

---

## License

MIT
