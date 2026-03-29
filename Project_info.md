# Durable Engine — Project Info

## High-Throughput Distributed Data Fan-Out & Transformation Engine

A production-grade Python engine that reads records from any data source (files, Kafka, webhooks, PostgreSQL CDC, WebSocket) and fans them out to multiple downstream systems simultaneously — each in the format it expects — with full resilience, backpressure, and observability.

---

## Project Structure

```
durable-engine/
├── pyproject.toml                          # Project metadata, dependencies (PEP 621)
├── Makefile                                # Build, run, test, docker commands
├── Dockerfile                              # Multi-stage Docker build
├── docker-compose.yml                      # Full local infra (Kafka, Cassandra, RabbitMQ, REST API)
│
├── config/
│   ├── default.yaml                        # Default config (mock mode, file source)
│   ├── docker.yaml                         # Connects to docker-compose local infra
│   ├── live.yaml                           # Production template with real endpoints + auth
│   ├── production.yaml                     # Production overrides (tuned batch sizes)
│   ├── source-kafka.yaml                   # Kafka consumer source config
│   ├── source-webhook.yaml                 # HTTP webhook source config
│   ├── source-postgres-cdc.yaml            # PostgreSQL CDC source config
│   ├── source-websocket.yaml               # WebSocket source config
│   └── schema.json                         # JSON Schema for config validation
│
├── proto/
│   └── record.proto                        # Protobuf schema for gRPC sink
│
├── samples/
│   ├── sample.csv                          # 10-record CSV test data
│   ├── sample.jsonl                        # 10-record JSONL test data
│   └── sample.fixed_width.txt              # 10-record fixed-width test data
│
├── src/durable_engine/
│   ├── __init__.py                         # Package init, version
│   ├── __main__.py                         # CLI entrypoint (click)
│   ├── app.py                              # Application bootstrap & orchestrator wiring
│   │
│   ├── config/
│   │   ├── loader.py                       # YAML loader with ${ENV_VAR} interpolation
│   │   ├── models.py                       # Pydantic config models (validated, typed)
│   │   └── schema.py                       # Runtime config validation
│   │
│   ├── ingestion/                          # --- Data Sources ---
│   │   ├── base.py                         # RecordSource (ABC) + FileReader (ABC)
│   │   ├── record.py                       # Canonical Record dataclass
│   │   ├── csv_reader.py                   # Streaming CSV reader
│   │   ├── jsonl_reader.py                 # Streaming JSONL reader
│   │   ├── fixed_width.py                  # Streaming fixed-width reader
│   │   ├── kafka_source.py                 # Live Kafka consumer (aiokafka)
│   │   ├── webhook_source.py               # Live HTTP webhook server (aiohttp)
│   │   ├── postgres_cdc_source.py          # Live PostgreSQL CDC (psycopg3, test_decoding)
│   │   ├── websocket_source.py             # Live WebSocket client (aiohttp)
│   │   └── reader_factory.py               # Factory — creates source from config
│   │
│   ├── transformation/                     # --- Format Converters (Strategy Pattern) ---
│   │   ├── base.py                         # Abstract Transformer interface
│   │   ├── json_transformer.py             # Record → JSON bytes (REST sinks)
│   │   ├── protobuf_transformer.py         # Record → Protobuf bytes (gRPC sinks)
│   │   ├── xml_transformer.py              # Record → XML bytes (MQ sinks)
│   │   ├── avro_transformer.py             # Record → Avro bytes (DB sinks)
│   │   └── transformer_registry.py         # Registry — lookup transformer by name
│   │
│   ├── sinks/                              # --- Output Destinations (Template Method) ---
│   │   ├── base.py                         # BaseSink ABC + SinkResult
│   │   ├── rest_sink.py                    # REST API (mock + live httpx HTTP/2)
│   │   ├── grpc_sink.py                    # gRPC (mock + live grpcio async)
│   │   ├── mq_sink.py                      # MQ (mock + live Kafka/RabbitMQ)
│   │   ├── widecolumn_sink.py              # DB (mock + live Cassandra/DynamoDB)
│   │   └── sink_factory.py                 # Factory — creates sinks from config
│   │
│   ├── resilience/                         # --- Fault Tolerance ---
│   │   ├── rate_limiter.py                 # Token bucket with adaptive rate
│   │   ├── retry.py                        # Exponential backoff + jitter (max 3)
│   │   ├── circuit_breaker.py              # Closed → Open → Half-Open states
│   │   ├── backpressure.py                 # Bounded async queue
│   │   └── dlq.py                          # Dead Letter Queue (JSONL files)
│   │
│   ├── orchestrator/                       # --- Core Engine ---
│   │   ├── engine.py                       # FanOutEngine — main orchestrator
│   │   ├── dispatcher.py                   # Per-sink dispatch with full resilience
│   │   └── pipeline.py                     # Source → fan-out → dispatchers
│   │
│   ├── observability/                      # --- Monitoring ---
│   │   ├── metrics.py                      # In-memory counters (success/failure/throughput)
│   │   ├── reporter.py                     # Periodic console/JSON reporter (every 5s)
│   │   ├── health.py                       # HTTP health check endpoint
│   │   └── structured_log.py               # structlog JSON logging setup
│   │
│   └── utils/
│       ├── async_helpers.py                # Async utilities, graceful shutdown
│       └── file_utils.py                   # File detection, encoding, validation
│
└── tests/
    ├── conftest.py                         # Shared fixtures
    ├── unit/                               # 11 unit test files
    │   ├── test_csv_reader.py
    │   ├── test_jsonl_reader.py
    │   ├── test_fixed_width_reader.py
    │   ├── test_json_transformer.py
    │   ├── test_protobuf_transformer.py
    │   ├── test_xml_transformer.py
    │   ├── test_avro_transformer.py
    │   ├── test_rate_limiter.py
    │   ├── test_retry.py
    │   ├── test_circuit_breaker.py
    │   └── test_dlq.py
    ├── integration/                        # 3 integration test files
    │   ├── test_pipeline.py
    │   ├── test_orchestrator.py
    │   └── test_backpressure.py
    └── performance/                        # 2 performance test files
        ├── test_throughput.py
        └── test_memory.py
```

---

## How The Engine Works — Step by Step

### The Core Idea

```
ONE Data Source  ──►  Engine  ──►  MANY Destinations (simultaneously)
```

Data comes in from one place and gets sent to multiple systems at the same time, each in the format it expects.

### Data Flow Diagram

```
                         ┌──────────────┐
                         │  Config YAML │
                         └──────┬───────┘
                                │
                                ▼
┌──────────────────┐    ┌──────────────┐    ┌─────────────────┐
│ CSV / JSONL /    │    │              │    │  Backpressure   │
│ Kafka / Webhook /│───►│  Ingestion   │───►│  Queue (bounded)│
│ CDC / WebSocket  │    │  (async)     │    │  per sink       │
└──────────────────┘    └──────────────┘    └────────┬────────┘
                                                     │
                     ┌───────────────────────────────┤
                     │              │                │               │
              ┌──────▼──────┐┌─────▼───────┐┌──────▼──────┐┌──────▼──────┐
              │ Transform   ││ Transform   ││ Transform   ││ Transform   │
              │ → JSON      ││ → Protobuf  ││ → XML       ││ → Avro      │
              └──────┬──────┘└──────┬──────┘└──────┬──────┘└──────┬──────┘
                     │              │               │              │
              ┌──────▼──────┐┌─────▼───────┐┌─────▼───────┐┌─────▼──────┐
              │ Rate Limit  ││ Rate Limit  ││ Rate Limit  ││ Rate Limit │
              │ + Retry     ││ + Retry     ││ + Retry     ││ + Retry    │
              │ + Circuit   ││ + Circuit   ││ + Circuit   ││ + Circuit  │
              └──────┬──────┘└──────┬──────┘└──────┬──────┘└──────┬─────┘
                     │              │               │              │
              ┌──────▼──────┐┌─────▼───────┐┌─────▼───────┐┌─────▼──────┐
              │  REST Sink  ││  gRPC Sink  ││   MQ Sink   ││  DB Sink   │
              │  (HTTP/2)   ││ (streaming) ││(Kafka/AMQP) ││ (UPSERT)   │
              └─────────────┘└─────────────┘└─────────────┘└────────────┘
                     │              │               │              │
                     └──────────────┴───────┬───────┴──────────────┘
                                            │
                                 ┌──────────▼──────────┐
                                 │   DLQ (failed)      │
                                 │   Metrics Reporter  │
                                 └─────────────────────┘
```

### STEP 1: Ingestion (Read Data In)

| Source | Type | How It Works | Command |
|---|---|---|---|
| CSV / JSONL / Fixed-width | File (finite) | Streams line-by-line, constant memory for 100GB+ | `make run` |
| Kafka Consumer | Live (infinite) | Subscribes to topic, consumes messages in real-time | `make run-kafka-source` |
| HTTP Webhook | Live (infinite) | Starts HTTP server on port 8082, receives POST | `make run-webhook` |
| PostgreSQL CDC | Live (infinite) | Watches table for INSERT/UPDATE/DELETE via logical replication | `make run-postgres-cdc` |
| WebSocket | Live (infinite) | Connects to WebSocket stream, auto-reconnects | `make run-websocket` |

All sources produce the same thing: a stream of `Record` objects (a dict with metadata).

### STEP 2: Backpressure Queue

Records go into a bounded `asyncio.Queue` per sink. If a sink is slow, the queue fills up and the producer **pauses** instead of crashing with OOM.

```
Producer (fast) ──► [Queue: ████████░░] ──► Consumer (slow)
                     ▲ blocks when full
```

### STEP 3: Transform (Convert Format Per Sink)

Each sink needs data in a different format. Transformers convert automatically:

```
Record (Python dict)
    ├──► JSON bytes      → for REST API sink
    ├──► Protobuf bytes  → for gRPC sink
    ├──► XML bytes       → for Message Queue sink
    └──► Avro bytes      → for Wide-Column DB sink
```

### STEP 4: Send to Sinks (With Full Resilience)

Each record goes through this pipeline per sink:

```
Record
  │
  ▼
Rate Limiter ──► "Only 50 req/sec allowed"
  │
  ▼
Circuit Breaker ──► "Target is down, stop for 30s"
  │
  ▼
Send to target
  │
  ├── Success ──► Count it, move on
  │
  └── Failure ──► Retry up to 3x with exponential backoff + jitter
                    │
                    └── Still failing? ──► Dead Letter Queue (no data lost)
```

### STEP 5: Observability

While running, the engine:
- Prints a **Rich status table every 5 seconds** (throughput, success/failure per sink)
- Writes **structured JSON logs** to `output/engine.log`
- Exposes a **health endpoint** at `http://localhost:8080/health`
- Prints a **final summary** on completion

---

## Sink Modes: Mock vs Live

Every sink supports two modes controlled by `mode` in config:

| Mode | What It Does | When To Use |
|---|---|---|
| `mock` | Simulates the target with configurable latency/error rates | Development, testing, demos |
| `live` | Connects to real external systems with auth and TLS | Production, staging |

### Supported Live Integrations

| Sink Type | Mock | Live Client | Library |
|---|---|---|---|
| REST API | Simulated HTTP | Real HTTP/2 with auth/TLS | `httpx[http2]` |
| gRPC | Simulated RPC | Real `grpc.aio` channel | `grpcio` |
| Kafka | Simulated publish | Real Kafka producer | `aiokafka` |
| RabbitMQ | Simulated publish | Real AMQP publisher | `aio-pika` |
| Cassandra/ScyllaDB | Simulated UPSERT | Real CQL driver | `cassandra-driver` |
| DynamoDB | Simulated PutItem | Real AWS client | `aiobotocore` |

---

## Features & Functionalities

### 1. Ingestion Layer

| Feature | Details |
|---|---|
| Streaming reads | Generator-based — never loads full file. Constant memory for 100GB+ |
| Multi-format files | CSV, JSONL, Fixed-width with auto-detection by extension |
| Live sources | Kafka consumer, HTTP webhook, PostgreSQL CDC, WebSocket |
| Chunked batching | Configurable batch size (default 500) to amortize dispatch overhead |
| File validation | Pre-flight checks — file exists, readable, encoding detection |

### 2. Transformation Layer (Strategy Pattern)

| Feature | Details |
|---|---|
| Strategy pattern | Each transformer implements `transform(record) → bytes` |
| Transformer registry | Register/lookup transformers by name from config |
| JSON transformer | `dict → JSON bytes` for REST API sinks |
| Protobuf transformer | `dict → Protobuf wire format bytes` for gRPC sinks |
| XML transformer | `dict → XML bytes` with configurable root/row tags |
| Avro transformer | `dict → Avro binary` using fastavro, or CQL Map format |

### 3. Distribution Layer (Sinks)

| Feature | Details |
|---|---|
| REST API Sink | Mock or live HTTP/2 POST via `httpx.AsyncClient` |
| gRPC Sink | Mock or live `grpc.aio` unary/streaming calls |
| Message Queue Sink | Mock or live Kafka (`aiokafka`) / RabbitMQ (`aio-pika`) |
| Wide-Column DB Sink | Mock or live Cassandra (`cassandra-driver`) / DynamoDB (`aiobotocore`) |
| Sink lifecycle | `connect() → send() → flush() → close()` with async context managers |
| Failure simulation | Configurable per-sink: error rate %, transient vs permanent, latency jitter |

### 4. Throttling & Resilience

| Feature | Details |
|---|---|
| Token bucket rate limiter | Per-sink configurable rate with burst allowance |
| Adaptive rate limiting | Reduce rate on failures, increase on success streaks |
| Backpressure | `asyncio.Queue(maxsize=N)` — producer blocks when full |
| Retry with backoff | Exponential backoff + jitter, max 3 retries per record |
| Circuit breaker | Opens after N failures, half-open probe after timeout |
| Dead Letter Queue | Failed records written to `dlq/<sink>.jsonl` with error metadata |
| Graceful shutdown | SIGINT/SIGTERM → drain queues, flush sinks, exit cleanly |

### 5. Concurrency Model

| Feature | Details |
|---|---|
| asyncio event loop | Single event loop with async/await |
| Per-sink worker tasks | Each sink gets N async workers consuming from its own queue |
| Semaphore concurrency | `asyncio.Semaphore` caps concurrent in-flight requests per sink |
| Linear scaling | Worker count configurable, scales with CPU cores |

### 6. Observability

| Feature | Details |
|---|---|
| Periodic reporter | Every 5s: records processed, throughput (rec/s), success/failure per sink |
| Structured JSON logging | `structlog` with sink name, record ID in every log line |
| Prometheus metrics | Optional `/metrics` endpoint via `prometheus_client` |
| Health check endpoint | HTTP server returns sink states at `/health` |
| Run summary report | On completion: total records, duration, per-sink breakdown, DLQ counts |

### 7. Configuration

| Feature | Details |
|---|---|
| YAML config | Validated with Pydantic models for type safety |
| Environment variables | `${ENV_VAR}` or `${ENV_VAR:default}` interpolation |
| Per-sink config | Rate limit, retry, batch size, concurrency, timeout, auth, TLS |
| Auth + TLS | Bearer, Basic, API key, mTLS — per sink |
| Multiple config files | `default.yaml`, `docker.yaml`, `live.yaml`, source-specific configs |

### 8. Testing

| Type | Files | What It Tests |
|---|---|---|
| Unit (11 files) | `tests/unit/` | Readers, transformers, rate limiter, retry, circuit breaker, DLQ |
| Integration (3 files) | `tests/integration/` | Full pipeline, orchestrator with mock sinks, backpressure |
| Performance (2 files) | `tests/performance/` | Throughput benchmarks, constant memory verification |

---

## Design Patterns Used

| Pattern | Where | Purpose |
|---|---|---|
| Strategy | Transformers | Pluggable serialization per sink (JSON/Protobuf/XML/Avro) |
| Factory | ReaderFactory, SinkFactory | Create sources/sinks from config |
| Registry | TransformerRegistry | Lookup transformers by name |
| Template Method | BaseSink | Lifecycle: connect → send → flush → close |
| Observer | MetricsCollector | Track success/failure events from sinks |
| Pipeline | Orchestrator | Ingestion → Transform → Dispatch chain |
| Circuit Breaker | Per-sink resilience | Protect failing downstream services |

---

## Key Python Libraries

| Library | Purpose |
|---|---|
| `asyncio` | Core async runtime (event loop, tasks, queues) |
| `pydantic` | Config and record validation |
| `structlog` | Structured JSON logging |
| `pyyaml` | YAML config loading |
| `fastavro` | Avro binary serialization |
| `protobuf` / `grpcio` | Protobuf encoding and gRPC client |
| `lxml` | XML generation |
| `httpx[http2]` | Async HTTP/2 client (REST sink) |
| `aiokafka` | Async Kafka producer/consumer |
| `aio-pika` | Async RabbitMQ (AMQP) client |
| `cassandra-driver` | Cassandra/ScyllaDB CQL driver |
| `aiobotocore` | Async AWS SDK (DynamoDB) |
| `psycopg` | Async PostgreSQL (CDC source) |
| `aiohttp` | Webhook server + WebSocket client |
| `rich` | Beautiful console table output |
| `click` | CLI argument parsing |
| `prometheus-client` | Prometheus metrics export |
| `pytest` / `pytest-asyncio` | Testing framework |

---

## How To Use With Other Projects / Systems

### Scenario 1: Your App Sends Data TO the Engine (Webhook)

```
Your App ──POST──► Durable Engine ──► Kafka + Cassandra + REST API
```

```bash
make run-webhook

curl -X POST http://localhost:8082/ingest \
  -H "Content-Type: application/json" \
  -d '{"user_id": 123, "event": "purchase", "amount": 49.99}'
```

### Scenario 2: Engine Reads From Your Kafka

```
Your App ──► Kafka Topic ──► Durable Engine ──► REST API + gRPC + Cassandra
```

```yaml
ingestion:
  source_type: "kafka"
  kafka:
    brokers: "your-kafka:9092"
    topic: "your-events-topic"
```

### Scenario 3: Engine Watches Your PostgreSQL (CDC)

```
Your App ──writes──► PostgreSQL ──CDC──► Durable Engine ──► Kafka + REST + Cache
```

```yaml
ingestion:
  source_type: "postgres_cdc"
  postgres_cdc:
    dsn: "postgresql://user:pass@your-db:5432/your_database"
    publication: "my_publication"
```

### Scenario 4: Engine Sends To Your Real Services

Switch any sink from `mode: "mock"` to `mode: "live"`:

```yaml
sinks:
  my_api:
    mode: "live"
    type: "rest"
    endpoint: "https://api.your-company.com"
    http_path: "/v1/events"
    auth:
      type: "bearer"
      token: "${YOUR_API_TOKEN}"
```

### Scenario 5: Consume a Live WebSocket API

```yaml
ingestion:
  source_type: "websocket"
  websocket:
    url: "wss://stream.binance.com:9443/ws/btcusdt@trade"
```

---

## Real-World Architecture Examples

### E-Commerce Event Pipeline

```
User clicks "Buy"
    │
    ▼
Your Backend ──POST──► Durable Engine
                            ├──► Kafka (for analytics team)
                            ├──► Elasticsearch (for search/dashboard)
                            ├──► Cassandra (for order history)
                            └──► REST API (for notification service)
```

### Database Sync / Migration

```
PostgreSQL (source of truth)
    │ CDC
    ▼
Durable Engine
    ├──► New Microservice API (REST)
    ├──► Data Warehouse (Kafka → Spark)
    └──► Search Index (Elasticsearch)
```

### IoT Data Pipeline

```
IoT Sensors ──WebSocket──► Durable Engine
                              ├──► Time-series DB
                              ├──► Alert Service (REST)
                              └──► Kafka (for ML pipeline)
```

---

## Installation & Running

```bash
# Basic install (mock mode, file sources)
pip install -e .

# Install with specific live integrations
pip install -e ".[kafka]"           # Kafka producer/consumer
pip install -e ".[rabbitmq]"        # RabbitMQ
pip install -e ".[cassandra]"       # Cassandra/ScyllaDB
pip install -e ".[dynamodb]"        # AWS DynamoDB
pip install -e ".[postgres-cdc]"    # PostgreSQL CDC
pip install -e ".[all]"             # Everything

# Dev install (includes test tools + all integrations)
pip install -e ".[dev]"
```

### Run Commands

| Command | What It Does |
|---|---|
| `make run` | Process `samples/sample.csv` through all 4 mock sinks |
| `make run-webhook` | Start webhook server, receive data via HTTP POST |
| `make run-kafka-source` | Consume from Kafka topic |
| `make run-websocket` | Connect to WebSocket stream (Binance by default) |
| `make run-postgres-cdc` | Stream PostgreSQL table changes |
| `make run-docker` | Connect to local docker-compose infrastructure |
| `make run-live` | Connect to real production systems |
| `make test` | Run all tests (unit + integration + performance) |
| `make test-unit` | Run unit tests only |
| `make test-cov` | Run tests with HTML coverage report |
| `make lint` | Run ruff + mypy |
| `make infra-up` | Start local Kafka, Cassandra, RabbitMQ via Docker |
| `make infra-down` | Stop local infrastructure |

---

## What Makes It "Industry Level"

| Feature | Why It Matters |
|---|---|
| Streaming ingestion | Handles 100GB+ files without crashing (constant memory) |
| 5 source types | File, Kafka, Webhook, PostgreSQL CDC, WebSocket |
| Fan-out to 4+ sinks | One record → many destinations simultaneously |
| Mock + Live modes | Develop locally, deploy to production with config change |
| Rate limiting | Token bucket prevents overwhelming downstream services |
| Retry + exponential backoff | Handles temporary failures gracefully (max 3 retries) |
| Circuit breaker | Stops hammering a dead service, auto-recovers |
| Dead Letter Queue | Zero data loss — failed records saved to disk |
| Backpressure | Slow sinks don't cause OOM crashes |
| Auth + TLS | Bearer, Basic, API key, mTLS — per sink |
| Config-driven | Change everything via YAML, no code changes |
| Extensible | Add new sink/source types without touching core engine |
| Observable | Real-time metrics table, structured logs, health endpoint |
| Docker-ready | docker-compose with Kafka, Cassandra, RabbitMQ |
| Tested | 16 test files — unit, integration, performance |
