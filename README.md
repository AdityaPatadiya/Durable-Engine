# Durable Engine

A high-throughput Distributed Data Fan-Out & Transformation Engine built with Python 3.11+ and async I/O.

Durable Engine reads records from flat-file sources (CSV, JSONL, Fixed-width) and dispatches them in parallel to multiple heterogeneous sinks — each with its own serialization format, rate limit, and resilience policy. It is designed to process files of arbitrary size (100GB+) without loading them entirely into memory.

---

## Architecture

```
                        ┌─────────────────────────┐
                        │    Flat File Source       │
                        │  (CSV / JSONL / Fixed)    │
                        └────────────┬────────────┘
                                     │ streaming read
                                     ▼
                        ┌─────────────────────────┐
                        │   Ingestion Pipeline      │
                        │  (batch → async queues)   │
                        └────────────┬────────────┘
                                     │ fan-out
             ┌───────────┬───────────┼───────────┬────────────┐
             ▼           ▼           ▼           ▼            │
      ┌────────────┐┌────────────┐┌────────────┐┌────────────┐│
      │  JSON Xform ││ Proto Xform││  XML Xform ││ Avro Xform ││
      └──────┬─────┘└──────┬─────┘└──────┬─────┘└──────┬─────┘│
             │             │             │             │       │
      ┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐│
      │ Rate Limit  ││ Rate Limit  ││ Rate Limit  ││ Rate Limit ││
      │  + Retry    ││  + Retry    ││  + Retry    ││  + Retry   ││
      │  + Circuit  ││  + Circuit  ││  + Circuit  ││  + Circuit ││
      │   Breaker   ││   Breaker   ││   Breaker   ││   Breaker  ││
      └──────┬─────┘└──────┬─────┘└──────┬─────┘└──────┬─────┘│
             ▼             ▼             ▼             ▼       │
      ┌────────────┐┌────────────┐┌────────────┐┌────────────┐│
      │  REST Sink  ││  gRPC Sink ││   MQ Sink   ││ WideCol DB ││
      │ (HTTP POST) ││ (Streaming)││ (Kafka-like)││ (Cassandra)││
      └────────────┘└────────────┘└────────────┘└────────────┘│
                                                               │
                        Failed records ──────────────────────▶ DLQ
```

### Data Flow

1. **Ingestion** — A streaming reader processes the input file in chunks without loading it all into memory. Records are normalized into a canonical internal representation.
2. **Fan-Out** — Each record is placed into bounded async queues, one per sink. Backpressure prevents memory overflow if any sink is slow.
3. **Transformation** — Per-sink transformers convert records to the required wire format (JSON, Protobuf, XML, Avro/CQL Map) using the Strategy pattern.
4. **Dispatch** — Worker pools send transformed records to each sink concurrently, governed by per-sink rate limiters, retry policies, and circuit breakers.
5. **Dead Letter Queue** — Records that exhaust all retries are persisted to JSONL files with full error metadata for later reprocessing.

---

## Design Decisions

### Concurrency Model — Python asyncio

Instead of Java's `VirtualThreads` / `ForkJoinPool` (as suggested in the original assignment), this implementation uses **Python's `asyncio` with bounded semaphores and async queues**. This was chosen because:

- Async I/O naturally models the network-bound sink dispatch workload without thread-per-connection overhead.
- Bounded `asyncio.Queue` provides built-in backpressure — producers block when the queue is full, preventing OOM.
- Semaphores control per-sink concurrency without thread pool tuning.

### Backpressure

Backpressure is enforced at two levels:

1. **Bounded async queues** (`max_queue_size: 10,000`) between ingestion and dispatch — if any sink falls behind, the ingestion pipeline pauses automatically.
2. **Per-sink concurrency semaphores** limit the number of in-flight requests, preventing downstream services from being overwhelmed.

### Resilience Stack

Each sink has a layered resilience pipeline:

- **Token-bucket rate limiter** — configurable requests/sec with burst capacity.
- **Exponential backoff retry** — up to 3 retries with jitter to avoid thundering herd.
- **Circuit breaker** — trips open after N consecutive failures, enters half-open state after a recovery timeout, and fully closes on successful probe calls.
- **Dead Letter Queue** — failed records are written to per-sink JSONL files with error metadata.

### Extensibility

Adding a new sink (e.g., Elasticsearch) requires:
1. Implement `BaseSink` (one class).
2. Register the sink type in `SinkFactory`.
3. Add a transformer if a new format is needed.

No changes to the core orchestrator, pipeline, or dispatcher are required — the engine is fully config-driven.

---

## Project Structure

```
.
├── config/
│   ├── default.yaml              # Default configuration (4 sinks)
│   ├── production.yaml           # Production overrides
│   └── schema.json               # Config JSON schema
├── src/durable_engine/
│   ├── __main__.py               # CLI entrypoint (Click)
│   ├── app.py                    # Application bootstrap
│   ├── config/                   # Configuration loader & Pydantic models
│   ├── ingestion/                # Streaming file readers (CSV, JSONL, Fixed-width)
│   ├── transformation/           # Format transformers (JSON, Protobuf, XML, Avro)
│   ├── sinks/                    # Mock sinks (REST, gRPC, MQ, Wide-column DB)
│   ├── orchestrator/             # Fan-out engine, dispatchers, pipeline
│   ├── resilience/               # Rate limiter, retry, circuit breaker, DLQ
│   └── observability/            # Metrics, health checks, structured logging
├── tests/
│   ├── unit/                     # Unit tests for each component
│   ├── integration/              # End-to-end pipeline tests
│   └── performance/              # Throughput & memory benchmarks
├── proto/                        # Protocol Buffer definitions
├── samples/                      # Sample input files
├── docs/                         # Design diagrams
├── Dockerfile                    # Multi-stage production build
├── docker-compose.yml            # Container orchestration
├── Makefile                      # Build & run commands
└── pyproject.toml                # Python package config (PEP 621)
```

---

## Setup Instructions

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (optional, for containerized runs)

### Local Setup

```bash
# Clone the repository
git clone <repo-url> && cd durable-engine

# Install with dev dependencies
make dev

# Compile Protocol Buffers (if modifying .proto files)
make proto
```

### Running the Engine

```bash
# Run with default config and sample data
make run

# Run with a custom config and input file
python -m durable_engine --config config/default.yaml --input samples/sample.csv

# Run with production config
make run-prod
```

### Docker

```bash
# Build the Docker image
make docker-build

# Start the engine
make docker-up

# Stop
make docker-down
```

The container exposes:
- **Port 8080** — Health check endpoint (`/health`)
- **Port 9090** — Prometheus metrics endpoint (`/metrics`)

---

## Configuration

All behavior is driven by `config/default.yaml`. Key sections:

| Section | Description |
|---------|-------------|
| `engine` | Batch size (500), queue size (10k), worker count (4) |
| `ingestion` | File path, format (auto-detect), checkpoint settings |
| `sinks.*` | Per-sink: endpoint, transformer, rate limit, retry, circuit breaker |
| `dlq` | Dead letter queue output directory and size limits |
| `observability` | Metrics (Prometheus), health check, structured logging |

### Default Sink Configuration

| Sink | Format | Rate Limit | Retry | Concurrency |
|------|--------|------------|-------|-------------|
| REST API | JSON | 50 req/s | 3 retries, 100ms base | 10 workers |
| gRPC | Protobuf | 200 req/s | 3 retries, 50ms base | 20 workers |
| Message Queue | XML | 500 req/s | 3 retries, 200ms base | 15 workers |
| Wide-Column DB | Avro | 1000 req/s | 3 retries, 50ms base | 30 workers |

---

## Testing

```bash
# Run all tests
make test

# Unit tests only
make test-unit

# Integration tests
make test-integration

# Performance / throughput benchmarks
make test-perf

# Coverage report (HTML)
make test-cov
```

---

## Observability

- **Status Reporter** — Prints a rich-formatted status update every 5 seconds showing records processed, throughput (records/sec), and success/failure counts per sink.
- **Structured Logging** — JSON-formatted logs written to `output/engine.log`.
- **Prometheus Metrics** — Exposed on port 9090 for scraping.
- **Health Check** — HTTP endpoint on port 8080 at `/health`.

---

## Assumptions

1. **Mock sinks** — All sinks simulate downstream behavior with configurable latency, jitter, and error rates. No real external services are required.
2. **File encoding** — Input files are assumed to be UTF-8 encoded.
3. **Record identity** — Each record is assigned a unique ID during ingestion for tracking through the pipeline and DLQ.
4. **Network conditions** — Simulated via configurable error rates and latency in each sink's `simulation` block. Transient errors (retryable) are distinguished from permanent failures.
5. **Single-node execution** — The engine runs on a single machine; horizontal scaling across nodes is out of scope.

---

## Tech Stack

| Category | Technology |
|----------|------------|
| Language | Python 3.11+ |
| Async I/O | asyncio, aiohttp, aiofiles |
| Config & Validation | Pydantic v2, PyYAML |
| Serialization | fastavro, protobuf, lxml |
| Observability | structlog, prometheus-client, rich |
| CLI | Click |
| Testing | pytest, pytest-asyncio, pytest-cov |
| Code Quality | ruff, mypy (strict mode) |
| Containerization | Docker, Docker Compose |

---

## License

MIT
