---
  Project Structure

  durable-engine/
  ├── pyproject.toml                  # Project metadata & dependencies (PEP 621)
  ├── Makefile                        # Common commands (run, test, lint, docker)
  ├── Dockerfile
  ├── docker-compose.yml
  ├── config/
  │   ├── default.yaml                # Default configuration
  │   ├── production.yaml             # Production overrides
  │   └── schema.json                 # JSON Schema for config validation
  ├── proto/
  │   └── record.proto                # Protobuf definitions for gRPC sink
  ├── samples/
  │   ├── sample.csv
  │   ├── sample.jsonl
  │   └── sample.fixed_width.txt
  ├── docs/
  │   ├── architecture.md
  │   ├── design_decisions.md
  │   └── diagrams/
  │       └── data_flow.png
  ├── src/
  │   └── durable_engine/
  │       ├── __init__.py
  │       ├── __main__.py             # CLI entrypoint
  │       ├── app.py                  # Application bootstrap & orchestrator
  │       │
  │       ├── config/
  │       │   ├── __init__.py
  │       │   ├── loader.py           # YAML config loader with env var interpolation
  │       │   ├── models.py           # Pydantic config models (validated, typed)
  │       │   └── schema.py           # Runtime config validation
  │       │
  │       ├── ingestion/
  │       │   ├── __init__.py
  │       │   ├── base.py             # Abstract FileReader (ABC)
  │       │   ├── csv_reader.py       # Streaming CSV reader (chunked)
  │       │   ├── jsonl_reader.py     # Streaming JSONL reader (line-by-line)
  │       │   ├── fixed_width.py      # Fixed-width format reader
  │       │   ├── reader_factory.py   # Factory pattern — picks reader by file ext
  │       │   └── record.py           # Internal canonical Record dataclass
  │       │
  │       ├── transformation/
  │       │   ├── __init__.py
  │       │   ├── base.py             # Abstract Transformer (Strategy interface)
  │       │   ├── json_transformer.py       # Record → JSON (REST sink)
  │       │   ├── protobuf_transformer.py   # Record → Protobuf (gRPC sink)
  │       │   ├── xml_transformer.py        # Record → XML (MQ sink)
  │       │   ├── avro_transformer.py       # Record → Avro/CQL Map (DB sink)
  │       │   └── transformer_registry.py   # Registry pattern — map sink→transformer
  │       │
  │       ├── sinks/
  │       │   ├── __init__.py
  │       │   ├── base.py             # Abstract Sink (ABC) with lifecycle hooks
  │       │   ├── rest_sink.py        # Mock HTTP/2 POST sink
  │       │   ├── grpc_sink.py        # Mock gRPC streaming sink
  │       │   ├── mq_sink.py          # Mock Kafka/RabbitMQ publisher sink
  │       │   ├── widecolumn_sink.py  # Mock Cassandra UPSERT sink
  │       │   └── sink_factory.py     # Factory to instantiate sinks from config
  │       │
  │       ├── resilience/
  │       │   ├── __init__.py
  │       │   ├── rate_limiter.py     # Token bucket / sliding window rate limiter
  │       │   ├── retry.py            # Retry with exponential backoff + jitter
  │       │   ├── circuit_breaker.py  # Circuit breaker (closed/open/half-open)
  │       │   ├── backpressure.py     # Bounded async queue with backpressure
  │       │   └── dlq.py              # Dead Letter Queue — persist failed records
  │       │
  │       ├── orchestrator/
  │       │   ├── __init__.py
  │       │   ├── engine.py           # Core fan-out engine (async pipeline)
  │       │   ├── dispatcher.py       # Dispatches records to sink workers
  │       │   └── pipeline.py         # Ingestion → Transform → Sink pipeline wiring
  │       │
  │       ├── observability/
  │       │   ├── __init__.py
  │       │   ├── metrics.py          # In-memory metrics collector (counters, gauges)
  │       │   ├── reporter.py         # Periodic console/log reporter (every 5s)
  │       │   ├── health.py           # Health check endpoint (optional HTTP)
  │       │   └── structured_log.py   # Structured JSON logging setup
  │       │
  │       └── utils/
  │           ├── __init__.py
  │           ├── async_helpers.py    # Async utilities, graceful shutdown
  │           └── file_utils.py       # File detection, size estimation
  │
  └── tests/
      ├── conftest.py                 # Shared fixtures
      ├── unit/
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
      ├── integration/
      │   ├── test_pipeline.py        # End-to-end pipeline test
      │   ├── test_orchestrator.py    # Orchestrator with mock sinks
      │   └── test_backpressure.py    # Backpressure under slow sinks
      └── performance/
          ├── test_throughput.py      # Benchmark records/sec
          └── test_memory.py          # Verify constant memory under large files

  ---
  Features & Functionalities (Industry-Grade)

  1. Ingestion Layer

  ┌──────────────────────┬───────────────────────────────────────────────────────────────────────────────────┐
  │       Feature        │                                      Details                                      │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ Streaming file reads │ Generator-based reading — never loads full file. Constant memory for 100GB+ files │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ Multi-format support │ CSV, JSONL, Fixed-width with auto-detection by extension                          │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ Schema inference     │ Auto-detect column types from first N rows                                        │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ Chunked batching     │ Configurable batch size (e.g., 500 records) to amortize dispatch overhead         │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ File validation      │ Pre-flight checks — file exists, readable, encoding detection (UTF-8/Latin-1)     │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ Resumable reads      │ Track byte offset; on restart, skip already-processed bytes (checkpoint file)     │
  └──────────────────────┴───────────────────────────────────────────────────────────────────────────────────┘

  2. Transformation Layer (Strategy Pattern)

  ┌──────────────────────┬───────────────────────────────────────────────────────────────────────┐
  │       Feature        │                                Details                                │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Strategy pattern     │ Each transformer implements transform(record) → bytes                 │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Transformer registry │ Register transformers by name; sinks reference them by config key     │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ JSON transformer     │ dict → JSON bytes with configurable field mapping/renaming            │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Protobuf transformer │ dict → compiled .proto message using google.protobuf                  │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ XML transformer      │ dict → XML element with configurable root/row tags, namespace support │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Avro transformer     │ dict → Avro binary using fastavro, or CQL-style Map<str, str>         │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Field filtering      │ Include/exclude fields per sink via config                            │
  ├──────────────────────┼───────────────────────────────────────────────────────────────────────┤
  │ Data validation      │ Pydantic model validation before transformation; malformed → DLQ      │
  └──────────────────────┴───────────────────────────────────────────────────────────────────────┘

  3. Distribution Layer (Mock Sinks)

  ┌─────────────────────┬────────────────────────────────────────────────────────────────────────────────────┐
  │       Feature       │                                      Details                                       │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
  │ REST API Sink       │ Simulates HTTP/2 POST with httpx.AsyncClient; configurable latency/failure rate    │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
  │ gRPC Sink           │ Simulates unary/streaming calls with grpcio; mock channel with latency             │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
  │ Message Queue Sink  │ Simulates Kafka produce with partitioning logic and ack simulation                 │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
  │ Wide-Column DB Sink │ Simulates async UPSERT with configurable consistency level latency                 │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
  │ Sink lifecycle      │ connect() → send_batch() → flush() → close() with async context managers           │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
  │ Failure simulation  │ Configurable per-sink: error rate %, transient vs permanent errors, latency jitter │
  └─────────────────────┴────────────────────────────────────────────────────────────────────────────────────┘

  4. Throttling & Resilience

  ┌───────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────┐
  │          Feature          │                                         Details                                         │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Token bucket rate limiter │ Per-sink configurable rate (e.g., REST: 50 rps, DB: 1000 rps), burst allowance          │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Adaptive rate limiting    │ Dynamically reduce rate on repeated failures, increase on success streaks               │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Backpressure              │ asyncio.Queue(maxsize=N) — producer blocks when queue is full                           │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Retry with backoff        │ Exponential backoff + jitter, max 3 retries per record                                  │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Circuit breaker           │ Per-sink: opens after N consecutive failures, half-open probe after timeout             │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Dead Letter Queue (DLQ)   │ Failed records (after all retries) written to dlq/<sink_name>.jsonl with error metadata │
  ├───────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
  │ Graceful shutdown         │ SIGINT/SIGTERM → drain queues, flush sinks, persist checkpoints                         │
  └───────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────┘

  5. Concurrency Model

  ┌─────────────────────────────┬─────────────────────────────────────────────────────────────────────────────┐
  │           Feature           │                                   Details                                   │
  ├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
  │ asyncio event loop          │ Single event loop with async/await — Python's equivalent of virtual threads │
  ├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
  │ Per-sink worker tasks       │ Each sink gets N async worker tasks consuming from its own queue            │
  ├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
  │ CPU-bound offloading        │ Protobuf/Avro serialization offloaded to ProcessPoolExecutor                │
  ├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
  │ Semaphore-based concurrency │ asyncio.Semaphore to cap concurrent in-flight requests per sink             │
  ├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
  │ Linear scaling              │ Worker count = os.cpu_count() for CPU tasks; configurable for I/O tasks     │
  └─────────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘

  6. Observability

  ┌─────────────────────────┬───────────────────────────────────────────────────────────────────────────┐
  │         Feature         │                                  Details                                  │
  ├─────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ Periodic reporter       │ Every 5s: records processed, throughput (rec/s), success/failure per sink │
  ├─────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ Structured JSON logging │ structlog with correlation IDs, sink name, record ID in every log line    │
  ├─────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ Prometheus metrics      │ Optional: expose /metrics endpoint via prometheus_client                  │
  ├─────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ Health check endpoint   │ Lightweight aiohttp server on configurable port — returns sink states     │
  ├─────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ Run summary report      │ On completion: total records, duration, per-sink breakdown, DLQ counts    │
  └─────────────────────────┴───────────────────────────────────────────────────────────────────────────┘

  7. Configuration

  ┌───────────────────────────────┬─────────────────────────────────────────────────────────────────────────┐
  │            Feature            │                                 Details                                 │
  ├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ YAML config                   │ Validated against JSON Schema; Pydantic models for type safety          │
  ├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ Environment variable override │ ${ENV_VAR} interpolation in YAML values                                 │
  ├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ Per-sink config               │ Rate limit, retry count, batch size, concurrency, timeout, failure rate │
  ├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ Hot-reload (optional)         │ Watch config file and update rate limits without restart                │
  └───────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘

  8. Testing

  ┌───────────────────────┬─────────────────────────────────────────────────────────────────────────────────────┐
  │        Feature        │                                       Details                                       │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ pytest                │ Unit + integration + performance tests                                              │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Unit tests            │ Every reader, transformer, rate limiter, retry, circuit breaker tested in isolation │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Integration tests     │ Full pipeline with mock sinks — verify zero data loss                               │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Memory tests          │ tracemalloc assertions — constant memory regardless of input size                   │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Throughput benchmarks │ Measure records/sec; regression guard                                               │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ pytest-asyncio        │ Async test support                                                                  │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ Coverage              │ pytest-cov targeting >90%                                                           │
  └───────────────────────┴─────────────────────────────────────────────────────────────────────────────────────┘

  9. Key Python Libraries

  ┌─────────────────────────┬──────────────────────────────────────────┐
  │         Library         │                 Purpose                  │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ asyncio                 │ Core async runtime                       │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ pydantic                │ Config & record validation               │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ structlog               │ Structured logging                       │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ pyyaml                  │ Config loading                           │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ fastavro                │ Avro serialization                       │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ protobuf / grpcio-tools │ Protobuf compilation & serialization     │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ lxml                    │ XML generation                           │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ httpx                   │ Async HTTP client (REST sink simulation) │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ prometheus_client       │ Metrics export (optional)                │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ pytest / pytest-asyncio │ Testing                                  │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │ rich                    │ Beautiful console output for reporter    │
  └─────────────────────────┴──────────────────────────────────────────┘

  10. Design Patterns Used

  ┌─────────────────┬────────────────────────────────────────────────────┐
  │     Pattern     │                       Where                        │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Strategy        │ Transformers — pluggable serialization per sink    │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Factory         │ ReaderFactory, SinkFactory — instantiate by config │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Registry        │ TransformerRegistry — register/lookup by name      │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Observer        │ Metrics collector observes sink events             │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Pipeline        │ Ingestion → Transform → Dispatch chain             │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Template Method │ Base sink with connect/send/close lifecycle        │
  ├─────────────────┼────────────────────────────────────────────────────┤
  │ Circuit Breaker │ Resilience pattern per sink                        │
  └─────────────────┴────────────────────────────────────────────────────┘

  ---
  Data Flow

                           ┌──────────────┐
                           │  Config YAML │
                           └──────┬───────┘
                                  │
    ┌─────────────┐       ┌──────▼───────┐       ┌─────────────────┐
    │  CSV/JSONL/  │──────▶│  Ingestion   │──────▶│  Backpressure   │
    │  Fixed-width │stream │  (Generator) │batch  │  Queue (bounded)│
    └─────────────┘       └──────────────┘       └────────┬────────┘
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
                   │  (HTTP/2)   ││ (streaming) ││ (Kafka-like)││ (UPSERT)   │
                   └─────────────┘└─────────────┘└─────────────┘└────────────┘
                          │              │               │              │
                          └──────────────┴───────┬───────┴──────────────┘
                                                 │
                                      ┌──────────▼──────────┐
                                      │   DLQ (failed)      │
                                      │   Metrics Reporter  │
                                      └─────────────────────┘

  ---