FROM python:3.11-slim AS builder

WORKDIR /app
# core dependencies for the compilation time and required for the `pip install ...` 
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir hatchling && \
    pip install --no-cache-dir .

FROM python:3.11-slim

WORKDIR /app
# core dependencies for the runtime `.so` files to exist on the system.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 libxslt1.1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app/src /app/src
COPY config/ config/
COPY samples/ samples/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python", "-m", "durable_engine"]
CMD ["--config", "config/default.yaml"]
