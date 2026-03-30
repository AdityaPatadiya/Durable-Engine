FROM python:3.11-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

FROM base AS production
COPY pyproject.toml README.md ./
COPY src/ src/
COPY config/ config/
COPY proto/ proto/
COPY samples/ samples/

RUN pip install --no-cache-dir hatchling && \
    pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python", "-m", "durable_engine"]
CMD ["--config", "config/default.yaml"]
