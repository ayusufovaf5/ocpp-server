# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder

WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY config.py main.py logging_config.py auth.py ./
COPY api ./api
COPY db ./db
COPY services ./services
COPY repositories ./repositories
COPY ocpp16 ./ocpp16
COPY tasks ./tasks
COPY state ./state
COPY events ./events
COPY migrations ./migrations
COPY alembic.ini ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN useradd --create-home --uid 1000 appuser

COPY --from=builder /install /usr/local
COPY --from=builder /build /app

USER appuser

EXPOSE 8000 9000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
