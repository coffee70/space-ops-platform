# Build stage: install dependencies
FROM python:3.11-slim AS builder

WORKDIR /app/platform

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY space-ops-platform/backend/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --extra-index-url https://download.pytorch.org/whl/cpu

# Runtime stage: minimal image
FROM python:3.11-slim AS runtime

WORKDIR /app/platform

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY space-ops-platform/backend /app/platform/backend
COPY space-ops-platform/telemetry_catalog /app/platform/telemetry_catalog
COPY space-ops-apps/vehicle-configurations /app/vehicle-configurations

ENV PYTHONPATH=/app/platform/backend:/app/platform
ENV PYTHONUNBUFFERED=1
ENV VEHICLE_CONFIG_ROOT=/app/vehicle-configurations

EXPOSE 8000

CMD ["sh", "-c", "cd /app/platform/backend && alembic -c alembic.ini upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
