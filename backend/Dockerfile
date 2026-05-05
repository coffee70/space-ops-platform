# Build stage: install dependencies
FROM python:3.11-slim AS builder

WORKDIR /app/backend

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    --extra-index-url https://download.pytorch.org/whl/cpu

# Runtime stage: minimal image
FROM python:3.11-slim AS runtime

WORKDIR /app/backend

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY backend /app/backend
COPY telemetry_catalog /app/telemetry_catalog
COPY backend/resources/vehicle-configurations /app/backend/resources/vehicle-configurations

ENV PYTHONPATH=/app/backend:/app
ENV PYTHONUNBUFFERED=1
ENV VEHICLE_CONFIG_ROOT=/app/backend/resources/vehicle-configurations

EXPOSE 8000

CMD ["sh", "-c", "alembic -c alembic.ini upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
