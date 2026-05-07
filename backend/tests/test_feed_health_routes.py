"""HTTP behavior for ingest feed-health endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.database import get_db
from app.routes import feed_health as feed_health_routes


def _client_with_db(fake_db):
    app = FastAPI()
    app.include_router(feed_health_routes.router, prefix="/telemetry")

    def _yield_db():
        yield fake_db

    app.dependency_overrides[get_db] = _yield_db
    return TestClient(app)


def test_feed_health_get_unknown_source_returns_disconnected_placeholder() -> None:
    db = MagicMock()
    db.get.return_value = None
    client = _client_with_db(db)

    resp = client.get("/telemetry/feed-health", params={"source_id": "deadbeef-dead-beef-dead-beefdeadbeef"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["connected"] is False
    assert body["state"] == "disconnected"
    assert body["approx_rate_hz"] is None


def test_feed_health_list_empty_table_returns_items() -> None:
    db = MagicMock()
    db.execute.return_value.scalars.return_value.all.return_value = []
    client = _client_with_db(db)

    resp = client.get("/telemetry/feed-health")

    assert resp.status_code == 200
    assert resp.json() == {"items": []}
