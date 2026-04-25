from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.routes import gateway_http
from platform_common import service_proxy
from platform_common.web import create_service_app


@pytest.fixture(autouse=True)
def clear_service_proxy_cache() -> None:
    service_proxy.clear_runtime_endpoint_cache()
    yield
    service_proxy.clear_runtime_endpoint_cache()


class _SyncClient:
    responses: list[httpx.Response] = []
    calls: list[tuple[str, str, dict | None]] = []

    def __init__(self, *, timeout: float, follow_redirects: bool):
        self.timeout = timeout
        self.follow_redirects = follow_redirects

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str, params: dict | None = None) -> httpx.Response:
        self.calls.append(("GET", url, params))
        assert self.responses
        return self.responses.pop(0)


class _AsyncClient:
    registry_responses: list[httpx.Response] = []
    request_results: list[httpx.Response | Exception] = []
    registry_calls: list[str] = []
    request_calls: list[tuple[str, str]] = []

    def __init__(self, *, timeout: float, follow_redirects: bool):
        self.timeout = timeout
        self.follow_redirects = follow_redirects

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str, params: dict | None = None) -> httpx.Response:
        self.registry_calls.append(url)
        assert self.registry_responses
        return self.registry_responses.pop(0)

    async def request(self, method: str, url: str, headers: dict | None = None, content: bytes | None = None):
        self.request_calls.append((method, url))
        assert self.request_results
        result = self.request_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _service_payload(*, host: str = "telemetry-ingest-service-dep-1", port: int = 8080, proxy_base_path: str = "") -> dict:
    return {
        "unit_id": "telemetry-ingest-service",
        "display_name": "Telemetry Ingest Service",
        "package_owner": "space-ops-platform",
        "runtime_kind": "service",
        "runtime_template": "python-service",
        "source_path": "project/space-ops-platform",
        "active_deployment_id": "dep_123",
        "deployment_status": "healthy",
        "health_status": "passing",
        "discovery_metadata_json": {"service_slug": "telemetry-ingest-service"},
        "runtime_endpoint": {
            "service_name": host,
            "host": host,
            "port": port,
            "proxy_base_path": proxy_base_path,
            "health_path": "/health",
        },
    }


def test_build_service_proxy_url_uses_registry_runtime_endpoint(monkeypatch) -> None:
    _SyncClient.calls = []
    _SyncClient.responses = [httpx.Response(200, json=_service_payload(host="registry-host", proxy_base_path="/runtime"))]
    monkeypatch.setattr(service_proxy.httpx, "Client", _SyncClient)

    url = service_proxy.build_service_proxy_url("telemetry-ingest-service", "telemetry/feed-health")

    assert url == "http://registry-host:8080/runtime/telemetry/feed-health"
    assert _SyncClient.calls == [
        ("GET", "http://localhost:8100/registry/services/telemetry-ingest-service", None)
    ]


def test_build_service_proxy_url_uses_cache_until_ttl_expires(monkeypatch) -> None:
    _SyncClient.calls = []
    _SyncClient.responses = [
        httpx.Response(200, json=_service_payload(host="first-host")),
        httpx.Response(200, json=_service_payload(host="second-host")),
    ]
    monkeypatch.setattr(service_proxy.httpx, "Client", _SyncClient)

    first = service_proxy.build_service_proxy_url("telemetry-ingest-service", "telemetry/feed-health")
    second = service_proxy.build_service_proxy_url("telemetry-ingest-service", "telemetry/feed-health")
    cached = service_proxy._runtime_endpoint_cache["telemetry-ingest-service"]
    service_proxy._runtime_endpoint_cache["telemetry-ingest-service"] = service_proxy.CachedRuntimeEndpoint(
        endpoint=cached.endpoint,
        expires_at=0.0,
    )
    refreshed = service_proxy.build_service_proxy_url("telemetry-ingest-service", "telemetry/feed-health")

    assert first == "http://first-host:8080/telemetry/feed-health"
    assert second == first
    assert refreshed == "http://second-host:8080/telemetry/feed-health"
    assert len(_SyncClient.calls) == 2


def test_build_service_proxy_url_propagates_registry_errors(monkeypatch) -> None:
    _SyncClient.responses = [httpx.Response(404, json={"detail": "service not found"})]
    monkeypatch.setattr(service_proxy.httpx, "Client", _SyncClient)

    with pytest.raises(service_proxy.HTTPException) as exc:
        service_proxy.build_service_proxy_url("missing-service")

    assert exc.value.status_code == 404
    assert exc.value.detail == "service not found"


def test_build_service_proxy_url_rejects_missing_runtime_endpoint(monkeypatch) -> None:
    _SyncClient.responses = [httpx.Response(502, json={"detail": "service has no active runtime"})]
    monkeypatch.setattr(service_proxy.httpx, "Client", _SyncClient)

    with pytest.raises(service_proxy.HTTPException) as exc:
        service_proxy.build_service_proxy_url("telemetry-ingest-service")

    assert exc.value.status_code == 502
    assert exc.value.detail == "service has no active runtime"


def test_proxy_request_invalidates_cache_and_retries_once_on_connect_error(monkeypatch) -> None:
    _AsyncClient.registry_calls = []
    _AsyncClient.request_calls = []
    _AsyncClient.registry_responses = [
        httpx.Response(200, json=_service_payload(host="stale-host")),
        httpx.Response(200, json=_service_payload(host="fresh-host")),
    ]
    _AsyncClient.request_results = [
        httpx.ConnectError("stale endpoint"),
        httpx.Response(200, json={"ok": True}),
    ]
    monkeypatch.setattr(service_proxy.httpx, "AsyncClient", _AsyncClient)

    app = FastAPI()

    @app.get("/proxy")
    async def run_proxy(request: Request):
        return await service_proxy.proxy_request("telemetry-ingest-service", request, path="telemetry/feed-health")

    response = TestClient(app).get("/proxy?source_id=alpha")

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert _AsyncClient.registry_calls == [
        "http://localhost:8100/registry/services/telemetry-ingest-service",
        "http://localhost:8100/registry/services/telemetry-ingest-service",
    ]
    assert _AsyncClient.request_calls == [
        ("GET", "http://stale-host:8080/telemetry/feed-health?source_id=alpha"),
        ("GET", "http://fresh-host:8080/telemetry/feed-health?source_id=alpha"),
    ]


def test_gateway_routes_proxy_expected_service_paths(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    async def fake_proxy_request(service_slug: str, request: Request, *, path: str = ""):
        calls.append((service_slug, path))
        return service_proxy.Response(status_code=200, content=b"ok", media_type="text/plain")

    monkeypatch.setattr(gateway_http, "proxy_request", fake_proxy_request)
    app = create_service_app(title="gateway-test", description="gateway-test")
    app.include_router(gateway_http.router)
    client = TestClient(app)

    telemetry = client.get("/telemetry/sources")
    vehicle_configs = client.get("/vehicle-configs")
    feed_status = client.get("/ops/feed-status")

    assert telemetry.status_code == 200
    assert vehicle_configs.status_code == 200
    assert feed_status.status_code == 200
    assert calls == [
        ("source-registry-service", "telemetry/sources"),
        ("vehicle-config-service", "vehicle-configs/"),
        ("telemetry-ingest-service", "telemetry/feed-health"),
    ]
