from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.routes import gateway_http
from platform_common import service_proxy
from platform_common.web import create_service_app


class _AsyncClient:
    request_results: list[httpx.Response | Exception] = []
    get_results: list[httpx.Response | Exception] = []
    request_calls: list[dict] = []
    get_calls: list[dict] = []

    def __init__(self, *, timeout: float, follow_redirects: bool):
        self.timeout = timeout
        self.follow_redirects = follow_redirects

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str, params: dict | None = None) -> httpx.Response:
        self.get_calls.append(
            {
                "url": url,
                "params": params,
                "timeout": self.timeout,
                "follow_redirects": self.follow_redirects,
            }
        )
        assert self.get_results
        result = self.get_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def request(self, method: str, url: str, headers: dict | None = None, content: bytes | None = None):
        self.request_calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "content": content,
                "timeout": self.timeout,
                "follow_redirects": self.follow_redirects,
            }
        )
        assert self.request_results
        result = self.request_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_build_service_proxy_url_uses_kernel_internal_service_proxy() -> None:
    url = service_proxy.build_service_proxy_url("telemetry-ingest-service", "telemetry/feed-health")

    assert url == "http://localhost:8100/internal/runtime-services/telemetry-ingest-service/telemetry/feed-health"


def test_proxy_request_does_not_call_registry_services(monkeypatch) -> None:
    _AsyncClient.request_calls = []
    _AsyncClient.get_calls = []
    _AsyncClient.request_results = [httpx.Response(200, json={"ok": True})]
    monkeypatch.setattr(service_proxy.httpx, "AsyncClient", _AsyncClient)

    app = FastAPI()

    @app.get("/proxy")
    async def run_proxy(request: Request):
        return await service_proxy.proxy_request("telemetry-ingest-service", request, path="telemetry/feed-health")

    response = TestClient(app).get("/proxy?source_id=alpha", headers={"connection": "close"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert _AsyncClient.get_calls == []
    assert len(_AsyncClient.request_calls) == 1
    call = _AsyncClient.request_calls[0]
    assert call["method"] == "GET"
    assert call["url"] == (
        "http://localhost:8100/internal/runtime-services/"
        "telemetry-ingest-service/telemetry/feed-health?source_id=alpha"
    )
    forwarded_headers = {key.lower() for key in call["headers"]}
    assert "host" not in forwarded_headers
    assert "connection" not in forwarded_headers
    assert call["content"] is None
    assert call["timeout"] == 30.0
    assert call["follow_redirects"] is False


@pytest.mark.anyio
async def test_fetch_service_json_uses_kernel_internal_service_proxy(monkeypatch) -> None:
    _AsyncClient.request_calls = []
    _AsyncClient.get_calls = []
    _AsyncClient.get_results = [httpx.Response(200, json={"status": "passing"})]
    monkeypatch.setattr(service_proxy.httpx, "AsyncClient", _AsyncClient)

    payload = await service_proxy.fetch_service_json(
        "telemetry-ingest-service",
        "telemetry/feed-health",
        params={"source_id": "alpha"},
    )

    assert payload == {"status": "passing"}
    assert _AsyncClient.request_calls == []
    assert _AsyncClient.get_calls == [
        {
            "url": (
                "http://localhost:8100/internal/runtime-services/"
                "telemetry-ingest-service/telemetry/feed-health"
            ),
            "params": {"source_id": "alpha"},
            "timeout": 15.0,
            "follow_redirects": False,
        }
    ]


def test_proxy_request_returns_502_when_control_plane_unavailable(monkeypatch) -> None:
    _AsyncClient.request_results = [httpx.ConnectError("control plane unavailable")]
    monkeypatch.setattr(service_proxy.httpx, "AsyncClient", _AsyncClient)

    app = FastAPI()

    @app.get("/proxy")
    async def run_proxy(request: Request):
        return await service_proxy.proxy_request("telemetry-ingest-service", request, path="telemetry/feed-health")

    response = TestClient(app).get("/proxy")

    assert response.status_code == 502
    assert response.json()["detail"] == "service proxy unavailable"


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
