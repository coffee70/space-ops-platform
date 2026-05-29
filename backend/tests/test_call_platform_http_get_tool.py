from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import HTTPException

from app.routes.handlers import tool_execution


class _FakeAsyncClient:
    response: httpx.Response | None = None
    error: Exception | None = None
    calls: list[dict] = []
    init_kwargs: dict = {}

    def __init__(self, **kwargs) -> None:
        type(self).init_kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, *, params: dict | None = None, headers: dict | None = None) -> httpx.Response:
        type(self).calls.append({"url": url, "params": params, "headers": headers})
        if type(self).error is not None:
            raise type(self).error
        assert type(self).response is not None
        return type(self).response


def _response(status_code: int, *, content: bytes | str = b"", headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(
        status_code,
        content=content.encode("utf-8") if isinstance(content, str) else content,
        headers=headers or {},
        request=httpx.Request("GET", "http://platform-api-gateway:8000/test"),
    )


@pytest.fixture(autouse=True)
def _reset_fake_client(monkeypatch):
    _FakeAsyncClient.response = None
    _FakeAsyncClient.error = None
    _FakeAsyncClient.calls = []
    _FakeAsyncClient.init_kwargs = {}
    monkeypatch.setattr(tool_execution.httpx, "AsyncClient", _FakeAsyncClient)
    monkeypatch.setattr(tool_execution, "_platform_http_base_url", lambda: "http://platform-api-gateway:8000")


def test_validate_platform_http_path_accepts_same_origin_path() -> None:
    assert tool_execution._validate_platform_http_path(" /telemetry/health ") == "/telemetry/health"


@pytest.mark.parametrize(
    "path",
    [
        "telemetry/health",
        "https://example.com",
        "http://example.com",
        "//example.com/path",
        "example.com/path",
    ],
)
def test_validate_platform_http_path_rejects_non_same_origin_paths(path: str) -> None:
    with pytest.raises(HTTPException):
        tool_execution._validate_platform_http_path(path)


def test_sanitize_platform_http_request_headers_uses_small_allowlist() -> None:
    headers = tool_execution._sanitize_platform_http_request_headers(
        {
            "Authorization": "secret",
            "Cookie": "session=secret",
            "X-Api-Key": "secret",
            "X-Request-Id": "req-1",
            "X-Trace-Id": "trace-1",
            "X-Not-Allowed": "ignore-me",
            "Accept": "application/json",
        }
    )

    assert headers == {
        "accept": "application/json",
        "x-request-id": "req-1",
        "x-trace-id": "trace-1",
    }


def test_redact_platform_http_response_headers_redacts_sensitive_values() -> None:
    headers = tool_execution._redact_platform_http_response_headers(
        httpx.Headers(
            {
                "content-type": "application/json",
                "set-cookie": "session=secret",
                "x-api-key": "secret",
            }
        )
    )

    assert headers["content-type"] == "application/json"
    assert headers["set-cookie"] == "***REDACTED***"
    assert headers["x-api-key"] == "***REDACTED***"


def test_expected_status_matches_defaults_to_successful_http_range() -> None:
    assert tool_execution._expected_status_matches(200, None) is True
    assert tool_execution._expected_status_matches(302, None) is True
    assert tool_execution._expected_status_matches(404, None) is False
    assert tool_execution._expected_status_matches(200, 200) is True
    assert tool_execution._expected_status_matches(404, 200) is False
    assert tool_execution._expected_status_matches(204, [200, 204]) is True


@pytest.mark.anyio
async def test_call_platform_http_get_returns_json_response_and_redacted_headers() -> None:
    _FakeAsyncClient.response = _response(
        200,
        content='{"state":"watch"}',
        headers={"content-type": "application/json", "set-cookie": "session=secret", "x-request-id": "trace-1"},
    )

    result = await tool_execution._call_platform_http_get(
        {
            "path": "/drogonsat-power-health/advisory/power",
            "query": {"source_id": "simulator"},
            "headers": {"Authorization": "secret", "X-Request-Id": "req-1"},
            "expected_status": 200,
        }
    )

    assert result["ok"] is True
    assert result["request"] == {
        "method": "GET",
        "path": "/drogonsat-power-health/advisory/power",
        "query": {"source_id": "simulator"},
        "resolved_base": "platform_gateway",
    }
    assert result["response"]["status"] == 200
    assert result["response"]["body_json"] == {"state": "watch"}
    assert result["response"]["headers"]["set-cookie"] == "***REDACTED***"
    assert result["validation"] == {"expected_status": 200, "status_matches": True}
    assert result["diagnostics"]["trace_id"] == "trace-1"
    assert _FakeAsyncClient.init_kwargs["follow_redirects"] is False
    assert _FakeAsyncClient.calls == [
        {
            "url": "http://platform-api-gateway:8000/drogonsat-power-health/advisory/power",
            "params": {"source_id": "simulator"},
            "headers": {"accept": "application/json, text/plain, */*", "x-request-id": "req-1"},
        }
    ]


@pytest.mark.anyio
async def test_call_platform_http_get_returns_404_as_diagnostic_data_not_tool_failure() -> None:
    _FakeAsyncClient.response = _response(404, content="not found", headers={"content-type": "text/plain"})

    result = await tool_execution._call_platform_http_get({"path": "/definitely-missing-route", "expected_status": 200})

    assert result["ok"] is False
    assert result["response"]["status"] == 404
    assert result["response"]["body_text"] == "not found"
    assert result["validation"] == {"expected_status": 200, "status_matches": False}
    assert result["diagnostics"]["error_type"] is None


@pytest.mark.anyio
async def test_call_platform_http_get_truncates_large_response_body() -> None:
    _FakeAsyncClient.response = _response(200, content=b"a" * 2048, headers={"content-type": "text/plain"})

    result = await tool_execution._call_platform_http_get({"path": "/large", "max_response_bytes": 1024})

    assert result["ok"] is True
    assert result["response"]["truncated"] is True
    assert result["response"]["body_text"] == "a" * 1024
    assert result["response"]["body_json"] is None


@pytest.mark.anyio
async def test_call_platform_http_get_returns_structured_timeout_result() -> None:
    _FakeAsyncClient.error = httpx.TimeoutException("boom")

    result = await tool_execution._call_platform_http_get({"path": "/slow", "timeout_ms": 1234, "expected_status": 200})

    assert result == {
        "ok": False,
        "request": {"method": "GET", "path": "/slow", "query": {}, "resolved_base": "platform_gateway"},
        "response": None,
        "validation": {"expected_status": 200, "status_matches": False},
        "diagnostics": {
            "trace_id": None,
            "error_type": "timeout",
            "error_message": "GET timed out after 1234 ms",
        },
    }


@pytest.mark.anyio
async def test_call_platform_http_get_returns_structured_request_error_result() -> None:
    _FakeAsyncClient.error = httpx.ConnectError("connection refused", request=httpx.Request("GET", "http://x"))

    result = await tool_execution._call_platform_http_get({"path": "/unreachable", "expected_status": [200, 204]})

    assert result["ok"] is False
    assert result["response"] is None
    assert result["validation"] == {"expected_status": [200, 204], "status_matches": False}
    assert result["diagnostics"]["error_type"] == "ConnectError"
    assert "connection refused" in result["diagnostics"]["error_message"]


@pytest.mark.anyio
async def test_execute_mapped_tool_dispatches_call_platform_http_get(monkeypatch) -> None:
    async def fake_call(tool_input: dict) -> dict:
        return {"ok": True, "path": tool_input["path"]}

    monkeypatch.setattr(tool_execution, "_call_platform_http_get", fake_call)

    result = await tool_execution._execute_mapped_tool(
        "call_platform_http_get",
        {"path": "/telemetry/health"},
        db=MagicMock(),
    )

    assert result == {"ok": True, "path": "/telemetry/health"}
