from __future__ import annotations

from app.intelligence.clients.context_retrieval_clients import HttpContextRetrievalClients


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = ""

    def json(self) -> object:
        return self._payload


class _FakeClient:
    def __init__(self, *, timeout: float) -> None:
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, *, json: dict, headers: dict[str, str]):
        _STATE["post"] = {"url": url, "json": json, "headers": headers}
        return _FakeResponse([{"document_id": "d-1"}])

    def get(self, url: str, *, params: dict[str, str], headers: dict[str, str]):
        _STATE["get"] = {"url": url, "params": params, "headers": headers}
        return _FakeResponse([{"name": "list_runtime_services", "enabled": True}])


_STATE: dict[str, dict] = {}


def test_http_context_clients_forward_trace_headers(monkeypatch) -> None:
    import app.intelligence.clients.context_retrieval_clients as mod

    monkeypatch.setattr(mod.httpx, "Client", _FakeClient)
    clients = HttpContextRetrievalClients(timeout_seconds=1.0)
    trace = {
        "conversation_id": "conv-1",
        "agent_run_id": "run-1",
        "request_id": "req-1",
        "tool_call_id": "tool-1",
    }

    clients.fetch_document_context(query="q", mission_id="m", vehicle_id="v", limit=3, trace=trace)
    clients.fetch_code_context(query="q", branch="main", limit=2, trace=trace)
    clients.fetch_tool_registry_metadata(limit=5, trace=trace)

    assert _STATE["post"]["headers"]["x-conversation-id"] == "conv-1"
    assert _STATE["post"]["headers"]["x-agent-run-id"] == "run-1"
    assert _STATE["post"]["headers"]["x-request-id"] == "req-1"
    assert _STATE["post"]["headers"]["x-tool-call-id"] == "tool-1"
    assert _STATE["get"]["headers"]["x-request-id"] == "req-1"
    assert _STATE["get"]["params"]["enabled"] == "true"

