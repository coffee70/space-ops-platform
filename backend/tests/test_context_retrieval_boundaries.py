from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from starlette.requests import Request

from app.routes.handlers import context_retrieval


def _request(headers: dict[str, str] | None = None) -> Request:
    header_items = []
    for key, value in (headers or {}).items():
        header_items.append((key.lower().encode("latin-1"), value.encode("latin-1")))
    return Request({"type": "http", "headers": header_items})


class FakeClients:
    def __init__(self) -> None:
        self.document_calls: list[dict] = []
        self.code_calls: list[dict] = []
        self.tool_calls: list[dict] = []
        self.fail_documents = False
        self.fail_code = False
        self.fail_tools = False

    def fetch_document_context(self, *, query: str, mission_id: str | None, vehicle_id: str | None, limit: int, trace: dict[str, str | None]) -> list[dict]:
        self.document_calls.append(
            {
                "query": query,
                "mission_id": mission_id,
                "vehicle_id": vehicle_id,
                "limit": limit,
                "trace": dict(trace),
            }
        )
        if self.fail_documents:
            raise HTTPException(status_code=504, detail="document timeout")
        return [{"document_id": f"doc-{i}", "title": "Doc", "content": "chunk"} for i in range(limit + 2)]

    def fetch_code_context(self, *, query: str, branch: str, limit: int, trace: dict[str, str | None]) -> list[dict]:
        self.code_calls.append({"query": query, "branch": branch, "limit": limit, "trace": dict(trace)})
        if self.fail_code:
            raise HTTPException(status_code=503, detail="code unavailable")
        return [{"repository": "repo", "file_path": f"file-{i}.py", "content": "code"} for i in range(limit + 3)]

    def fetch_tool_registry_metadata(self, *, limit: int, trace: dict[str, str | None]) -> list[dict]:
        self.tool_calls.append({"limit": limit, "trace": dict(trace)})
        if self.fail_tools:
            raise RuntimeError("tool registry down")
        return [{"name": f"tool_{i}", "description": "List", "enabled": True} for i in range(limit + 4)]


def test_context_retrieval_does_not_import_document_or_code_route_handlers() -> None:
    src = (Path(__file__).resolve().parents[1] / "app/routes/handlers/context_retrieval.py").read_text()
    assert "from app.routes.handlers import document_knowledge" not in src
    assert "from app.routes.handlers import code_intelligence" not in src


def test_context_packet_uses_injected_clients_with_explicit_instructions() -> None:
    clients = FakeClients()
    payload = context_retrieval.context_packet(
        {
            "conversation_id": "c-1",
            "agent_run_id": "run-1",
            "request_id": "req-1",
            "message": "hello",
            "retrieval_instructions": {"documents": True, "code": False, "platform": False, "tools": True},
            "mission_id": "mission-a",
            "vehicle_id": "vehicle-a",
        },
        request=_request(),
        clients=clients,
    )

    assert len(clients.document_calls) == 1
    assert len(clients.code_calls) == 0
    assert len(clients.tool_calls) == 1
    assert payload["document_chunk_count"] == 6
    assert payload["code_chunk_count"] == 0
    assert payload["tool_definition_count"] == 20


def test_context_packet_passes_trace_to_injected_clients() -> None:
    clients = FakeClients()
    context_retrieval.context_packet(
        {
            "message": "trace test",
            "retrieval_instructions": {"documents": True, "code": True, "platform": False, "tools": True},
        },
        request=_request(
            {
                "x-conversation-id": "conv-1",
                "x-agent-run-id": "run-1",
                "x-request-id": "req-1",
                "x-tool-call-id": "tool-1",
            }
        ),
        clients=clients,
    )

    assert clients.document_calls[0]["trace"]["conversation_id"] == "conv-1"
    assert clients.document_calls[0]["trace"]["agent_run_id"] == "run-1"
    assert clients.document_calls[0]["trace"]["request_id"] == "req-1"
    assert clients.document_calls[0]["trace"]["tool_call_id"] == "tool-1"
    assert clients.code_calls[0]["trace"]["request_id"] == "req-1"
    assert clients.tool_calls[0]["trace"]["tool_call_id"] == "tool-1"


def test_context_packet_returns_partial_context_and_failure_metadata() -> None:
    clients = FakeClients()
    clients.fail_documents = True
    clients.fail_code = True
    clients.fail_tools = True

    payload = context_retrieval.context_packet(
        {
            "conversation_id": "c-1",
            "agent_run_id": "run-1",
            "request_id": "req-1",
            "message": "failures",
            "retrieval_instructions": {"documents": True, "code": True, "platform": False, "tools": True},
        },
        request=_request(),
        clients=clients,
    )

    assert payload["document_chunk_count"] == 0
    assert payload["code_chunk_count"] == 0
    assert payload["tool_definition_count"] == 0
    assert payload["truncated"] is True
    assert set(reason.split(":")[0] for reason in payload["truncation_reasons"]) == {
        "document_context_failed",
        "code_context_failed",
        "tool_context_failed",
    }
    assert payload["failed_sources"] == [
        {"service": "document-knowledge-service", "failure_type": "timeout"},
        {"service": "code-intelligence-service", "failure_type": "unavailable"},
        {"service": "tool-registry-service", "failure_type": "error"},
    ]
    assert payload["data"]["mission_documents"] == []
    assert payload["data"]["code_context"] == []
    assert payload["data"]["tool_context"] == []


def test_context_packet_enforces_context_limits_on_client_results() -> None:
    clients = FakeClients()
    payload = context_retrieval.context_packet(
        {
            "conversation_id": "c-1",
            "agent_run_id": "run-1",
            "request_id": "req-1",
            "message": "limits",
            "retrieval_instructions": {"documents": True, "code": True, "platform": False, "tools": True},
        },
        request=_request(),
        clients=clients,
    )

    assert payload["document_chunk_count"] == 6
    assert payload["code_chunk_count"] == 6
    assert payload["tool_definition_count"] == 20

