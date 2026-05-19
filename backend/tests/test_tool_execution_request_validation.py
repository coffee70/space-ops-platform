from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from app.routes import tool_execution


VALID_PAYLOAD = {
    "conversation_id": "11111111-1111-4111-8111-111111111111",
    "agent_run_id": "22222222-2222-4222-8222-222222222222",
    "request_id": "33333333-3333-4333-8333-333333333333",
    "tool_call_id": "44444444-4444-4444-8444-444444444444",
    "tool_name": "list_available_tools",
    "input": {},
    "execution_mode": "read_only",
    "confirmation_token": None,
}

VALID_HEADERS = {
    "x-agent-run-id": VALID_PAYLOAD["agent_run_id"],
    "x-request-id": VALID_PAYLOAD["request_id"],
    "x-tool-call-id": VALID_PAYLOAD["tool_call_id"],
}


def _client() -> TestClient:
    app = FastAPI()
    app.add_exception_handler(RequestValidationError, tool_execution.tool_execution_validation_exception_handler)
    app.include_router(tool_execution.router)
    return TestClient(app)


@pytest.mark.parametrize("field_name", ["conversation_id", "agent_run_id", "request_id", "tool_call_id"])
def test_tool_execution_invalid_uuid_body_returns_diagnostic_400(field_name: str) -> None:
    payload = dict(VALID_PAYLOAD)
    payload[field_name] = "call_demo_non_uuid"

    response = _client().post("/execute", json=payload, headers=VALID_HEADERS)

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error_code"] == "invalid_tool_execution_trace_id"
    assert detail["message"] == f"{field_name} must be a valid UUID."
    assert any(issue["field"] == f"body.{field_name}" for issue in detail["issues"])
