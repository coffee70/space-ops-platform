from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import httpx
from fastapi import HTTPException

from app.intelligence.trace import trace_headers
from platform_common.service_proxy import build_service_proxy_url


class ContextRetrievalClients(Protocol):
    def fetch_document_context(self, *, query: str, mission_id: str | None, vehicle_id: str | None, limit: int, trace: dict[str, str | None]) -> list[dict]:
        ...

    def fetch_code_context(self, *, query: str, branch: str, limit: int, trace: dict[str, str | None]) -> list[dict]:
        ...

    def fetch_tool_registry_metadata(self, *, limit: int, trace: dict[str, str | None]) -> list[dict]:
        ...


@dataclass(slots=True)
class HttpContextRetrievalClients:
    timeout_seconds: float = 30.0

    def fetch_document_context(self, *, query: str, mission_id: str | None, vehicle_id: str | None, limit: int, trace: dict[str, str | None]) -> list[dict]:
        payload = {"query": query, "mission_id": mission_id, "vehicle_id": vehicle_id, "limit": limit}
        response = self._post("document-knowledge-service", "/search", payload=payload, trace=trace)
        return self._ensure_list(response)

    def fetch_code_context(self, *, query: str, branch: str, limit: int, trace: dict[str, str | None]) -> list[dict]:
        payload = {"query": query, "branch": branch, "limit": limit}
        response = self._post("code-intelligence-service", "/search", payload=payload, trace=trace)
        return self._ensure_list(response)

    def fetch_tool_registry_metadata(self, *, limit: int, trace: dict[str, str | None]) -> list[dict]:
        response = self._get("tool-registry-service", "/definitions", params={"enabled": "true"}, trace=trace)
        return self._ensure_list(response)[:limit]

    def _post(self, service_slug: str, path: str, *, payload: dict, trace: dict[str, str | None]) -> object:
        url = build_service_proxy_url(service_slug, path)
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(url, json=payload, headers=trace_headers(trace))
        except httpx.TimeoutException as exc:
            raise HTTPException(status_code=504, detail="downstream request timeout") from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="downstream service unavailable") from exc
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text or "downstream service error")
        return response.json()

    def _get(self, service_slug: str, path: str, *, params: dict[str, str], trace: dict[str, str | None]) -> object:
        url = build_service_proxy_url(service_slug, path)
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(url, params=params, headers=trace_headers(trace))
        except httpx.TimeoutException as exc:
            raise HTTPException(status_code=504, detail="downstream request timeout") from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="downstream service unavailable") from exc
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text or "downstream service error")
        return response.json()

    @staticmethod
    def _ensure_list(payload: object) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        raise HTTPException(status_code=502, detail="downstream service returned invalid payload")


def get_context_clients() -> ContextRetrievalClients:
    return HttpContextRetrievalClients()
