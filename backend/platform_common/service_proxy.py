"""Helpers for proxying requests to managed services through the control plane."""

from __future__ import annotations

from typing import Iterable

import httpx
from fastapi import HTTPException, Request, Response

from app.config import get_settings

HOP_BY_HOP_HEADERS = {
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def build_service_proxy_url(service_slug: str, path: str = "") -> str:
    base_url = get_settings().control_plane_url.rstrip("/")
    upstream = f"{base_url}/proxy/services/{service_slug}"
    clean_path = path.lstrip("/")
    if clean_path:
        upstream = f"{upstream}/{clean_path}"
    return upstream


async def proxy_request(service_slug: str, request: Request, *, path: str = "") -> Response:
    upstream = build_service_proxy_url(service_slug, path)
    if request.url.query:
        upstream = f"{upstream}?{request.url.query}"

    headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_BY_HOP_HEADERS}
    body = await request.body()
    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
        upstream_response = await client.request(
            request.method,
            upstream,
            headers=headers,
            content=body if body else None,
        )

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            key: value
            for key, value in upstream_response.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS
        },
        media_type=upstream_response.headers.get("content-type"),
    )


async def fetch_service_json(service_slug: str, path: str, *, params: dict[str, object] | None = None) -> dict:
    upstream = build_service_proxy_url(service_slug, path)
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(upstream, params=params)
    if response.status_code >= 400:
        detail = response.text
        try:
            payload = response.json()
            detail = payload.get("detail", detail)
        except ValueError:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()
