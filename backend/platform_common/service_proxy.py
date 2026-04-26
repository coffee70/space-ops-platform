"""Helpers for proxying requests to managed services through the control plane."""

from __future__ import annotations

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


def _join_url_path(base_path: str, path: str = "") -> str:
    segments = [segment.strip("/") for segment in (base_path, path) if segment and segment.strip("/")]
    if not segments:
        return "/"
    return "/" + "/".join(segments)


def _kernel_service_proxy_url(service_slug: str, path: str = "", query: str = "") -> str:
    base_url = get_settings().control_plane_url.rstrip("/")
    query_suffix = f"?{query}" if query else ""
    return f"{base_url}{_join_url_path(f'/internal/runtime-services/{service_slug}', path)}{query_suffix}"


def _parse_error_detail(response: httpx.Response) -> str:
    detail = response.text
    try:
        payload = response.json()
    except ValueError:
        return detail
    return payload.get("detail", detail)


def build_service_proxy_url(service_slug: str, path: str = "") -> str:
    return _kernel_service_proxy_url(service_slug, path)


async def proxy_request(service_slug: str, request: Request, *, path: str = "") -> Response:
    headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_BY_HOP_HEADERS}
    body = await request.body()
    upstream = _kernel_service_proxy_url(service_slug, path, request.url.query)

    try:
        async with httpx.AsyncClient(follow_redirects=False, timeout=30.0) as client:
            upstream_response = await client.request(
                request.method,
                upstream,
                headers=headers,
                content=body if body else None,
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="service proxy unavailable") from exc

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
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=False) as client:
            response = await client.get(_kernel_service_proxy_url(service_slug, path), params=params)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="service proxy unavailable") from exc
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=_parse_error_detail(response))
    return response.json()
