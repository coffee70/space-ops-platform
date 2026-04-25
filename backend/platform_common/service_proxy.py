"""Helpers for proxying requests to managed services resolved from the control-plane registry."""

from __future__ import annotations

from dataclasses import dataclass
import time

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
RUNTIME_ENDPOINT_CACHE_TTL_SECONDS = 5.0


@dataclass(slots=True)
class RuntimeEndpoint:
    host: str
    port: int
    proxy_base_path: str


@dataclass(slots=True)
class CachedRuntimeEndpoint:
    endpoint: RuntimeEndpoint
    expires_at: float


_runtime_endpoint_cache: dict[str, CachedRuntimeEndpoint] = {}


def clear_runtime_endpoint_cache() -> None:
    _runtime_endpoint_cache.clear()


def _now() -> float:
    return time.monotonic()


def _registry_service_url(service_slug: str) -> str:
    base_url = get_settings().control_plane_url.rstrip("/")
    return f"{base_url}/registry/services/{service_slug}"


def _join_url_path(base_path: str, path: str = "") -> str:
    segments = [segment.strip("/") for segment in (base_path, path) if segment and segment.strip("/")]
    if not segments:
        return "/"
    return "/" + "/".join(segments)


def _build_upstream_url(endpoint: RuntimeEndpoint, path: str = "", query: str = "") -> str:
    query_suffix = f"?{query}" if query else ""
    return f"http://{endpoint.host}:{endpoint.port}{_join_url_path(endpoint.proxy_base_path, path)}{query_suffix}"


def _parse_error_detail(response: httpx.Response) -> str:
    detail = response.text
    try:
        payload = response.json()
    except ValueError:
        return detail
    return payload.get("detail", detail)


def _raise_registry_error(response: httpx.Response) -> None:
    raise HTTPException(status_code=response.status_code, detail=_parse_error_detail(response))


def _parse_runtime_endpoint(payload: dict) -> RuntimeEndpoint:
    runtime_endpoint = payload.get("runtime_endpoint")
    if not isinstance(runtime_endpoint, dict):
        raise HTTPException(status_code=502, detail="service has no runtime endpoint")
    host = runtime_endpoint.get("host")
    port = runtime_endpoint.get("port")
    proxy_base_path = runtime_endpoint.get("proxy_base_path") or ""
    if not isinstance(host, str) or not host:
        raise HTTPException(status_code=502, detail="service has invalid runtime endpoint")
    if not isinstance(port, int) or port <= 0:
        raise HTTPException(status_code=502, detail="service has invalid runtime endpoint")
    if not isinstance(proxy_base_path, str):
        raise HTTPException(status_code=502, detail="service has invalid runtime endpoint")
    return RuntimeEndpoint(host=host, port=port, proxy_base_path=proxy_base_path)


def _cache_get(service_slug: str) -> RuntimeEndpoint | None:
    cached = _runtime_endpoint_cache.get(service_slug)
    if cached is None:
        return None
    if cached.expires_at <= _now():
        _runtime_endpoint_cache.pop(service_slug, None)
        return None
    return cached.endpoint


def _cache_set(service_slug: str, endpoint: RuntimeEndpoint) -> RuntimeEndpoint:
    _runtime_endpoint_cache[service_slug] = CachedRuntimeEndpoint(
        endpoint=endpoint,
        expires_at=_now() + RUNTIME_ENDPOINT_CACHE_TTL_SECONDS,
    )
    return endpoint


def invalidate_runtime_endpoint(service_slug: str) -> None:
    _runtime_endpoint_cache.pop(service_slug, None)


def _fetch_registry_service_sync(service_slug: str) -> RuntimeEndpoint:
    with httpx.Client(timeout=15.0, follow_redirects=False) as client:
        response = client.get(_registry_service_url(service_slug))
    if response.status_code >= 400:
        _raise_registry_error(response)
    return _parse_runtime_endpoint(response.json())


async def _fetch_registry_service_async(service_slug: str) -> RuntimeEndpoint:
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=False) as client:
        response = await client.get(_registry_service_url(service_slug))
    if response.status_code >= 400:
        _raise_registry_error(response)
    return _parse_runtime_endpoint(response.json())


def resolve_runtime_endpoint(service_slug: str, *, refresh: bool = False) -> RuntimeEndpoint:
    if not refresh:
        cached = _cache_get(service_slug)
        if cached is not None:
            return cached
    invalidate_runtime_endpoint(service_slug)
    return _cache_set(service_slug, _fetch_registry_service_sync(service_slug))


async def resolve_runtime_endpoint_async(service_slug: str, *, refresh: bool = False) -> RuntimeEndpoint:
    if not refresh:
        cached = _cache_get(service_slug)
        if cached is not None:
            return cached
    invalidate_runtime_endpoint(service_slug)
    return _cache_set(service_slug, await _fetch_registry_service_async(service_slug))


def build_service_proxy_url(service_slug: str, path: str = "", *, refresh: bool = False) -> str:
    endpoint = resolve_runtime_endpoint(service_slug, refresh=refresh)
    return _build_upstream_url(endpoint, path)


async def proxy_request(service_slug: str, request: Request, *, path: str = "") -> Response:
    endpoint = await resolve_runtime_endpoint_async(service_slug)
    headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_BY_HOP_HEADERS}
    body = await request.body()

    for attempt in range(2):
        upstream = _build_upstream_url(endpoint, path, request.url.query)
        try:
            async with httpx.AsyncClient(follow_redirects=False, timeout=30.0) as client:
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
        except httpx.RequestError as exc:
            if attempt == 0:
                invalidate_runtime_endpoint(service_slug)
                endpoint = await resolve_runtime_endpoint_async(service_slug, refresh=True)
                continue
            raise HTTPException(status_code=502, detail="service upstream unavailable") from exc

    raise HTTPException(status_code=502, detail="service upstream unavailable")


async def fetch_service_json(service_slug: str, path: str, *, params: dict[str, object] | None = None) -> dict:
    endpoint = await resolve_runtime_endpoint_async(service_slug)

    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=False) as client:
                response = await client.get(_build_upstream_url(endpoint, path), params=params)
        except httpx.RequestError as exc:
            if attempt == 0:
                invalidate_runtime_endpoint(service_slug)
                endpoint = await resolve_runtime_endpoint_async(service_slug, refresh=True)
                continue
            raise HTTPException(status_code=502, detail="service upstream unavailable") from exc
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=_parse_error_detail(response))
        return response.json()

    raise HTTPException(status_code=502, detail="service upstream unavailable")
