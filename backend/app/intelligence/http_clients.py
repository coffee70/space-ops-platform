"""HTTP client helpers for control-plane and service calls."""

from __future__ import annotations

import httpx

from app.config import get_settings


async def get_json(url: str, *, params: dict | None = None, headers: dict[str, str] | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


async def post_json(url: str, *, payload: dict, headers: dict[str, str] | None = None) -> dict:
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def control_plane_url(path: str) -> str:
    base = get_settings().control_plane_url.rstrip('/')
    return f"{base}/{path.lstrip('/')}"
