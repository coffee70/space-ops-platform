"""Shared SatNOGS request coordination across live and backfill workers."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from math import ceil
from typing import Any, Literal

import httpx


class CoordinatedRateLimitError(RuntimeError):
    def __init__(self, retry_after_seconds: int) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"SatNOGS request throttled; retry after {retry_after_seconds}s")


def _parse_retry_after(value: str | None) -> int:
    if not value:
        return 60
    try:
        return max(1, int(value))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return 60
    return max(1, ceil(retry_at.timestamp() - time.time()))


@dataclass(frozen=True)
class _RequestJob:
    owner: Literal["live", "backfill"]
    url: str
    params: dict[str, Any] | None
    headers: dict[str, str] | None


class SatnogsRequestCoordinator:
    """Single HTTP client and global rate-limit state for SatNOGS work."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self.client = client or httpx.Client(timeout=30.0)
        self._lock = threading.Lock()
        self._retry_after_until_monotonic = 0.0
        self._turn: Literal["live", "backfill"] = "live"
        self._waiting: dict[str, int] = {"live": 0, "backfill": 0}

    def get(
        self,
        url: str,
        *,
        owner: Literal["live", "backfill"] = "live",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        job = _RequestJob(owner=owner, url=url, params=params, headers=headers)
        return self._execute(job)

    def _execute(self, job: _RequestJob) -> httpx.Response:
        with self._lock:
            now = time.monotonic()
            if now < self._retry_after_until_monotonic:
                raise CoordinatedRateLimitError(ceil(self._retry_after_until_monotonic - now))
            self._waiting[job.owner] += 1
            try:
                opposite = "backfill" if job.owner == "live" else "live"
                if self._turn != job.owner and self._waiting[opposite] > 0:
                    self._turn = opposite
                response = self.client.get(job.url, params=job.params, headers=job.headers)
                self._turn = opposite
                if response.status_code == 429:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    self._retry_after_until_monotonic = time.monotonic() + retry_after
                    raise CoordinatedRateLimitError(retry_after)
                return response
            finally:
                self._waiting[job.owner] -= 1


class CoordinatedHttpClient:
    """httpx.Client-compatible shim used by SatnogsNetworkConnector."""

    def __init__(self, coordinator: SatnogsRequestCoordinator, *, owner: Literal["live", "backfill"]) -> None:
        self.coordinator = coordinator
        self.owner = owner

    def get(self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> httpx.Response:
        return self.coordinator.get(url, owner=self.owner, params=params, headers=headers)
