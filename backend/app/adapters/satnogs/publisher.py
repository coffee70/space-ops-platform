"""Realtime ingest publishing with retry and DLQ support."""

from __future__ import annotations

from dataclasses import dataclass
from time import sleep
from typing import Any

import httpx

from app.adapters.satnogs.config import PublisherConfig
from app.adapters.satnogs.dlq import FilesystemDlq
from app.adapters.satnogs.models import TelemetryEvent


@dataclass(slots=True)
class PublishResult:
    success: bool
    attempts: int
    status_code: int | None = None
    response_body: str | None = None


class IngestPublisher:
    def __init__(
        self,
        *,
        ingest_url: str,
        config: PublisherConfig,
        dlq: FilesystemDlq,
        client: httpx.Client | None = None,
    ) -> None:
        self.ingest_url = ingest_url
        self.config = config
        self.dlq = dlq
        self.client = client or httpx.Client(timeout=config.timeout_seconds)

    def publish(self, events: list[TelemetryEvent], *, context: dict[str, Any]) -> PublishResult:
        payload = {"events": [event.to_payload() for event in events]}
        attempts = 0
        backoff = self.config.retry.backoff_seconds
        retryable = set(self.config.retry.retryable_status_codes)

        while attempts < self.config.retry.max_attempts:
            attempts += 1
            try:
                response = self.client.post(self.ingest_url, json=payload)
            except httpx.TimeoutException as exc:
                if attempts >= self.config.retry.max_attempts:
                    self.dlq.write("batch", {"request": payload, "context": context, "error": repr(exc), "attempts": attempts})
                    return PublishResult(success=False, attempts=attempts, response_body=repr(exc))
                sleep(backoff)
                backoff *= self.config.retry.backoff_multiplier
                continue

            if 200 <= response.status_code < 300:
                return PublishResult(success=True, attempts=attempts, status_code=response.status_code, response_body=response.text)

            if response.status_code < 500 and response.status_code not in retryable:
                self.dlq.write(
                    "batch",
                    {
                        "request": payload,
                        "context": context,
                        "status_code": response.status_code,
                        "response_body": response.text,
                        "attempts": attempts,
                    },
                )
                return PublishResult(
                    success=False,
                    attempts=attempts,
                    status_code=response.status_code,
                    response_body=response.text,
                )

            if attempts >= self.config.retry.max_attempts:
                self.dlq.write(
                    "batch",
                    {
                        "request": payload,
                        "context": context,
                        "status_code": response.status_code,
                        "response_body": response.text,
                        "attempts": attempts,
                    },
                )
                return PublishResult(
                    success=False,
                    attempts=attempts,
                    status_code=response.status_code,
                    response_body=response.text,
                )

            sleep(backoff)
            backoff *= self.config.retry.backoff_multiplier

        return PublishResult(success=False, attempts=attempts)


class ObservationsPublisher:
    def __init__(
        self,
        *,
        batch_upsert_url: str,
        config: PublisherConfig,
        dlq: FilesystemDlq,
        client: httpx.Client | None = None,
    ) -> None:
        self.batch_upsert_url = batch_upsert_url
        self.config = config
        self.dlq = dlq
        self.client = client or httpx.Client(timeout=config.timeout_seconds)

    def publish(
        self,
        observations: list[dict[str, Any]],
        *,
        provider: str,
        replace_future_scheduled: bool = True,
        context: dict[str, Any],
    ) -> PublishResult:
        payload = {
            "provider": provider,
            "replace_future_scheduled": replace_future_scheduled,
            "observations": observations,
        }
        attempts = 0
        backoff = self.config.retry.backoff_seconds
        retryable = set(self.config.retry.retryable_status_codes)

        while attempts < self.config.retry.max_attempts:
            attempts += 1
            try:
                response = self.client.post(self.batch_upsert_url, json=payload)
            except httpx.TimeoutException as exc:
                if attempts >= self.config.retry.max_attempts:
                    self.dlq.write("observation-sync", {"request": payload, "context": context, "error": repr(exc), "attempts": attempts})
                    return PublishResult(success=False, attempts=attempts, response_body=repr(exc))
                sleep(backoff)
                backoff *= self.config.retry.backoff_multiplier
                continue

            if 200 <= response.status_code < 300:
                return PublishResult(success=True, attempts=attempts, status_code=response.status_code, response_body=response.text)

            if response.status_code < 500 and response.status_code not in retryable:
                self.dlq.write(
                    "observation-sync",
                    {
                        "request": payload,
                        "context": context,
                        "status_code": response.status_code,
                        "response_body": response.text,
                        "attempts": attempts,
                    },
                )
                return PublishResult(
                    success=False,
                    attempts=attempts,
                    status_code=response.status_code,
                    response_body=response.text,
                )

            if attempts >= self.config.retry.max_attempts:
                self.dlq.write(
                    "observation-sync",
                    {
                        "request": payload,
                        "context": context,
                        "status_code": response.status_code,
                        "response_body": response.text,
                        "attempts": attempts,
                    },
                )
                return PublishResult(
                    success=False,
                    attempts=attempts,
                    status_code=response.status_code,
                    response_body=response.text,
                )

            sleep(backoff)
            backoff *= self.config.retry.backoff_multiplier

        return PublishResult(success=False, attempts=attempts)


class SourceStatePublisher:
    def __init__(
        self,
        *,
        backfill_progress_url: str,
        live_state_url: str,
        config: PublisherConfig,
        client: httpx.Client | None = None,
    ) -> None:
        self.backfill_progress_url = backfill_progress_url
        self.live_state_url = live_state_url
        self.config = config
        self.client = client or httpx.Client(timeout=config.timeout_seconds)

    def publish_backfill_progress(self, payload: dict[str, Any]) -> PublishResult:
        return self._post(self.backfill_progress_url, payload)

    def publish_live_state(self, state: str, *, error: str | None = None) -> PublishResult:
        payload: dict[str, Any] = {"state": state}
        if error:
            payload["error"] = error
        return self._post(self.live_state_url, payload)

    def _post(self, url: str, payload: dict[str, Any]) -> PublishResult:
        attempts = 0
        backoff = self.config.retry.backoff_seconds
        retryable = set(self.config.retry.retryable_status_codes)
        while attempts < self.config.retry.max_attempts:
            attempts += 1
            try:
                response = self.client.post(url, json=payload)
            except httpx.TimeoutException as exc:
                if attempts >= self.config.retry.max_attempts:
                    return PublishResult(success=False, attempts=attempts, response_body=repr(exc))
                sleep(backoff)
                backoff *= self.config.retry.backoff_multiplier
                continue
            if 200 <= response.status_code < 300:
                return PublishResult(success=True, attempts=attempts, status_code=response.status_code, response_body=response.text)
            if response.status_code < 500 and response.status_code not in retryable:
                return PublishResult(success=False, attempts=attempts, status_code=response.status_code, response_body=response.text)
            if attempts >= self.config.retry.max_attempts:
                return PublishResult(success=False, attempts=attempts, status_code=response.status_code, response_body=response.text)
            sleep(backoff)
            backoff *= self.config.retry.backoff_multiplier
        return PublishResult(success=False, attempts=attempts)
