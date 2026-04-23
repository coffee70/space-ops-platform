"""Platform-owned NATS messaging helpers."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import uuid4

from pydantic import BaseModel, Field

from app.config import get_settings

logger = logging.getLogger(__name__)


class EventEnvelope(BaseModel):
    """Stable event envelope for platform internal messaging."""

    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex}")
    event_type: str
    subject: str
    emitted_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    payload: dict[str, Any]


class Subjects:
    """Canonical subject names."""

    TELEMETRY_MEASUREMENT = "platform.telemetry.measurement"
    TELEMETRY_UPDATE = "platform.telemetry.update"
    TELEMETRY_ALERT = "platform.telemetry.alert"
    FEED_HEALTH = "platform.telemetry.feed_health"
    ORBIT_STATUS = "platform.telemetry.orbit.status"
    ORBIT_RESET = "platform.telemetry.orbit.reset"


MessageHandler = Callable[[EventEnvelope], Awaitable[None]]


@dataclass
class _Subscription:
    subject: str
    callback: Any


class PlatformMessaging:
    """Small NATS wrapper used by backend services."""

    def __init__(self) -> None:
        self._client = None
        self._subscriptions: list[_Subscription] = []
        self._connect_lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        async with self._connect_lock:
            if self._client is not None:
                return
            from nats.aio.client import Client as NATS

            client = NATS()
            await client.connect(get_settings().nats_url, connect_timeout=2, max_reconnect_attempts=-1)
            self._client = client
            self._loop = asyncio.get_running_loop()
            logger.info("Connected to NATS at %s", get_settings().nats_url)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.drain()
            self._client = None
            self._subscriptions = []

    async def publish(self, subject: str, *, event_type: str, payload: dict[str, Any]) -> None:
        await self.connect()
        envelope = EventEnvelope(event_type=event_type, subject=subject, payload=payload)
        assert self._client is not None
        await self._client.publish(subject, envelope.model_dump_json().encode("utf-8"))

    def publish_nowait(self, subject: str, *, event_type: str, payload: dict[str, Any]) -> None:
        """Schedule a publish onto the broker loop from sync or async code."""

        if self._loop is None:
            return
        future = asyncio.run_coroutine_threadsafe(
            self.publish(subject, event_type=event_type, payload=payload),
            self._loop,
        )

        def _swallow_errors(result_future) -> None:
            try:
                result_future.result()
            except Exception:
                logger.exception("Failed to publish event %s on %s", event_type, subject)

        future.add_done_callback(_swallow_errors)

    async def subscribe(self, subject: str, handler: MessageHandler) -> None:
        await self.connect()

        async def _callback(message) -> None:
            try:
                payload = json.loads(message.data.decode("utf-8"))
                envelope = EventEnvelope.model_validate(payload)
                await handler(envelope)
            except Exception:
                logger.exception("Failed to handle NATS message on %s", subject)

        assert self._client is not None
        subscription = await self._client.subscribe(subject, cb=_callback)
        self._subscriptions.append(_Subscription(subject=subject, callback=subscription))


_messaging: PlatformMessaging | None = None


def get_messaging() -> PlatformMessaging:
    global _messaging
    if _messaging is None:
        _messaging = PlatformMessaging()
    return _messaging
