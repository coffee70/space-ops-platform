from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from app import main as main_module


class _FakeHub:
    def __init__(self) -> None:
        self.loop = None
        self.stopped = False

    def set_loop(self, loop) -> None:
        self.loop = loop

    def schedule_telemetry_update(self, *_args, **_kwargs) -> None:
        return None

    def schedule_alert_event(self, *_args, **_kwargs) -> None:
        return None

    def schedule_feed_status(self, *_args, **_kwargs) -> None:
        return None

    def schedule_orbit_status(self, *_args, **_kwargs) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True


class _FakeMessaging:
    def __init__(self) -> None:
        self.connect = AsyncMock()
        self.close = AsyncMock()
        self.subjects: list[str] = []

    async def subscribe(self, subject: str, _handler) -> None:
        self.subjects.append(subject)


def test_gateway_lifespan_wires_hub_and_messaging(monkeypatch) -> None:
    hub = _FakeHub()
    messaging = _FakeMessaging()
    monkeypatch.setattr(main_module, "get_ws_hub", lambda: hub)
    monkeypatch.setattr(main_module, "get_messaging", lambda: messaging)

    async def run_lifespan() -> None:
        async with main_module.lifespan(main_module.app):
            assert hub.loop is asyncio.get_running_loop()

    asyncio.run(run_lifespan())

    messaging.connect.assert_awaited_once()
    messaging.close.assert_awaited_once()
    assert hub.stopped is True
    assert messaging.subjects == [
        main_module.Subjects.TELEMETRY_UPDATE,
        main_module.Subjects.TELEMETRY_ALERT,
        main_module.Subjects.FEED_HEALTH,
        main_module.Subjects.ORBIT_STATUS,
    ]
