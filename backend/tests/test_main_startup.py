from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.modules.setdefault(
    "app.services.embedding_service",
    SimpleNamespace(SentenceTransformerEmbeddingProvider=object),
)

from app import main as main_module


class _FakeSession:
    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeHub:
    def set_loop(self, _loop) -> None:
        return None

    def schedule_telemetry_update(self, *_args, **_kwargs) -> None:
        return None

    def schedule_orbit_status(self, *_args, **_kwargs) -> None:
        return None

    def schedule_alert_event(self, *_args, **_kwargs) -> None:
        return None

    def schedule_feed_status(self, *_args, **_kwargs) -> None:
        return None

    async def stop(self) -> None:
        return None


class _FakeBus:
    def subscribe_alerts(self, _handler) -> None:
        return None

    def unsubscribe_alerts(self, _handler) -> None:
        return None


class _FakeProcessor:
    def __init__(self) -> None:
        self._bus = _FakeBus()

    def register_telemetry_update_handler(self, _handler) -> None:
        return None

    def unregister_telemetry_update_handler(self, _handler) -> None:
        return None

    def stop(self) -> None:
        return None


class _FakeFeedTracker:
    def set_on_transition(self, _handler) -> None:
        return None

    def get_all_statuses(self) -> list[dict]:
        return []

    def get_status(self, _source_id: str) -> dict:
        return {}


def _install_lifespan_dependencies(monkeypatch, realtime_service_module) -> None:
    fake_session = _FakeSession()
    monkeypatch.setitem(
        sys.modules,
        "app.database",
        SimpleNamespace(get_session_factory=lambda: lambda: fake_session),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.realtime",
        SimpleNamespace(get_realtime_processor=lambda: _FakeProcessor()),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.realtime.feed_health",
        SimpleNamespace(get_feed_health_tracker=lambda: _FakeFeedTracker()),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.realtime.ws_hub",
        SimpleNamespace(get_ws_hub=lambda: _FakeHub()),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.services.ops_events_service",
        SimpleNamespace(write_event=lambda *args, **kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.services.realtime_service",
        realtime_service_module,
    )
    monkeypatch.setitem(
        sys.modules,
        "app.orbit",
        SimpleNamespace(register_on_status_change=lambda _handler: None),
    )


def test_lifespan_runs_repair_then_auto_register(monkeypatch) -> None:
    calls: list[object] = []
    provider = object()
    realtime_service_module = SimpleNamespace(
        repair_registered_sources_on_startup=lambda session: calls.append(("repair", session)) or [],
        auto_register_sources_from_configs=lambda session, embedding_provider: calls.append(
            ("auto_register", session, embedding_provider)
        )
        or {"created": [], "existing": [], "invalid": [], "skipped": [], "examined": 0},
        refresh_source_embeddings=lambda *args, **kwargs: None,
    )
    _install_lifespan_dependencies(monkeypatch, realtime_service_module)
    monkeypatch.setitem(
        sys.modules,
        "app.services.embedding_service",
        SimpleNamespace(SentenceTransformerEmbeddingProvider=lambda: provider),
    )

    async def run_lifespan() -> None:
        async with main_module.lifespan(main_module.app):
            pass

    asyncio.run(run_lifespan())

    assert [call[0] for call in calls] == ["repair", "auto_register"]
    assert calls[1][2] is provider


def test_lifespan_skips_auto_register_when_embedding_provider_init_fails(monkeypatch) -> None:
    bootstrap = MagicMock(return_value=[])
    auto_register = MagicMock()
    realtime_service_module = SimpleNamespace(
        repair_registered_sources_on_startup=bootstrap,
        auto_register_sources_from_configs=auto_register,
        refresh_source_embeddings=lambda *args, **kwargs: None,
    )
    _install_lifespan_dependencies(monkeypatch, realtime_service_module)

    class BrokenProvider:
        def __init__(self) -> None:
            raise RuntimeError("model unavailable")

    monkeypatch.setitem(
        sys.modules,
        "app.services.embedding_service",
        SimpleNamespace(SentenceTransformerEmbeddingProvider=BrokenProvider),
    )

    async def run_lifespan() -> None:
        async with main_module.lifespan(main_module.app):
            pass

    asyncio.run(run_lifespan())

    bootstrap.assert_called_once()
    auto_register.assert_not_called()
