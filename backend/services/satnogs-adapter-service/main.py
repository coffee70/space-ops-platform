"""SatNOGS adapter managed service entrypoint."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException
from pydantic import BaseModel

from app.adapters.satnogs.config import load_config
from app.adapters.satnogs.main import build_runner
from platform_common.web import create_service_app

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "app/adapters/satnogs/config.example.yaml"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bool_env(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _config_path() -> str:
    return os.environ.get("SATNOGS_ADAPTER_CONFIG", DEFAULT_CONFIG_PATH)


class AdapterRuntimeState(BaseModel):
    live_enabled: bool = False
    vehicle_name: str | None = None
    norad_id: int | None = None
    transmitter_uuid_present: bool = False
    source_resolved: bool = False
    source_id: str | None = None
    last_live_poll_at: str | None = None
    last_observation_sync_at: str | None = None
    last_successful_publish_at: str | None = None
    last_error: str | None = None
    dlq_counts: dict[str, int] = {}


state = AdapterRuntimeState()


def _refresh_config_status() -> None:
    try:
        config = load_config(_config_path())
    except Exception as exc:
        state.last_error = f"config: {exc!r}"
        return
    state.vehicle_name = config.vehicle.name
    state.norad_id = config.vehicle.norad_id
    state.transmitter_uuid_present = bool(config.satnogs.transmitter_uuid.strip())


def _dlq_counts() -> dict[str, int]:
    try:
        config = load_config(_config_path())
    except Exception:
        return {}
    root = Path(config.dlq.root_dir)
    if not root.exists():
        return {}
    counts: dict[str, int] = {}
    for child in root.iterdir():
        if child.is_dir():
            counts[child.name] = len(list(child.glob("*.json")))
    return counts


async def _run_live_worker() -> None:
    try:
        runner = await asyncio.to_thread(build_runner, _config_path())
        state.source_id = runner.source_contract.id
        state.source_resolved = True
        await asyncio.to_thread(runner.run_forever)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("SatNOGS live worker failed")
        state.last_error = repr(exc)


@asynccontextmanager
async def lifespan(_app):
    state.live_enabled = _bool_env("SATNOGS_LIVE_ENABLED", default=False)
    _refresh_config_status()
    task: asyncio.Task[None] | None = None
    if state.live_enabled:
        task = asyncio.create_task(_run_live_worker())
    yield
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = create_service_app(
    title="SatNOGS Adapter Service",
    description="Layer 2 adapter for SatNOGS observation ingestion.",
    lifespan=lifespan,
)


@app.get("/status")
def get_status() -> dict:
    _refresh_config_status()
    state.live_enabled = _bool_env("SATNOGS_LIVE_ENABLED", default=False)
    state.dlq_counts = _dlq_counts()
    return state.model_dump()


@app.post("/run-once")
async def run_once() -> dict:
    try:
        runner = await asyncio.to_thread(build_runner, _config_path())
        state.source_id = runner.source_contract.id
        state.source_resolved = True
        await asyncio.to_thread(runner.run_live_once)
        state.last_live_poll_at = _utc_now()
        state.last_observation_sync_at = state.last_live_poll_at
        state.last_successful_publish_at = None
        state.last_error = None
        return {"status": "completed", "source_id": state.source_id}
    except Exception as exc:
        state.last_error = repr(exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/sync-observations")
async def sync_observations() -> dict:
    try:
        runner = await asyncio.to_thread(build_runner, _config_path())
        state.source_id = runner.source_contract.id
        state.source_resolved = True
        await asyncio.to_thread(runner._sync_upcoming_observations_if_due)
        state.last_observation_sync_at = _utc_now()
        state.last_error = None
        return {"status": "completed", "source_id": state.source_id}
    except Exception as exc:
        state.last_error = repr(exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/replay-dlq")
async def replay_dlq(max_age_seconds: int | None = None) -> dict:
    try:
        runner = await asyncio.to_thread(build_runner, _config_path())
        state.source_id = runner.source_contract.id
        state.source_resolved = True
        replayed = await asyncio.to_thread(runner.replay_batch_dlq, max_age_seconds=max_age_seconds)
        state.last_error = None
        return {"status": "completed", "source_id": state.source_id, "replayed": replayed}
    except Exception as exc:
        state.last_error = repr(exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
