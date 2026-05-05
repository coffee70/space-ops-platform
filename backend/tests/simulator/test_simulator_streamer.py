"""Tests for simulator position frame emission."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.simulator import streamer as streamer_module
from telemetry_catalog.coordinates import eci_to_ecef_m


def test_append_position_batch_emits_eci_channels(monkeypatch) -> None:
    monkeypatch.setattr(
        streamer_module,
        "POSITION_MAPPING",
        SimpleNamespace(
            frame_type="eci",
            x_channel_name="POS_ECI_X",
            y_channel_name="POS_ECI_Y",
            z_channel_name="POS_ECI_Z",
            lat_channel_name=None,
            lon_channel_name=None,
            alt_channel_name=None,
        ),
    )
    monkeypatch.setattr(
        streamer_module,
        "position_at_time",
        lambda *args, **kwargs: (0.0, 0.0, 400_000.0),
    )
    monkeypatch.setattr(streamer_module.random, "gauss", lambda mean, _stddev: mean)

    streamer = streamer_module.TelemetryStreamer(
        base_url="http://example.test",
        vehicle_id="sim-src",
        stream_id="sim-src",
    )
    batch: list[dict[str, object]] = []
    generation_time = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)

    streamer._append_position_batch(
        batch,
        seq=0,
        sim_elapsed=0.0,
        generation_time=generation_time,
        orbit_profile="nominal",
    )

    assert {item["channel_name"] for item in batch} == {
        "POS_ECI_X",
        "POS_ECI_Y",
        "POS_ECI_Z",
    }
    emitted = {item["channel_name"]: float(item["value"]) for item in batch}
    x_ecef, y_ecef, z_ecef = eci_to_ecef_m(
        emitted["POS_ECI_X"],
        emitted["POS_ECI_Y"],
        emitted["POS_ECI_Z"],
        generation_time,
    )
    assert x_ecef == pytest.approx(6778137.0, abs=0.1)
    assert y_ecef == pytest.approx(0.0, abs=0.1)
    assert z_ecef == pytest.approx(0.0, abs=0.1)
