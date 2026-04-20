"""Tests for orbit validation module."""

import math
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from app.utils.coordinates import ecef_to_lla, eci_to_lla
from app.orbit.math import (
    EARTH_RADIUS_KM,
    MU_KM3_S2,
    lla_to_ecef_km,
    velocity_from_positions,
    orbital_parameters,
)
from app.orbit.classifier import classify_orbit
from app.orbit.anomaly import check_anomaly
import app.orbit as orbit_module
from app.orbit import submit_position_sample, get_status
from app.orbit.state import get_orbit_state
from app.realtime.processor import RealtimeProcessor
from telemetry_catalog.coordinates import ecef_to_eci_m

DROGONSAT_SOURCE_ID = "test-drogonsat-source"
RHAEGALSAT_SOURCE_ID = "test-rhaegalsat-source"


class TestLlaToEcefKm:
    def test_equator_zero_alt(self) -> None:
        x, y, z = lla_to_ecef_km(0.0, 0.0, 0.0)
        assert x == pytest.approx(EARTH_RADIUS_KM, rel=1e-5)
        assert y == 0.0
        assert z == 0.0

    def test_roundtrip_with_ecef_to_lla(self) -> None:
        lat, lon, alt_m = 45.0, 10.0, 400_000.0  # 400 km
        x_km, y_km, z_km = lla_to_ecef_km(lat, lon, alt_m)
        x_m, y_m, z_m = x_km * 1000, y_km * 1000, z_km * 1000
        lat2, lon2, alt2 = ecef_to_lla(x_m, y_m, z_m)
        assert lat2 == pytest.approx(lat, abs=1e-6)
        assert lon2 == pytest.approx(lon, abs=1e-6)
        assert alt2 == pytest.approx(alt_m, abs=0.01)

    def test_roundtrip_with_eci_to_lla(self) -> None:
        lat, lon, alt_m = 12.0, -45.0, 550_000.0
        ts = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
        x_km, y_km, z_km = lla_to_ecef_km(lat, lon, alt_m)
        x_eci, y_eci, z_eci = ecef_to_eci_m(x_km * 1000, y_km * 1000, z_km * 1000, ts)
        lat2, lon2, alt2 = eci_to_lla(x_eci, y_eci, z_eci, ts)
        assert lat2 == pytest.approx(lat, abs=1e-6)
        assert lon2 == pytest.approx(lon, abs=1e-6)
        assert alt2 == pytest.approx(alt_m, abs=0.01)


class TestVelocityFromPositions:
    def test_basic(self) -> None:
        r_prev = (7000.0, 0.0, 0.0)
        r_curr = (7000.0, 100.0, 0.0)  # 100 km in 1 s -> 100 km/s in y
        v = velocity_from_positions(r_prev, r_curr, 1.0)
        assert v[0] == 0.0
        assert v[1] == 100.0
        assert v[2] == 0.0

    def test_zero_dt(self) -> None:
        v = velocity_from_positions((0, 0, 0), (1, 1, 1), 0.0)
        assert v == (0.0, 0.0, 0.0)


class TestOrbitalParameters:
    def test_circular_leo(self) -> None:
        # ~400 km altitude circular orbit: r = 6378 + 400 = 6778 km, v ~ sqrt(mu/r) ~ 7.67 km/s
        r = (6778.0, 0.0, 0.0)
        v_expected = math.sqrt(MU_KM3_S2 / 6778.0)
        v = (0.0, v_expected, 0.0)  # tangential
        params = orbital_parameters(r, v)
        assert params["perigee_alt_km"] == pytest.approx(400.0, abs=1.0)
        assert params["apogee_alt_km"] == pytest.approx(400.0, abs=1.0)
        assert params["eccentricity"] == pytest.approx(0.0, abs=0.01)
        assert params["speed_km_s"] == pytest.approx(v_expected, rel=0.01)

    def test_escape_trajectory(self) -> None:
        r = (7000.0, 0.0, 0.0)
        v = (15.0, 0.0, 0.0)  # very high radial velocity
        params = orbital_parameters(r, v)
        assert params["specific_energy_km2_s2"] >= 0


class TestClassifier:
    def test_leo(self) -> None:
        assert classify_orbit(300.0, 400.0) == "LEO"
        assert classify_orbit(160.0, 500.0) == "LEO"

    def test_meo(self) -> None:
        assert classify_orbit(5000.0, 10000.0) == "MEO"

    def test_geo(self) -> None:
        assert classify_orbit(35700.0, 35800.0) == "GEO"


class TestAnomaly:
    def test_valid_leo(self) -> None:
        status, reason = check_anomaly(
            True, -30.0, 7.67, 400.0, 400.0, 0.001, "LEO"
        )
        assert status == "VALID"
        assert reason == ""

    def test_escape(self) -> None:
        status, reason = check_anomaly(
            True, 0.1, 12.0, 400.0, 400.0, 0.5, "LEO"
        )
        assert status == "ESCAPE_TRAJECTORY"
        assert "energy" in reason.lower() or "unbound" in reason.lower()

    def test_suborbital(self) -> None:
        status, reason = check_anomaly(
            True, -20.0, 5.0, 500.0, 500.0, 0.0, "LEO"
        )
        assert status == "SUBORBITAL"
        assert "7" in reason or "velocity" in reason.lower()

    def test_orbit_decay(self) -> None:
        status, reason = check_anomaly(
            True, -25.0, 7.5, 200.0, 100.0, 0.01, "LEO"
        )
        assert status == "ORBIT_DECAY"
        assert "120" in reason or "perigee" in reason.lower()

    def test_highly_elliptical_leo(self) -> None:
        status, reason = check_anomaly(
            True, -15.0, 8.0, 500.0, 400.0, 0.25, "LEO"
        )
        assert status == "HIGHLY_ELLIPTICAL"
        assert "0.2" in reason or "eccentricity" in reason.lower()


class TestSubmitAndGetStatus:
    def test_submit_position_sample_notifies_registered_callbacks(
        self,
        monkeypatch,
    ) -> None:
        state = get_orbit_state()
        state._status.clear()
        state._buffers.clear()

        seen = []
        monkeypatch.setattr(
            orbit_module,
            "_on_status_callbacks",
            [lambda source_id, payload: seen.append((source_id, payload))],
        )

        submit_position_sample("test_src", 1000.0, 45.0, 10.0, 400_000.0)

        assert seen == [
            (
                "test_src",
                {
                    "source_id": "test_src",
                    "status": "INSUFFICIENT_DATA",
                    "reason": "Need at least 2 position samples to compute orbit",
                    "orbit_type": None,
                    "perigee_km": None,
                    "apogee_km": None,
                    "eccentricity": None,
                    "velocity_kms": None,
                    "period_sec": None,
                },
            )
        ]

    def test_insufficient_data(self) -> None:
        # Reset state so we don't leak from other tests
        state = get_orbit_state()
        state._status.clear()
        state._buffers.clear()
        submit_position_sample("test_src", 1000.0, 45.0, 10.0, 400_000.0)
        data = get_status("test_src")
        assert data["source_id"] == "test_src"
        assert data["status"] == "INSUFFICIENT_DATA"
        assert "2" in data["reason"] or "sample" in data["reason"].lower()

    def test_two_samples_valid_leo(self) -> None:
        state = get_orbit_state()
        state._status.clear()
        state._buffers.clear()
        # Two samples ~1 s apart at ~400 km altitude (circular LEO)
        submit_position_sample("leo_src", 1000.0, 0.0, 0.0, 400_000.0)
        submit_position_sample("leo_src", 1001.0, 0.0, 0.05, 400_000.0)  # small lon change
        data = get_status("leo_src")
        st = data
        assert "status" in st
        assert "reason" in st
        assert "source_id" in st
        assert st["source_id"] == "leo_src"


class TestRealtimeProcessorOrbitHandoff:
    def test_submit_orbit_sample_uses_logical_source_for_stream_ids(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            "app.realtime.processor.get_realtime_bus",
            lambda: MagicMock(),
        )
        processor = RealtimeProcessor()
        processor._orbit_mappings = {
            DROGONSAT_SOURCE_ID: {
                "frame_type": "gps_lla",
                "lat": "GPS_LAT",
                "lon": "GPS_LON",
                "alt": "GPS_ALT",
            },
        }
        processor._orbit_mappings_at = time.time()
        processor._orbit_position_buffer.clear()

        submitted = []

        monkeypatch.setattr(
            "app.orbit.submit_position_sample",
            lambda source_id, timestamp, lat, lon, alt: submitted.append(
                (source_id, timestamp, lat, lon, alt)
            ),
        )
        monkeypatch.setattr(
            "app.realtime.processor.resolve_active_stream_id",
            lambda _db, _vehicle_id: stream_id,
        )

        ts = datetime(2026, 3, 13, 17, 29, 17, tzinfo=timezone.utc)
        db = MagicMock()
        stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T17-29-17Z"
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_LAT",
            1.0,
            ts,
        )
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_LON",
            2.0,
            ts,
        )
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_ALT",
            400_000.0,
            ts,
        )

        assert submitted == [
            (DROGONSAT_SOURCE_ID, ts.timestamp(), 1.0, 2.0, 400_000.0),
        ]

    def test_submit_orbit_sample_does_not_mix_timestamps(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            "app.realtime.processor.get_realtime_bus",
            lambda: MagicMock(),
        )
        processor = RealtimeProcessor()
        processor._orbit_mappings = {
            DROGONSAT_SOURCE_ID: {
                "frame_type": "gps_lla",
                "lat": "GPS_LAT",
                "lon": "GPS_LON",
                "alt": "GPS_ALT",
            },
        }
        processor._orbit_mappings_at = time.time()
        processor._orbit_position_buffer.clear()

        submitted = []

        monkeypatch.setattr(
            "app.orbit.submit_position_sample",
            lambda source_id, timestamp, lat, lon, alt: submitted.append(
                (source_id, timestamp, lat, lon, alt)
            ),
        )
        monkeypatch.setattr(
            "app.realtime.processor.resolve_active_stream_id",
            lambda _db, _vehicle_id: stream_id,
        )

        ts1 = datetime(2026, 3, 13, 17, 29, 17, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 13, 17, 29, 18, tzinfo=timezone.utc)
        db = MagicMock()
        stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T17-29-17Z"
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_LAT",
            1.0,
            ts1,
        )
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_LON",
            2.0,
            ts2,
        )
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_ALT",
            400_000.0,
            ts1,
        )
        processor._maybe_submit_orbit_sample(
            db,
            DROGONSAT_SOURCE_ID,
            stream_id,
            "GPS_LON",
            2.1,
            ts1,
        )

        assert submitted == [
            (DROGONSAT_SOURCE_ID, ts1.timestamp(), 1.0, 2.1, 400_000.0),
        ]

    def test_older_run_events_do_not_overwrite_newer_active_run(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            "app.realtime.processor.get_realtime_bus",
            lambda: MagicMock(),
        )
        processor = RealtimeProcessor()
        processor._orbit_mappings = {
            DROGONSAT_SOURCE_ID: {
                "frame_type": "gps_lla",
                "lat": "GPS_LAT",
                "lon": "GPS_LON",
                "alt": "GPS_ALT",
            },
        }
        processor._orbit_mappings_at = time.time()
        processor._orbit_position_buffer.clear()

        submitted = []
        monkeypatch.setattr(
            "app.orbit.submit_position_sample",
            lambda source_id, timestamp, lat, lon, alt: submitted.append(
                (source_id, timestamp, lat, lon, alt)
            ),
        )
        monkeypatch.setattr(
            "app.realtime.processor.resolve_active_stream_id",
            lambda _db, _vehicle_id: f"{DROGONSAT_SOURCE_ID}-2026-03-13T19-26-42Z",
        )

        ts = datetime(2026, 3, 13, 19, 26, 16, tzinfo=timezone.utc)
        db = MagicMock()
        stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T19-26-16Z"
        for channel_name, value in (
            ("GPS_LAT", 1.0),
            ("GPS_LON", 2.0),
            ("GPS_ALT", 400_000.0),
        ):
            processor._maybe_submit_orbit_sample(
                db,
                DROGONSAT_SOURCE_ID,
                stream_id,
                channel_name,
                value,
                ts,
            )

        assert submitted == []

    def test_submit_orbit_sample_converts_ecef_mapping(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            "app.realtime.processor.get_realtime_bus",
            lambda: MagicMock(),
        )
        processor = RealtimeProcessor()
        processor._orbit_mappings = {
            RHAEGALSAT_SOURCE_ID: {
                "frame_type": "ecef",
                "x": "POS_ECEF_X",
                "y": "POS_ECEF_Y",
                "z": "POS_ECEF_Z",
            },
        }
        processor._orbit_mappings_at = time.time()
        processor._orbit_position_buffer.clear()

        submitted = []
        monkeypatch.setattr(
            "app.orbit.submit_position_sample",
            lambda source_id, timestamp, lat, lon, alt: submitted.append(
                (source_id, timestamp, lat, lon, alt)
            ),
        )
        monkeypatch.setattr(
            "app.realtime.processor.resolve_active_stream_id",
            lambda _db, _vehicle_id: stream_id,
        )

        ts = datetime(2026, 3, 13, 17, 29, 17, tzinfo=timezone.utc)
        db = MagicMock()
        stream_id = f"{RHAEGALSAT_SOURCE_ID}-2026-03-13T17-29-17Z"
        for channel_name, value in (
            ("POS_ECEF_X", 6778137.0),
            ("POS_ECEF_Y", 0.0),
            ("POS_ECEF_Z", 0.0),
        ):
            processor._maybe_submit_orbit_sample(
                db,
                RHAEGALSAT_SOURCE_ID,
                stream_id,
                channel_name,
                value,
                ts,
            )

        assert len(submitted) == 1
        assert submitted[0][0] == RHAEGALSAT_SOURCE_ID

    def test_submit_orbit_sample_converts_eci_mapping(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            "app.realtime.processor.get_realtime_bus",
            lambda: MagicMock(),
        )
        processor = RealtimeProcessor()
        processor._orbit_mappings = {
            RHAEGALSAT_SOURCE_ID: {
                "frame_type": "eci",
                "x": "POS_ECI_X",
                "y": "POS_ECI_Y",
                "z": "POS_ECI_Z",
            },
        }
        processor._orbit_mappings_at = time.time()
        processor._orbit_position_buffer.clear()

        submitted = []
        monkeypatch.setattr(
            "app.orbit.submit_position_sample",
            lambda source_id, timestamp, lat, lon, alt: submitted.append(
                (source_id, timestamp, lat, lon, alt)
            ),
        )
        monkeypatch.setattr(
            "app.realtime.processor.resolve_active_stream_id",
            lambda _db, _vehicle_id: stream_id,
        )

        ts = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
        x_eci, y_eci, z_eci = ecef_to_eci_m(6778137.0, 0.0, 0.0, ts)
        db = MagicMock()
        stream_id = f"{RHAEGALSAT_SOURCE_ID}-2026-03-15T12-00-00Z"
        for channel_name, value in (
            ("POS_ECI_X", x_eci),
            ("POS_ECI_Y", y_eci),
            ("POS_ECI_Z", z_eci),
        ):
            processor._maybe_submit_orbit_sample(
                db,
                RHAEGALSAT_SOURCE_ID,
                stream_id,
                channel_name,
                value,
                ts,
            )

        assert len(submitted) == 1
        assert submitted[0][0] == RHAEGALSAT_SOURCE_ID
        assert submitted[0][2] == pytest.approx(0.0, abs=1e-6)
        assert submitted[0][3] == pytest.approx(0.0, abs=1e-6)
        assert submitted[0][4] == pytest.approx(400_000.0, abs=0.1)
