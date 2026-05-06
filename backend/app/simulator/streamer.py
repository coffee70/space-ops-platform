"""Background telemetry streamer with pause/resume/stop support."""

import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.simulator.lib.audit import audit_log
from app.simulator.orbit import position_at_time
from app.simulator.telemetry_definitions import (
    POSITION_MAPPING,
    RATES_HZ,
    SCENARIOS,
    TELEMETRY_DEFINITIONS,
    load_definition,
)
from telemetry_catalog.coordinates import ecef_to_eci_m
from telemetry_catalog.definitions import channel_rate_hz, lla_to_ecef_m

# Retry on timeout/connection errors; backoff avoids hammering a slow backend
def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[502, 503, 504],
        allowed_methods=["POST"],
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

# Default circular LEO orbit for position telemetry
_ORBIT_PERIOD_SEC = 90.0 * 60.0  # ~90 min
_ORBIT_INCLINATION_DEG = 51.6
_ORBIT_ALT_M = 400_000.0
_ORBIT_NOISE_DEG = 0.00002
_ORBIT_NOISE_M = 2.0
_DEFAULT_POSITION_SAMPLE_PERIOD_SEC = 1.0


class StreamerState:
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"


class TelemetryStreamer:
    """Runs telemetry streaming in a background thread with pause/resume/stop."""

    def __init__(
        self,
        base_url: str,
        scenario: str = "nominal",
        duration: float = 300,
        speed: float = 1.0,
        drop_prob: float = 0.0,
        jitter: float = 0.1,
        vehicle_id: str = "",
        stream_id: str = "",
        packet_source: str | None = None,
        receiver_id: str | None = None,
        vehicle_config_path: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.ingest_url = f"{self.base_url}/telemetry/realtime/ingest"
        self.scenario_name = scenario
        if vehicle_config_path:
            definition = load_definition(vehicle_config_path)
            self.telemetry_rows = [
                (
                    channel.name,
                    channel.units,
                    channel.description,
                    channel.mean,
                    channel.std_dev,
                    channel.subsystem,
                    channel.red_low,
                    channel.red_high,
                )
                for channel in definition.channels
            ]
            self._rates = {
                channel.name: channel_rate_hz(channel) for channel in definition.channels
            }
            scenario_map = {
                name: scenario.model_dump()
                for name, scenario in definition.scenarios.items()
            }
            self.scenario = scenario_map.get(scenario)
            self.position_mapping = definition.position_mapping or POSITION_MAPPING
        else:
            self.telemetry_rows = TELEMETRY_DEFINITIONS
            self._rates = RATES_HZ.copy()
            scenario_map = SCENARIOS
            self.scenario = scenario_map.get(scenario)
            self.position_mapping = POSITION_MAPPING
        if self.scenario is None:
            self.scenario = next(iter(scenario_map.values()))
        self.duration = duration
        self.speed = speed
        self.drop_prob = drop_prob
        self.jitter = jitter
        self.vehicle_id = vehicle_id
        self.stream_id = stream_id
        self.packet_source = packet_source
        self.receiver_id = receiver_id
        self._state = StreamerState.IDLE
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # Not paused initially
        self._lock = threading.Lock()
        self._sim_elapsed = 0.0
        self._start_wall: float | None = None
        self._sim_epoch: datetime | None = None
        self._next_gps_emit_sim: float = 0.0
        self._paused_at_sim: float = 0.0
        self._session = _make_session()
        self._position_channel_names = self._build_position_channels()
        position_rates = [
            max(self._channel_rate(name), 0.01) for name in self._position_channel_names
        ]
        if position_rates:
            self.position_sample_period_sec = 1.0 / max(position_rates)
        else:
            self.position_sample_period_sec = _DEFAULT_POSITION_SAMPLE_PERIOD_SEC

    def _channel_rate(self, name: str) -> float:
        return self._rates.get(name, 0.5)

    def _build_position_channels(self) -> set[str]:
        mapping = self.position_mapping
        names: set[str] = set()
        if mapping and mapping.frame_type in {"ecef", "eci"}:
            names.update(
                filter(
                    None,
                    [
                        mapping.x_channel_name or "POS_X",
                        mapping.y_channel_name or "POS_Y",
                        mapping.z_channel_name or "POS_Z",
                    ],
                )
            )
        elif mapping:
            names.update(
                filter(
                    None,
                    [
                        mapping.lat_channel_name or "GPS_LAT",
                        mapping.lon_channel_name or "GPS_LON",
                        mapping.alt_channel_name or "GPS_ALT",
                    ],
                )
            )
        else:
            names.update({"GPS_LAT", "GPS_LON", "GPS_ALT"})
        return names

    def _append_position_batch(
        self,
        batch: list[dict[str, Any]],
        *,
        seq: int,
        sim_elapsed: float,
        generation_time: datetime,
        orbit_profile: str,
    ) -> int:
        """Emit a coherent position frame so position/orbit logic sees aligned samples."""
        lat_deg, lon_deg, alt_m = position_at_time(
            sim_elapsed,
            period_sec=_ORBIT_PERIOD_SEC,
            inclination_deg=_ORBIT_INCLINATION_DEG,
            alt_m=_ORBIT_ALT_M,
            profile=orbit_profile,
        )
        noisy_lat = lat_deg + random.gauss(0, _ORBIT_NOISE_DEG)
        noisy_lon = lon_deg + random.gauss(0, _ORBIT_NOISE_DEG)
        noisy_alt = alt_m + random.gauss(0, _ORBIT_NOISE_M)
        mapping = self.position_mapping
        if mapping and mapping.frame_type in {"ecef", "eci"}:
            x_m, y_m, z_m = lla_to_ecef_m(noisy_lat, noisy_lon, noisy_alt)
            if mapping.frame_type == "eci":
                x_m, y_m, z_m = ecef_to_eci_m(x_m, y_m, z_m, generation_time)
            frame_values = {
                mapping.x_channel_name or "POS_X": x_m,
                mapping.y_channel_name or "POS_Y": y_m,
                mapping.z_channel_name or "POS_Z": z_m,
            }
        elif mapping:
            frame_values = {
                mapping.lat_channel_name or "GPS_LAT": noisy_lat,
                mapping.lon_channel_name or "GPS_LON": noisy_lon,
                mapping.alt_channel_name or "GPS_ALT": noisy_alt,
            }
        else:
            frame_values = {
                "GPS_LAT": noisy_lat,
                "GPS_LON": noisy_lon,
                "GPS_ALT": noisy_alt,
            }
        for name, value in frame_values.items():
            seq += 1
            batch.append(
                {
                    "source_id": self.vehicle_id,
                    "stream_id": self.stream_id,
                    "channel_name": name,
                    "generation_time": generation_time.isoformat(),
                    "value": value,
                    "quality": "valid",
                    "sequence": seq,
                    "packet_source": self.packet_source,
                    "receiver_id": self.receiver_id,
                }
            )
        return seq

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def sim_elapsed(self) -> float:
        with self._lock:
            if self._state == StreamerState.PAUSED:
                return self._paused_at_sim
            if self._state == StreamerState.RUNNING and self._start_wall is not None:
                return (time.monotonic() - self._start_wall) * self.speed
            return self._sim_elapsed

    def _run_loop(self) -> None:
        dropout_window = self.scenario.get("dropout")
        events = self.scenario.get("events", [])
        anomaly_frac = self.scenario.get("anomaly_fraction", 0.02)
        orbit_profile = self.scenario.get("orbit_profile", "nominal")
        batch_size = 20
        batch: list[dict[str, Any]] = []
        seq = 0
        batch_num = 0

        while not self._stop_event.is_set():
            self._pause_event.wait()
            if self._stop_event.is_set():
                break

            with self._lock:
                if self._start_wall is None:
                    break
                sim_elapsed = (time.monotonic() - self._start_wall) * self.speed

            if self.duration > 0 and sim_elapsed >= self.duration:
                with self._lock:
                    self._state = StreamerState.IDLE
                audit_log("streamer.state", old_state="running", new_state="idle", reason="duration_reached")
                break

            if self._sim_epoch is None:
                break
            generation_time = self._sim_epoch + timedelta(seconds=sim_elapsed)

            if dropout_window and dropout_window["t0"] <= sim_elapsed < dropout_window["t0"] + dropout_window["duration"]:
                time.sleep(0.1)
                continue

            if self.drop_prob > 0 and random.random() < self.drop_prob:
                time.sleep(0.05)
                continue

            while sim_elapsed >= self._next_gps_emit_sim:
                gps_elapsed = self._next_gps_emit_sim
                gps_generation_time = self._sim_epoch + timedelta(seconds=gps_elapsed)
                seq = self._append_position_batch(
                    batch,
                    seq=seq,
                    sim_elapsed=gps_elapsed,
                    generation_time=gps_generation_time,
                    orbit_profile=orbit_profile,
                )
                self._next_gps_emit_sim += self.position_sample_period_sec

            for row in self.telemetry_rows:
                name, mean, std = row[0], row[3], row[4]
                if name in self._position_channel_names:
                    continue
                rate = self._channel_rate(name)
                dt = 0.1
                if random.random() > rate * dt:
                    continue

                value = random.gauss(mean, std)
                for ev in events:
                    if ev["t0"] <= sim_elapsed < ev["t0"] + ev.get("duration", 999):
                        if name in ev["channels"]:
                            if ev["type"] == "offset":
                                value += ev["magnitude"]
                            elif ev["type"] == "ramp":
                                progress = (sim_elapsed - ev["t0"]) / ev["duration"]
                                value += ev["magnitude"] * min(1.0, progress)
                            elif ev["type"] == "set":
                                value = ev["magnitude"]

                # Keep position telemetry physically plausible; orbit anomalies come from explicit orbit profiles.
                if random.random() < anomaly_frac:
                    value += random.choice([-1, 1]) * random.uniform(2.5, 5.0) * std

                seq += 1
                batch.append({
                    "source_id": self.vehicle_id,
                    "stream_id": self.stream_id,
                    "channel_name": name,
                    "generation_time": generation_time.isoformat(),
                    "value": value,
                    "quality": "valid",
                    "sequence": seq,
                    "packet_source": self.packet_source,
                    "receiver_id": self.receiver_id,
                })

            if len(batch) >= batch_size:
                batch_num += 1
                try:
                    r = self._session.post(
                        self.ingest_url,
                        json={"events": batch},
                        timeout=(5, 30),  # connect 5s, read 30s
                    )
                    if r.status_code == 200:
                        audit_log(
                            "ingest.sent",
                            direction="simulator_to_backend",
                            count=len(batch),
                            vehicle_id=self.vehicle_id,
                            stream_id=self.stream_id,
                            status_code=r.status_code,
                        )
                    else:
                        audit_log(
                            "ingest.error",
                            level="warning",
                            count=len(batch),
                            vehicle_id=self.vehicle_id,
                            stream_id=self.stream_id,
                            status_code=r.status_code,
                        )
                    batch = []
                except requests.RequestException as e:
                    audit_log(
                        "ingest.error",
                        level="warning",
                        count=len(batch),
                        vehicle_id=self.vehicle_id,
                        stream_id=self.stream_id,
                        error=str(e),
                    )
                    batch = []

            jitter_sleep = 0.1 * (1 + (random.random() - 0.5) * self.jitter * 2)
            time.sleep(jitter_sleep / self.speed)

        if batch:
            try:
                r = self._session.post(
                    self.ingest_url,
                    json={"events": batch},
                    timeout=(5, 30),
                )
                if r.status_code == 200:
                    audit_log(
                        "ingest.sent",
                        direction="simulator_to_backend",
                        count=len(batch),
                        vehicle_id=self.vehicle_id,
                        stream_id=self.stream_id,
                        status_code=r.status_code,
                    )
                else:
                    audit_log(
                        "ingest.error",
                        level="warning",
                        count=len(batch),
                        vehicle_id=self.vehicle_id,
                        stream_id=self.stream_id,
                        status_code=r.status_code,
                    )
            except Exception as e:
                audit_log(
                    "ingest.error",
                    level="warning",
                    count=len(batch),
                    vehicle_id=self.vehicle_id,
                    stream_id=self.stream_id,
                    error=str(e),
                )

        with self._lock:
            old_state = self._state
            self._state = StreamerState.IDLE
        audit_log("streamer.state", old_state=old_state, new_state="idle")

    def start(self) -> bool:
        with self._lock:
            if self._state != StreamerState.IDLE:
                return False
            self._state = StreamerState.RUNNING
            self._stop_event.clear()
            self._pause_event.set()
            self._start_wall = time.monotonic()
            self._sim_epoch = datetime.now(timezone.utc)
            self._next_gps_emit_sim = 0.0
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
        return True

    def pause(self) -> bool:
        with self._lock:
            if self._state != StreamerState.RUNNING:
                return False
            self._state = StreamerState.PAUSED
            self._paused_at_sim = (time.monotonic() - (self._start_wall or 0)) * self.speed
            self._pause_event.clear()
        return True

    def resume(self) -> bool:
        with self._lock:
            if self._state != StreamerState.PAUSED:
                return False
            self._state = StreamerState.RUNNING
            elapsed = self._paused_at_sim
            self._start_wall = time.monotonic() - (elapsed / self.speed)
            self._pause_event.set()
        return True

    def stop(self) -> bool:
        with self._lock:
            if self._state == StreamerState.IDLE:
                return True
            self._stop_event.set()
            self._pause_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self._session.close()
        with self._lock:
            self._state = StreamerState.IDLE
            self._thread = None
            self._sim_epoch = None
            self._next_gps_emit_sim = 0.0
        return True
