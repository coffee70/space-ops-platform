"""SatNOGS transport connectors and frame extraction."""

from __future__ import annotations

import binascii
from email.utils import parsedate_to_datetime
from math import ceil
import re
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
import time
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import httpx

from app.adapters.satnogs.config import SatnogsConfig
from app.adapters.satnogs.models import FrameRecord, ObservationRecord
from app.adapters.satnogs.request_coordinator import CoordinatedRateLimitError

@dataclass(frozen=True, slots=True)
class ObservationPage:
    results: list[dict[str, Any]]
    next_url: str | None = None


class SatnogsRateLimitError(RuntimeError):
    """Raised when SatNOGS asks the client to wait before retrying."""

    def __init__(self, retry_after_seconds: int) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"SatNOGS request throttled; retry after {retry_after_seconds}s")


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


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
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(1, ceil((retry_at - datetime.now(timezone.utc)).total_seconds()))


def _timestamp_from_artifact_ref(ref: str) -> str | None:
    path = unquote(urlparse(ref).path or ref)
    for match in re.finditer(
        r"(?P<date>\d{4}[-_]\d{2}[-_]\d{2})[T_\- ](?P<time>\d{2}[:_\-]\d{2}[:_\-]\d{2}(?:[.,]\d+)?)(?P<zone>Z|[+-]\d{2}:?\d{2})?",
        path,
    ):
        date = match.group("date").replace("_", "-")
        time_part = match.group("time").replace("_", ":").replace("-", ":").replace(",", ".")
        zone = match.group("zone") or "Z"
        if zone != "Z" and ":" not in zone:
            zone = f"{zone[:3]}:{zone[3:]}"
        candidate = f"{date}T{time_part}{zone}"
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            continue
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return None


class SatnogsNetworkConnector:
    def __init__(self, config: SatnogsConfig, *, norad_id: int, client: httpx.Client | None = None) -> None:
        self.config = config
        self.norad_id = norad_id
        self.client = client or httpx.Client(timeout=30.0)
        self._rate_limited_until_monotonic = 0.0

    def _headers(self) -> dict[str, str]:
        if not self.config.api_token:
            return {}
        return {"Authorization": f"Token {self.config.api_token}"}

    def _build_url(self, path: str) -> str:
        return path if path.startswith("http://") or path.startswith("https://") else urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))

    def _get_response(self, path: str, *, params: dict[str, Any] | None = None) -> httpx.Response:
        now = time.monotonic()
        if now < self._rate_limited_until_monotonic:
            raise SatnogsRateLimitError(ceil(self._rate_limited_until_monotonic - now))

        try:
            response = self.client.get(
                self._build_url(path),
                params=params,
                headers=self._headers(),
            )
        except CoordinatedRateLimitError as exc:
            raise SatnogsRateLimitError(exc.retry_after_seconds) from exc
        if response.status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            self._rate_limited_until_monotonic = time.monotonic() + retry_after
            raise SatnogsRateLimitError(retry_after)
        response.raise_for_status()
        return response

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        response = self._get_response(path, params=params)
        return response.json()

    def list_recent_observations(
        self,
        *,
        now: datetime | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        next_url: str | None = None,
        status: str | None = None,
    ) -> ObservationPage:
        if next_url is not None:
            response = self._get_response(next_url)
            payload = response.json()
            if isinstance(payload, list):
                return ObservationPage(
                    results=[item for item in payload if isinstance(item, dict)],
                    next_url=self._extract_next_url(response),
                )
            raise ValueError("SatNOGS observations response must be an array")

        params: dict[str, Any] = {
            "satellite__norad_cat_id": self.norad_id,
            "transmitter_uuid": self.config.transmitter_uuid,
            "status": status or self.config.status,
        }
        if start_time is not None:
            params["start"] = start_time
        if end_time is not None:
            params["end"] = end_time
        response = self._get_response("/api/observations/", params=params)
        payload = response.json()
        if isinstance(payload, list):
            return ObservationPage(
                results=[item for item in payload if isinstance(item, dict)],
                next_url=self._extract_next_url(response),
            )
        raise ValueError("SatNOGS observations response must be an array")

    def list_upcoming_observations(self, *, now: datetime | None = None) -> ObservationPage:
        observed_at = now or datetime.now(timezone.utc)
        end_time = observed_at + timedelta(hours=self.config.upcoming_lookahead_hours)
        return self.list_recent_observations(
            start_time=observed_at.isoformat(),
            end_time=end_time.isoformat(),
            status=self.config.upcoming_status,
        )

    def _extract_next_url(self, response: httpx.Response) -> str | None:
        next_link = response.links.get("next")
        if next_link:
            url = next_link.get("url")
            if isinstance(url, str) and url:
                return url
        return None

    def get_observation_detail(self, observation_id: str) -> dict[str, Any]:
        payload = self._get(f"/api/observations/{observation_id}/")
        if not isinstance(payload, dict):
            raise ValueError("SatNOGS observation detail response must be an object")
        return payload

    def is_eligible_observation(
        self,
        payload: dict[str, Any],
        *,
        status: str | None = None,
        require_status: bool = True,
    ) -> bool:
        try:
            observation = self.normalize_observation(payload)
        except (KeyError, TypeError, ValueError):
            return False
        identity_matches = (
            observation.satellite_norad_cat_id == self.norad_id
            and observation.transmitter_uuid == self.config.transmitter_uuid
        )
        if not identity_matches:
            return False
        if not require_status:
            return True
        return _stringify(observation.status) == (status or self.config.status)

    def normalize_observation(self, payload: dict[str, Any]) -> ObservationRecord:
        return ObservationRecord(
            observation_id=str(payload["id"]),
            satellite_norad_cat_id=int(
                payload.get("satellite__norad_cat_id")
                or payload.get("norad_cat_id")
                or payload.get("satellite", {}).get("norad_cat_id")
            ),
            transmitter_uuid=_stringify(payload.get("transmitter_uuid") or payload.get("transmitter")),
            start_time=_stringify(payload.get("start") or payload.get("start_time")),
            end_time=_stringify(payload.get("end") or payload.get("end_time")),
            ground_station_id=self._extract_ground_station_id(payload),
            observer=_stringify(payload.get("observer")),
            station_callsign=_stringify(payload.get("station_callsign") or payload.get("ground_station_callsign") or payload.get("station_name")),
            station_lat=payload.get("station_lat"),
            station_lng=payload.get("station_lng"),
            station_alt=payload.get("station_alt"),
            status=payload.get("status"),
            demoddata=payload.get("demoddata"),
            artifact_refs=self._extract_artifact_refs(payload),
            raw_json=payload,
        )

    def _extract_ground_station_id(self, payload: dict[str, Any]) -> str | None:
        value = payload.get("ground_station_id")
        if value is None and isinstance(payload.get("ground_station"), dict):
            value = payload["ground_station"].get("id")
        if value is None:
            value = payload.get("ground_station")
        return _stringify(value)

    def _extract_artifact_refs(self, payload: dict[str, Any]) -> list[str]:
        refs: list[str] = []
        for candidate in payload.get("artifact_refs", []) or []:
            if isinstance(candidate, str):
                refs.append(candidate)
        for key in ("demoddata_url", "payload_demod_url"):
            if isinstance(payload.get(key), str):
                refs.append(payload[key])
        return refs

    def _download_artifact_lines(self, url: str) -> list[str | bytes]:
        artifact_url = url if url.startswith("http://") or url.startswith("https://") else urljoin(self.config.base_url.rstrip("/") + "/", url.lstrip("/"))
        response = self.client.get(artifact_url, headers=self._headers())
        response.raise_for_status()
        content = getattr(response, "content", None)
        if content is None:
            return [line for line in response.text.splitlines() if line.strip()]
        return [line for line in content.splitlines() if line.strip()]

    def _decode_frame_line(self, raw_line: str | bytes) -> tuple[bytes | None, str, str | None]:
        if isinstance(raw_line, bytes):
            stripped = raw_line.strip()
            raw_display = stripped.decode("utf-8", errors="replace")
            if not stripped:
                return None, raw_display, None
            return stripped, raw_display, None

        stripped_text = raw_line.strip()
        if not stripped_text:
            return None, stripped_text, None
        compact_hex = "".join(stripped_text.split())
        if compact_hex and len(compact_hex) % 2 == 0 and re.fullmatch(r"[0-9A-Fa-f]+", compact_hex):
            try:
                return binascii.unhexlify(compact_hex), stripped_text, None
            except binascii.Error as exc:
                return None, stripped_text, repr(exc)
        return None, stripped_text, "line is not hex-encoded"

    def extract_frames(
        self,
        observation: ObservationRecord,
        *,
        source: str = "satnogs_network",
    ) -> tuple[list[FrameRecord], list[dict[str, Any]]]:
        lines: list[tuple[str | bytes, str | None]] = []
        demoddata = observation.demoddata

        if isinstance(demoddata, str):
            for raw_line in demoddata.splitlines():
                if raw_line.strip():
                    lines.append((raw_line, None))
        elif isinstance(demoddata, list):
            for item in demoddata:
                if isinstance(item, str):
                    lines.append((item, None))
                    continue
                if isinstance(item, dict):
                    raw_line = item.get("payload") or item.get("frame") or item.get("hex")
                    if isinstance(raw_line, str) and raw_line.strip():
                        lines.append((raw_line, _stringify(item.get("timestamp") or item.get("time"))))
                        continue
                    payload_demod = item.get("payload_demod")
                    if isinstance(payload_demod, str) and payload_demod.strip():
                        item_time = (
                            _stringify(item.get("timestamp") or item.get("time"))
                            or _timestamp_from_artifact_ref(payload_demod)
                        )
                        for downloaded_line in self._download_artifact_lines(payload_demod):
                            lines.append((downloaded_line, item_time))
        elif demoddata is None and observation.artifact_refs:
            for ref in observation.artifact_refs:
                ref_time = _timestamp_from_artifact_ref(ref)
                for raw_line in self._download_artifact_lines(ref):
                    lines.append((raw_line, ref_time))

        frames: list[FrameRecord] = []
        invalid_lines: list[dict[str, Any]] = []
        for index, (raw_line, explicit_time) in enumerate(lines):
            frame_bytes, raw_display, error = self._decode_frame_line(raw_line)
            if error is not None:
                invalid_lines.append({"frame_index": index, "raw_line": raw_display, "error": error})
                continue
            if not frame_bytes:
                continue
            frames.append(
                FrameRecord(
                    frame_bytes=frame_bytes,
                    reception_time=explicit_time or observation.end_time or observation.start_time,
                    observation_id=observation.observation_id,
                    ground_station_id=observation.ground_station_id,
                    source=source,
                    frame_index=index,
                    raw_line=raw_display,
                )
            )
        return frames, invalid_lines
