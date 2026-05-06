"""Internal records for the SatNOGS adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ObservationRecord:
    observation_id: str
    satellite_norad_cat_id: int
    start_time: str | None
    end_time: str | None
    ground_station_id: str | None
    transmitter_uuid: str | None = None
    observer: str | None = None
    station_callsign: str | None = None
    station_lat: float | None = None
    station_lng: float | None = None
    station_alt: float | None = None
    status: str | int | None = None
    demoddata: Any = None
    artifact_refs: list[str] = field(default_factory=list)
    raw_json: dict[str, Any] | None = None


@dataclass(slots=True)
class FrameRecord:
    frame_bytes: bytes
    reception_time: str | None
    observation_id: str
    ground_station_id: str | None
    source: str
    frame_index: int
    raw_line: str | None = None


@dataclass(slots=True)
class AX25Frame:
    dest_callsign: str
    src_callsign: str
    digipeater_path: list[str]
    control: int
    pid: int
    info_bytes: bytes
    parse_notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class APRSPacket:
    packet_type: str
    fields: dict[str, float]
    kv_fields: dict[str, float]
    raw_payload: str


@dataclass(slots=True)
class TelemetryEvent:
    source_id: str
    stream_id: str
    value: float
    reception_time: str | None
    generation_time: str | None
    channel_name: str | None = None
    quality: str = "valid"
    sequence: int | None = None
    packet_source: str | None = None
    receiver_id: str | None = None
    tags: dict[str, str] | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source_id": self.source_id,
            "stream_id": self.stream_id,
            "value": self.value,
        }
        if self.reception_time is not None:
            payload["reception_time"] = self.reception_time
        if self.generation_time is not None:
            payload["generation_time"] = self.generation_time
        if self.channel_name is not None:
            payload["channel_name"] = self.channel_name
        if self.quality:
            payload["quality"] = self.quality
        if self.sequence is not None:
            payload["sequence"] = self.sequence
        if self.packet_source is not None:
            payload["packet_source"] = self.packet_source
        if self.receiver_id is not None:
            payload["receiver_id"] = self.receiver_id
        if self.tags:
            payload["tags"] = self.tags
        return payload

