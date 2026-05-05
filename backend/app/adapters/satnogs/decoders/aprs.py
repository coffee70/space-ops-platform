"""APRS decoder strategy and parser."""

from __future__ import annotations

import re

from app.adapters.satnogs.decoders.models import DecodedPacketResult, PacketMatchResult
from app.adapters.satnogs.decoders.service import PayloadDecodeError, PayloadDecoder
from app.adapters.satnogs.models import APRSPacket, AX25Frame, FrameRecord, ObservationRecord

POSITION_RE = re.compile(
    r"^(?:[!=/]|[@/]\d{6}[hz/])"
    r"(?P<lat>\d{4}\.\d{2}[NS])(?P<symtbl>.)"
    r"(?P<lon>\d{5}\.\d{2}[EW])(?P<symcode>.)(?P<rest>.*)$"
)
COURSE_SPEED_RE = re.compile(r"^(?P<course>\d{3})/(?P<speed>\d{3})")
ALTITUDE_RE = re.compile(r"/A=(?P<altitude>\d{6})")
KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z][A-Za-z0-9_ -]{1,32})\s*[:=]\s*(?P<value>-?\d+(?:\.\d+)?)")
CSV_PACKET_RE = re.compile(r"(?P<family>[A-Za-z][A-Za-z0-9_]{1,16}),(?P<values>.*)")


def _ddmm_to_decimal(raw: str, *, is_lat: bool) -> float:
    head = 2 if is_lat else 3
    degrees = float(raw[:head])
    minutes = float(raw[head:-1])
    value = degrees + minutes / 60.0
    if raw.endswith(("S", "W")):
        value *= -1
    return value


def parse_aprs_payload(info_bytes: bytes) -> APRSPacket:
    payload = info_bytes.decode("ascii", errors="ignore").strip()
    if not payload:
        raise ValueError("empty APRS payload")

    fields: dict[str, float] = {}
    kv_fields: dict[str, float] = {}
    packet_type = "unknown"

    match = POSITION_RE.match(payload)
    if match:
        packet_type = "position"
        fields["latitude"] = _ddmm_to_decimal(match.group("lat"), is_lat=True)
        fields["longitude"] = _ddmm_to_decimal(match.group("lon"), is_lat=False)
        rest = match.group("rest")
        course_match = COURSE_SPEED_RE.match(rest)
        if course_match:
            fields["course_deg"] = float(course_match.group("course"))
            fields["speed_kmh"] = round(float(course_match.group("speed")) * 1.852, 3)
        altitude_match = ALTITUDE_RE.search(rest)
        if altitude_match:
            fields["altitude_m"] = round(float(altitude_match.group("altitude")) * 0.3048, 3)

    for kv_match in KEY_VALUE_RE.finditer(payload):
        key = kv_match.group("key").strip().lower().replace(" ", "_")
        value = float(kv_match.group("value"))
        kv_fields[key] = value
        fields.setdefault(key, value)

    if not fields:
        csv_match = CSV_PACKET_RE.search(payload)
        if csv_match:
            packet_type = f"csv:{csv_match.group('family').lower()}"
            family = csv_match.group("family").strip().lower()
            for index, raw_value in enumerate(csv_match.group("values").split(","), start=1):
                value_text = raw_value.strip()
                if not value_text:
                    continue
                try:
                    fields[f"{family}_{index:02d}"] = float(value_text)
                except ValueError:
                    continue

    if not fields:
        raise ValueError("APRS payload did not contain numeric telemetry")
    return APRSPacket(packet_type=packet_type, fields=fields, kv_fields=kv_fields, raw_payload=payload)


class AprsDecoder(PayloadDecoder):
    strategy = "aprs"
    name = "aprs"
    decoder_id = None

    def matches(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> PacketMatchResult:
        del observation
        if ax25_packet.control != 0x03:
            return PacketMatchResult(matched=False, reason="unexpected_ax25_control", metadata={"control": ax25_packet.control})
        if ax25_packet.pid != 0xF0:
            return PacketMatchResult(matched=False, reason="unexpected_ax25_pid", metadata={"pid": ax25_packet.pid})
        if not ax25_packet.info_bytes:
            return PacketMatchResult(matched=False, reason="empty_payload", metadata={})
        return PacketMatchResult(matched=True, reason=None, metadata={})

    def decode(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> DecodedPacketResult:
        del observation, frame
        try:
            packet = parse_aprs_payload(ax25_packet.info_bytes)
        except ValueError as exc:
            raise PayloadDecodeError(
                reason="normalization_failed",
                decoder_strategy=self.strategy,
                decoder_id=self.decoder_id,
                decoder_name=self.name,
                error_message=str(exc),
            ) from exc
        return DecodedPacketResult(
            decode_mode="aprs",
            decoder_strategy=self.strategy,
            decoder_name=self.name,
            packet_name=packet.packet_type,
            fields=dict(packet.fields),
            raw_payload_hex=ax25_packet.info_bytes.hex(),
            metadata={
                "raw_payload": packet.raw_payload,
                "kv_fields": dict(packet.kv_fields),
            },
        )
