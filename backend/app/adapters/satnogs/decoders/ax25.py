"""AX.25 helpers."""

from __future__ import annotations

from typing import Iterable

from app.adapters.satnogs.models import AX25Frame

AX25_ADDRESS_SIZE = 7


def _decode_ax25_callsign(chunk: bytes) -> str:
    raw = "".join(chr((byte >> 1) & 0x7F) for byte in chunk[:6]).strip()
    ssid = (chunk[6] >> 1) & 0x0F
    return f"{raw}-{ssid}" if ssid else raw


def parse_ax25_frame(frame_bytes: bytes) -> AX25Frame:
    if len(frame_bytes) < AX25_ADDRESS_SIZE * 2 + 2:
        raise ValueError("AX.25 frame too short")

    addresses: list[bytes] = []
    cursor = 0
    while cursor + AX25_ADDRESS_SIZE <= len(frame_bytes):
        chunk = frame_bytes[cursor : cursor + AX25_ADDRESS_SIZE]
        addresses.append(chunk)
        cursor += AX25_ADDRESS_SIZE
        if chunk[6] & 0x01:
            break

    if len(addresses) < 2:
        raise ValueError("AX.25 frame missing destination/source")
    if cursor + 2 > len(frame_bytes):
        raise ValueError("AX.25 frame missing control/pid")

    dest_callsign = _decode_ax25_callsign(addresses[0])
    src_callsign = _decode_ax25_callsign(addresses[1])
    digipeater_path = [_decode_ax25_callsign(chunk) for chunk in addresses[2:]]
    control = frame_bytes[cursor]
    pid = frame_bytes[cursor + 1]
    info_bytes = frame_bytes[cursor + 2 :]
    return AX25Frame(
        dest_callsign=dest_callsign,
        src_callsign=src_callsign,
        digipeater_path=digipeater_path,
        control=control,
        pid=pid,
        info_bytes=info_bytes,
    )


def normalize_callsign(callsign: str) -> str:
    return callsign.strip().upper()


def is_originated_packet(frame: AX25Frame, allowed_source_callsigns: Iterable[str]) -> bool:
    allowed = {normalize_callsign(item) for item in allowed_source_callsigns}
    return normalize_callsign(frame.src_callsign).split("-", 1)[0] in {
        item.split("-", 1)[0] for item in allowed
    }
