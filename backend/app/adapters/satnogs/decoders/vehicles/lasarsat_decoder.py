"""LASARSAT Kaitai-backed decoder wrapper."""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from typing import Any

from app.adapters.satnogs.decoders.models import DecodedPacketResult, PacketMatchResult
from app.adapters.satnogs.decoders.service import PayloadDecodeError, PayloadDecoder
from app.adapters.satnogs.models import AX25Frame, FrameRecord, ObservationRecord

from app.adapters.satnogs.decoders.generated.lasarsat import Lasarsat

_TYPE_NAME_RE = re.compile(r"(?<!^)(?=[A-Z])")
_SKIP_ATTR_NAMES = {
    "ar",
    "ctl",
    "de_ok0lsr",
    "hbit",
    "pid",
    "ssid",
}
_SKIP_ATTR_PREFIXES = ("message_type",)
_SKIP_ATTR_SUFFIXES = ("_raw", "_str", "_mask")


@dataclass(slots=True)
class _LeafValue:
    path: tuple[str, ...]
    value: object

    @property
    def base_name(self) -> str:
        return self.path[-1]


class LasarsatDecoder(PayloadDecoder):
    strategy = "kaitai"
    name = "lasarsat"
    decoder_id = "lasarsat"

    def matches(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> PacketMatchResult:
        del observation
        notes: list[str] = []
        if ax25_packet.control != 0x03:
            return PacketMatchResult(matched=False, reason="unexpected_ax25_control", metadata={"control": ax25_packet.control})
        if ax25_packet.pid != 0xF0:
            return PacketMatchResult(matched=False, reason="unexpected_ax25_pid", metadata={"pid": ax25_packet.pid})
        if not ax25_packet.info_bytes:
            return PacketMatchResult(matched=False, reason="empty_payload", metadata={})
        notes.append("control_pid_and_payload_length_matched")
        return PacketMatchResult(matched=True, reason=None, metadata={"match_notes": notes})

    def decode(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> DecodedPacketResult:
        del observation
        payload_candidates: list[tuple[str, bytes]] = [("full_frame", frame.frame_bytes)]
        if ax25_packet.info_bytes.lower().startswith(b"de "):
            payload_candidates.append(("info_payload", ax25_packet.info_bytes))

        parse_error: Exception | None = None
        for payload_source, payload in payload_candidates:
            try:
                parsed = Lasarsat.from_bytes(payload)
            except Exception as exc:
                parse_error = exc
                continue

            try:
                leaves = list(self._collect_leaf_values(parsed))
                assignments = self._assign_field_names(leaves)
            except Exception as exc:
                raise PayloadDecodeError(
                    reason="normalization_failed",
                    decoder_strategy=self.strategy,
                    decoder_id=self.decoder_id,
                    decoder_name=self.name,
                    error_message=str(exc),
                ) from exc

            fields: dict[str, int | float] = {}
            metadata_fields: dict[str, object] = {}
            for leaf in leaves:
                key = assignments[leaf.path]
                value = leaf.value
                if value is None:
                    continue
                if isinstance(value, bool):
                    metadata_fields[key] = value
                    continue
                if isinstance(value, enum.Enum):
                    metadata_fields[key] = value.name
                    continue
                if isinstance(value, int):
                    fields[key] = value
                    continue
                if isinstance(value, float):
                    fields[key] = value
                    continue
                if isinstance(value, bytes):
                    metadata_fields[key] = value.hex()
                    continue
                metadata_fields[key] = value

            if not fields and payload_source != payload_candidates[-1][0]:
                continue

            packet_name = self._packet_name(parsed)
            return DecodedPacketResult(
                decode_mode="vehicle",
                decoder_strategy=self.strategy,
                decoder_name=self.name,
                packet_name=packet_name,
                fields=fields,
                raw_payload_hex=ax25_packet.info_bytes.hex(),
                metadata={
                    "variant": packet_name,
                    "match_notes": ["control_pid_and_payload_length_matched"],
                    "full_frame_hex": frame.frame_bytes.hex(),
                    "payload_source": payload_source,
                    "non_numeric_fields": metadata_fields,
                },
            )

        error_message = str(parse_error) if parse_error is not None else "No LASARSAT payload bytes available"
        raise PayloadDecodeError(
            reason="vehicle_decoder_parse_failed",
            decoder_strategy=self.strategy,
            decoder_id=self.decoder_id,
            decoder_name=self.name,
            error_message=error_message,
        )

    def _collect_leaf_values(self, value: object, path: tuple[str, ...] = (), seen: set[int] | None = None) -> list[_LeafValue]:
        if seen is None:
            seen = set()
        if value is None:
            return []
        if isinstance(value, (str, bytes, int, float, bool, enum.Enum)):
            if not path:
                return []
            return [_LeafValue(path=path, value=value)]
        if isinstance(value, (list, tuple, set, dict)):
            return []
        identity = id(value)
        if identity in seen:
            return []
        seen.add(identity)
        leaves: list[_LeafValue] = []
        for attr_name in dir(value):
            if attr_name.startswith("_"):
                continue
            if attr_name in _SKIP_ATTR_NAMES:
                continue
            if any(attr_name.startswith(prefix) for prefix in _SKIP_ATTR_PREFIXES):
                continue
            if any(attr_name.endswith(suffix) for suffix in _SKIP_ATTR_SUFFIXES):
                continue
            try:
                child = getattr(value, attr_name)
            except Exception:
                continue
            if callable(child):
                continue
            child_path = path + (attr_name,)
            if child is None:
                continue
            if isinstance(child, (str, bytes, int, float, bool, enum.Enum)):
                leaves.append(_LeafValue(path=child_path, value=child))
                continue
            if isinstance(child, (list, tuple, set, dict)):
                continue
            leaves.extend(self._collect_leaf_values(child, child_path, seen))
        return leaves

    def _assign_field_names(self, leaves: list[_LeafValue]) -> dict[tuple[str, ...], str]:
        collisions: dict[str, list[_LeafValue]] = {}
        for leaf in leaves:
            collisions.setdefault(leaf.base_name, []).append(leaf)

        assigned: dict[tuple[str, ...], str] = {}
        used: set[str] = set()
        for base_name in sorted(collisions):
            group = sorted(collisions[base_name], key=lambda item: item.path)
            if len(group) == 1 and base_name not in used:
                assigned[group[0].path] = base_name
                used.add(base_name)
                continue
            for leaf in group:
                candidate = self._disambiguated_name(leaf, used)
                assigned[leaf.path] = candidate
                used.add(candidate)
        return assigned

    def _disambiguated_name(self, leaf: _LeafValue, used: set[str]) -> str:
        parts = list(leaf.path)
        for count in range(2, len(parts) + 1):
            candidate = "__".join(parts[-count:])
            if candidate not in used:
                return candidate
        return "__".join(parts)

    def _packet_name(self, parsed: Lasarsat) -> str:
        candidate = parsed
        names: list[str] = []
        for attr_name in ("id1", "id2", "id3", "id4", "id5", "id6"):
            if not hasattr(candidate, attr_name):
                break
            child = getattr(candidate, attr_name, None)
            if child is None:
                break
            class_name = child.__class__.__name__
            if not class_name.startswith("Type") and not class_name.startswith("Not"):
                names.append(self._to_snake_case(class_name))
            candidate = child
        return names[-1] if names else self._to_snake_case(parsed.__class__.__name__)

    def _to_snake_case(self, value: str) -> str:
        return _TYPE_NAME_RE.sub("_", value).lower()
