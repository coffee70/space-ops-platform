"""Payload decode service contracts and orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from app.adapters.satnogs.decoders.models import DecodedPacketResult, DecoderConfig, PacketMatchResult
from app.adapters.satnogs.models import AX25Frame, FrameRecord, ObservationRecord


@dataclass(slots=True)
class PayloadDecodeError(Exception):
    reason: str
    decoder_strategy: str
    decoder_id: str | None
    decoder_name: str
    error_message: str
    packet_name: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.error_message


class PayloadDecoder(Protocol):
    strategy: str
    name: str
    decoder_id: str | None

    def matches(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> PacketMatchResult: ...

    def decode(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> DecodedPacketResult: ...


class PayloadDecodeService:
    def __init__(self, *, decoder_config: DecoderConfig, registry) -> None:
        self.decoder_config = decoder_config
        self.registry = registry

    def validate_configuration(self) -> None:
        self.registry.resolve(self.decoder_config)

    def decode(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25_packet: AX25Frame,
    ) -> DecodedPacketResult | None:
        decoder = self.registry.resolve(self.decoder_config)
        match_result = decoder.matches(observation=observation, frame=frame, ax25_packet=ax25_packet)
        if not match_result.matched:
            return None
        try:
            decoded = decoder.decode(observation=observation, frame=frame, ax25_packet=ax25_packet)
        except PayloadDecodeError:
            raise
        except Exception as exc:
            raise PayloadDecodeError(
                reason="normalization_failed",
                decoder_strategy=decoder.strategy,
                decoder_id=decoder.decoder_id,
                decoder_name=decoder.name,
                error_message=str(exc),
                metadata={"exception_type": type(exc).__name__},
            ) from exc
        if match_result.metadata:
            merged_metadata = dict(decoded.metadata)
            for key, value in match_result.metadata.items():
                merged_metadata.setdefault(key, value)
            decoded.metadata = merged_metadata
        return decoded
