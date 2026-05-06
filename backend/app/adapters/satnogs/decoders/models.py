"""Decoder configuration and result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, model_validator

DecoderStrategy = Literal["aprs", "kaitai"]
DecodeMode = Literal["aprs", "vehicle"]


class DecoderConfig(BaseModel):
    strategy: DecoderStrategy = "aprs"
    decoder_id: str | None = None

    @model_validator(mode="after")
    def validate_strategy(self) -> "DecoderConfig":
        if self.decoder_id == "":
            self.decoder_id = None
        if self.strategy == "aprs":
            self.decoder_id = None
            return self
        if self.strategy == "kaitai" and not self.decoder_id:
            raise ValueError("vehicle.decoder.decoder_id is required for strategy='kaitai'")
        return self


@dataclass(slots=True)
class PacketMatchResult:
    matched: bool
    reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class DecodedPacketResult:
    decode_mode: DecodeMode
    decoder_strategy: DecoderStrategy
    decoder_name: str
    packet_name: str
    fields: dict[str, float | int]
    raw_payload_hex: str
    metadata: dict[str, object] = field(default_factory=dict)
