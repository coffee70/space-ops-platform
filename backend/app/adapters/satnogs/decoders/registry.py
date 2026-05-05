"""Decoder registry."""

from __future__ import annotations

from app.adapters.satnogs.decoders.aprs import AprsDecoder
from app.adapters.satnogs.decoders.models import DecoderConfig
from app.adapters.satnogs.decoders.service import PayloadDecodeError, PayloadDecoder
from app.adapters.satnogs.decoders.vehicles.lasarsat_decoder import LasarsatDecoder


class DecoderRegistry:
    def __init__(self) -> None:
        self._decoders: dict[tuple[str, str | None], PayloadDecoder] = {
            ("aprs", None): AprsDecoder(),
            ("kaitai", "lasarsat"): LasarsatDecoder(),
        }

    def resolve(self, config: DecoderConfig) -> PayloadDecoder:
        key = (config.strategy, config.decoder_id)
        decoder = self._decoders.get(key)
        if decoder is None:
            raise PayloadDecodeError(
                reason="decoder_strategy_not_supported",
                decoder_strategy=config.strategy,
                decoder_id=config.decoder_id,
                decoder_name=config.decoder_id or config.strategy,
                error_message=f"Unsupported payload decoder: strategy={config.strategy!r} decoder_id={config.decoder_id!r}",
            )
        return decoder
