"""Packet classification and event mapping."""

from __future__ import annotations

from typing import Iterable

from app.adapters.satnogs.decoders import is_originated_packet
from app.adapters.satnogs.decoders.models import DecodedPacketResult
from app.adapters.satnogs.models import APRSPacket, AX25Frame, ObservationRecord, TelemetryEvent


class TelemetryMapper:
    def __init__(
        self,
        *,
        source_id: str,
        stable_field_mappings: dict[str, str],
        allowed_source_callsigns: Iterable[str],
        vehicle_norad_cat_id: int,
    ) -> None:
        self.source_id = source_id
        self.stable_field_mappings = stable_field_mappings
        self.allowed_source_callsigns = list(allowed_source_callsigns)
        self.vehicle_norad_cat_id = vehicle_norad_cat_id

    def is_originated_packet(self, frame: AX25Frame) -> bool:
        return is_originated_packet(frame, self.allowed_source_callsigns)

    def build_receiver_id(self, observation: ObservationRecord) -> str | None:
        if observation.ground_station_id is None:
            return None
        return f"satnogs-station-{observation.ground_station_id}"

    def stream_id_for_observation(self, observation: ObservationRecord) -> str:
        return f"satnogs-obs-{observation.observation_id}"

    def map_packet(
        self,
        *,
        observation: ObservationRecord,
        frame: AX25Frame,
        aprs_packet: APRSPacket,
        reception_time: str | None,
        sequence_seed: int,
    ) -> list[TelemetryEvent]:
        decoded_packet = DecodedPacketResult(
            decode_mode="aprs",
            decoder_strategy="aprs",
            decoder_name="aprs",
            packet_name=aprs_packet.packet_type,
            fields=dict(aprs_packet.fields),
            raw_payload_hex=frame.info_bytes.hex(),
            metadata={
                "raw_payload": aprs_packet.raw_payload,
                "kv_fields": dict(aprs_packet.kv_fields),
            },
        )
        return self.map_decoded_packet(
            observation=observation,
            frame=frame,
            decoded_packet=decoded_packet,
            reception_time=reception_time,
            sequence_seed=sequence_seed,
        )

    def map_decoded_packet(
        self,
        *,
        observation: ObservationRecord,
        frame: AX25Frame,
        decoded_packet: DecodedPacketResult,
        reception_time: str | None,
        sequence_seed: int,
    ) -> list[TelemetryEvent]:
        tags = {
            "satnogs.observation_id": observation.observation_id,
            "satnogs.ground_station_id": observation.ground_station_id or "",
            "satnogs.satellite_norad_cat_id": str(self.vehicle_norad_cat_id),
        }
        if observation.start_time:
            tags["satnogs.observation_start"] = observation.start_time
        if observation.end_time:
            tags["satnogs.observation_end"] = observation.end_time

        receiver_id = self.build_receiver_id(observation)
        stream_id = self.stream_id_for_observation(observation)
        packet_source = frame.src_callsign
        events: list[TelemetryEvent] = []
        sequence = sequence_seed

        for field_name, value in decoded_packet.fields.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            sequence += 1
            stable_channel_name = self.stable_field_mappings.get(field_name)
            if stable_channel_name:
                event_tags = dict(tags)
                events.append(
                    TelemetryEvent(
                        source_id=self.source_id,
                        stream_id=stream_id,
                        value=float(value),
                        reception_time=reception_time,
                        generation_time=None,
                        channel_name=stable_channel_name,
                        packet_source=packet_source,
                        receiver_id=receiver_id,
                        sequence=sequence,
                        tags=event_tags,
                    )
                )
                continue

            event_tags = dict(tags)
            event_tags.update(
                {
                    "decoder": decoded_packet.decoder_name,
                    "decoder_strategy": decoded_packet.decoder_strategy,
                    "field_name": field_name,
                    "packet_name": decoded_packet.packet_name,
                    "ax25.src": frame.src_callsign,
                    "ax25.dst": frame.dest_callsign,
                }
            )
            if frame.digipeater_path:
                event_tags["ax25.path"] = ",".join(frame.digipeater_path)
            events.append(
                TelemetryEvent(
                    source_id=self.source_id,
                    stream_id=stream_id,
                    value=float(value),
                    reception_time=reception_time,
                    generation_time=None,
                    packet_source=packet_source,
                    receiver_id=receiver_id,
                    sequence=sequence,
                    tags=event_tags,
                    )
            )
        return events
