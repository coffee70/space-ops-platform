"""Runtime orchestration for the SatNOGS adapter."""

from __future__ import annotations

import binascii
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from app.adapters.satnogs.config import AdapterConfig
from app.adapters.satnogs.connectors import SatnogsNetworkConnector, SatnogsRateLimitError
from app.adapters.satnogs.decoders import PayloadDecodeError, PayloadDecodeService, parse_ax25_frame
from app.adapters.satnogs.dlq import FilesystemDlq
from app.adapters.satnogs.mapper import TelemetryMapper
from app.adapters.satnogs.models import FrameRecord, ObservationRecord, TelemetryEvent
from app.adapters.satnogs.publisher import IngestPublisher, ObservationsPublisher, SourceStatePublisher
from app.adapters.satnogs.source_resolver import ResolvedSource

logger = logging.getLogger(__name__)


class _NoopSourceStatePublisher:
    def publish_live_state(self, _state: str, *, error: str | None = None):
        return type("Result", (), {"success": True, "status_code": 200, "response_body": ""})()

    def publish_backfill_progress(self, _payload: dict[str, Any]):
        return type("Result", (), {"success": True, "status_code": 200, "response_body": ""})()


def _compat_source_contract(source_id: str) -> ResolvedSource:
    now = datetime.now(timezone.utc)
    return ResolvedSource(
        id=source_id,
        name=source_id,
        source_type="vehicle",
        vehicle_config_path="vehicles/iss.yaml",
        created=False,
        monitoring_start_time=now,
        last_reconciled_at=now,
        history_mode="live_only",
        live_state="idle",
        backfill_state="complete",
        chunk_size_hours=6,
    )


def _parse_observation_datetime(value: str | None) -> datetime:
    if not value:
        raise ValueError("observation timestamp is missing")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class AdapterRunner:
    def __init__(
        self,
        config: AdapterConfig,
        *,
        network_connector: SatnogsNetworkConnector,
        backfill_network_connector: SatnogsNetworkConnector | None = None,
        publisher: IngestPublisher,
        observations_publisher: ObservationsPublisher,
        state_publisher: SourceStatePublisher | None = None,
        dlq: FilesystemDlq | None = None,
        payload_decode_service: PayloadDecodeService | None = None,
        source_contract: ResolvedSource | None = None,
        source_id: str | None = None,
        checkpoint_store: object | None = None,
        startup_cutoff_time: datetime | None = None,
    ) -> None:
        self.config = config
        self.network_connector = network_connector
        self.backfill_network_connector = backfill_network_connector or network_connector
        self.publisher = publisher
        self.observations_publisher = observations_publisher
        self.state_publisher = state_publisher or _NoopSourceStatePublisher()
        if dlq is None or payload_decode_service is None:
            raise ValueError("AdapterRunner requires dlq and payload_decode_service")
        self.dlq = dlq
        self.payload_decode_service = payload_decode_service
        self._last_observation_sync_monotonic: float | None = None
        if source_contract is None:
            if not source_id:
                raise ValueError("AdapterRunner requires a resolved source contract")
            source_contract = _compat_source_contract(source_id)
        self.source_contract = source_contract
        self.startup_cutoff_time = startup_cutoff_time or datetime.now(timezone.utc)
        resolved_source_id = source_contract.id
        self.mapper = TelemetryMapper(
            source_id=resolved_source_id,
            stable_field_mappings=config.resolve_stable_field_mappings(),
            allowed_source_callsigns=config.vehicle.allowed_source_callsigns,
            vehicle_norad_cat_id=config.vehicle.norad_id,
        )

    def run_forever(self) -> None:
        self.start_background_backfill()
        self.state_publisher.publish_live_state("active")
        while True:
            try:
                self.run_live_once()
            except Exception:
                logger.exception("SatNOGS live poll failed")
            time.sleep(self.config.satnogs.poll_interval_seconds)

    def run_live_once(self) -> None:
        self._sync_upcoming_observations_if_due()
        self._run_observation_pages()

    def start_background_backfill(self) -> threading.Thread | None:
        if self.source_contract.history_mode == "live_only":
            return None
        thread = threading.Thread(target=self.run_backfill_snapshot, name="satnogs-backfill", daemon=True)
        thread.start()
        return thread

    def _sync_upcoming_observations_if_due(self) -> None:
        now_monotonic = time.monotonic()
        if (
            self._last_observation_sync_monotonic is not None
            and now_monotonic - self._last_observation_sync_monotonic
            < self.config.satnogs.observation_sync_interval_seconds
        ):
            return
        self._last_observation_sync_monotonic = now_monotonic
        try:
            observation_page = self.network_connector.list_upcoming_observations()
            observations = []
            for raw_observation in observation_page.results:
                if not self.network_connector.is_eligible_observation(
                    raw_observation,
                    status=self.config.satnogs.upcoming_status,
                    require_status=False,
                ):
                    continue
                observation = self.network_connector.normalize_observation(raw_observation)
                payload = self._observation_window_payload(observation)
                if payload is not None:
                    observations.append(payload)
            result = self.observations_publisher.publish(
                observations,
                provider="satnogs",
                replace_future_scheduled=True,
                context={"source_id": self.mapper.source_id, "count": len(observations)},
            )
            if not result.success:
                logger.warning("SatNOGS observation sync failed: status=%s body=%s", result.status_code, result.response_body)
            else:
                logger.info(
                    "Synced SatNOGS upcoming observations: source_id=%s count=%s status=%s",
                    self.mapper.source_id,
                    len(observations),
                    result.status_code,
                )
        except SatnogsRateLimitError as exc:
            logger.warning("SatNOGS observation sync throttled; retry after %ss", exc.retry_after_seconds)
        except Exception:
            logger.exception("SatNOGS observation sync failed")

    def _observation_window_payload(self, observation: ObservationRecord) -> dict[str, object] | None:
        if not observation.start_time or not observation.end_time:
            return None
        station_name = observation.station_callsign or observation.observer
        raw_json = observation.raw_json or {}
        max_elevation = (
            raw_json.get("max_elevation")
            or raw_json.get("max_elevation_deg")
            or raw_json.get("max_altitude")
        )
        details = {
            "satnogs_status": observation.status,
            "satellite_norad_cat_id": observation.satellite_norad_cat_id,
        }
        if observation.transmitter_uuid:
            details["transmitter_uuid"] = observation.transmitter_uuid
        payload: dict[str, object] = {
            "external_id": observation.observation_id,
            "status": "scheduled",
            "start_time": observation.start_time,
            "end_time": observation.end_time,
            "station_name": station_name,
            "station_id": observation.ground_station_id,
            "receiver_id": self.mapper.build_receiver_id(observation),
            "details": details,
        }
        if max_elevation is not None:
            try:
                payload["max_elevation_deg"] = float(max_elevation)
            except (TypeError, ValueError):
                pass
        return payload

    def run_backfill_snapshot(self) -> None:
        if self.source_contract.history_mode == "live_only":
            return
        start = max(
            self.source_contract.last_reconciled_at or self.source_contract.monitoring_start_time,
            self.source_contract.monitoring_start_time,
        )
        target_time = self.startup_cutoff_time
        if start >= target_time:
            self.state_publisher.publish_backfill_progress(
                {
                    "status": "started",
                    "target_time": target_time.isoformat(),
                }
            )
            self.state_publisher.publish_backfill_progress(
                {
                    "status": "completed",
                    "target_time": target_time.isoformat(),
                    "chunk_start": start.isoformat(),
                    "chunk_end": start.isoformat(),
                    "backlog_drained": True,
                }
            )
            return

        started = self.state_publisher.publish_backfill_progress(
            {"status": "started", "target_time": target_time.isoformat()}
        )
        if not started.success:
            logger.warning("Failed to start SatNOGS backfill: status=%s body=%s", started.status_code, started.response_body)
            return

        cursor = start
        chunk_size = timedelta(hours=self.source_contract.chunk_size_hours)
        while cursor < target_time:
            chunk_end = min(target_time, cursor + chunk_size)
            try:
                self._run_observation_pages(
                    start_time=cursor.isoformat(),
                    end_time=chunk_end.isoformat(),
                    mode="backfill",
                    chunk_start=cursor,
                    chunk_end=chunk_end,
                    connector=self.backfill_network_connector,
                    suppress_rate_limit=False,
                )
            except SatnogsRateLimitError as exc:
                logger.warning(
                    "SatNOGS backfill chunk throttled; retrying same chunk after %ss: chunk_start=%s chunk_end=%s",
                    exc.retry_after_seconds,
                    cursor.isoformat(),
                    chunk_end.isoformat(),
                )
                time.sleep(exc.retry_after_seconds)
                continue
            except Exception as exc:
                self.state_publisher.publish_backfill_progress(
                    {
                        "status": "failed",
                        "target_time": target_time.isoformat(),
                        "chunk_start": cursor.isoformat(),
                        "chunk_end": chunk_end.isoformat(),
                        "error": repr(exc),
                    }
                )
                raise
            drained = chunk_end >= target_time
            completed = self.state_publisher.publish_backfill_progress(
                {
                    "status": "completed",
                    "target_time": target_time.isoformat(),
                    "chunk_start": cursor.isoformat(),
                    "chunk_end": chunk_end.isoformat(),
                    "backlog_drained": drained,
                }
            )
            if not completed.success:
                logger.warning(
                    "Failed to report SatNOGS backfill progress: status=%s body=%s",
                    completed.status_code,
                    completed.response_body,
                )
                return
            cursor = chunk_end

    def _run_observation_pages(
        self,
        *,
        start_time: str | None = None,
        end_time: str | None = None,
        mode: str = "live",
        chunk_start: datetime | None = None,
        chunk_end: datetime | None = None,
        max_observations: int | None = None,
        connector: SatnogsNetworkConnector | None = None,
        suppress_rate_limit: bool = True,
    ) -> None:
        active_connector = connector or self.network_connector
        next_url: str | None = None
        observations_seen = 0
        while True:
            page_type = "next" if next_url else "first"
            try:
                observation_page = active_connector.list_recent_observations(
                    next_url=next_url,
                    start_time=None if next_url else start_time,
                    end_time=None if next_url else end_time,
                )
            except SatnogsRateLimitError as exc:
                logger.warning("SatNOGS observation poll throttled; retry after %ss", exc.retry_after_seconds)
                if not suppress_rate_limit:
                    raise
                return
            results = observation_page.results
            if not results:
                logger.info("SatNOGS observation poll returned no results: mode=%s page_type=%s", mode, page_type)
                return
            logger.info(
                "SatNOGS observation poll returned page: mode=%s page_type=%s count=%s has_next=%s chunk_start=%s chunk_end=%s startup_cutoff_time=%s",
                mode,
                page_type,
                len(results),
                bool(observation_page.next_url),
                chunk_start.isoformat() if chunk_start else None,
                chunk_end.isoformat() if chunk_end else None,
                self.startup_cutoff_time.isoformat(),
            )
            stop_after_page = False
            for raw_observation in results:
                if max_observations is not None and observations_seen >= max_observations:
                    return
                observations_seen += 1
                eligibility = self._observation_time_eligibility(
                    raw_observation,
                    mode=mode,
                    connector=active_connector,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                )
                if not eligibility["eligible"]:
                    logger.info(
                        "Skipping SatNOGS observation outside temporal responsibility: mode=%s observation_id=%s reason=%s",
                        mode,
                        raw_observation.get("id"),
                        eligibility["reason"],
                    )
                    if eligibility["stop_pagination"]:
                        stop_after_page = True
                        break
                    continue
                self._process_observation_payload(raw_observation, connector=active_connector)
                if eligibility["stop_pagination"]:
                    stop_after_page = True
                    break
            if stop_after_page:
                logger.info("Stopping SatNOGS pagination at startup cutoff: mode=%s", mode)
                return
            if not observation_page.next_url:
                return
            next_url = observation_page.next_url

    def _observation_time_eligibility(
        self,
        raw_observation: dict[str, object],
        *,
        mode: str,
        connector: SatnogsNetworkConnector,
        chunk_start: datetime | None,
        chunk_end: datetime | None,
    ) -> dict[str, object]:
        observation_id = raw_observation.get("id")
        try:
            observation = connector.normalize_observation(raw_observation)
            start = _parse_observation_datetime(observation.start_time)
            end = _parse_observation_datetime(observation.end_time)
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "Skipping SatNOGS observation with invalid timestamps: mode=%s observation_id=%s error=%s",
                mode,
                observation_id,
                exc,
            )
            return {"eligible": False, "stop_pagination": False, "reason": "invalid_timestamps"}

        if mode == "backfill":
            if chunk_start is None or chunk_end is None:
                raise ValueError("backfill observation filtering requires chunk_start and chunk_end")
            if start < chunk_start or end > chunk_end:
                return {"eligible": False, "stop_pagination": False, "reason": "outside_backfill_chunk"}
            return {"eligible": True, "stop_pagination": False, "reason": None}

        stop_pagination = start < self.startup_cutoff_time
        if end <= self.startup_cutoff_time:
            return {"eligible": False, "stop_pagination": stop_pagination, "reason": "before_live_cutoff"}
        return {"eligible": True, "stop_pagination": stop_pagination, "reason": None}

    def replay_batch_dlq(self, *, max_age_seconds: int | None = None) -> int:
        replayed = 0
        now = datetime.now(timezone.utc).timestamp()
        for path in self.dlq.iter_kind("batch"):
            if max_age_seconds is not None and now - path.stat().st_mtime > max_age_seconds:
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            request = payload.get("request") or {}
            events = request.get("events") or []
            result = self.publisher.client.post(self.config.platform.ingest_url, json={"events": events})
            if 200 <= result.status_code < 300:
                replayed += 1
                path.unlink(missing_ok=True)
        return replayed

    def _process_observation_payload(self, raw_observation: dict[str, object], *, connector: SatnogsNetworkConnector | None = None) -> None:
        active_connector = connector or self.network_connector
        observation_id = str(raw_observation.get("id"))
        if not active_connector.is_eligible_observation(raw_observation):
            logger.info("Skipping non-eligible observation %s", observation_id)
            return

        detail = raw_observation
        if not raw_observation.get("demoddata"):
            detail = active_connector.get_observation_detail(observation_id)
        if not active_connector.is_eligible_observation(detail):
            logger.info("Skipping observation %s after detail mismatch", observation_id)
            return
        observation = active_connector.normalize_observation(detail)
        if not self._has_demoddata(observation):
            logger.info("Skipping observation %s without demoddata", observation_id)
            return
        if observation.ground_station_id is None:
            logger.warning("Skipping observation %s without ground_station_id", observation_id)
            self._write_observation_dlq("missing_ground_station_id", observation)
            return

        try:
            frames, invalid_lines = active_connector.extract_frames(observation)
        except (binascii.Error, ValueError) as exc:
            logger.warning("Frame extraction failed for observation %s: %r", observation.observation_id, exc)
            self._write_observation_dlq("frame_extraction_failed", observation, extra={"error": repr(exc)})
            return
        logger.info(
            "Extracted SatNOGS frames: observation_id=%s ground_station_id=%s frames=%s invalid_lines=%s",
            observation.observation_id,
            observation.ground_station_id,
            len(frames),
            len(invalid_lines),
        )
        for item in invalid_lines:
            self.dlq.write(
                "frame",
                {
                    "reason": "invalid_hex_payload",
                    "observation_id": observation.observation_id,
                    "ground_station_id": observation.ground_station_id,
                    **item,
                },
            )

        if not frames:
            logger.info("No frames extracted for observation %s", observation.observation_id)
            return

        self._process_frames(observation, frames)

    def _has_demoddata(self, observation: ObservationRecord) -> bool:
        demoddata = observation.demoddata
        if isinstance(demoddata, str):
            return bool(demoddata.strip())
        if isinstance(demoddata, list):
            return any(self._has_demoddata_item(item) for item in demoddata)
        return bool(observation.artifact_refs)

    def _has_demoddata_item(self, item: object) -> bool:
        if isinstance(item, str):
            return bool(item.strip())
        if isinstance(item, dict):
            for key in ("payload_demod", "payload", "frame", "hex"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    return True
        return False

    def _process_frames(self, observation: ObservationRecord, frames: list[FrameRecord]) -> None:
        receiver_id = self.mapper.build_receiver_id(observation)
        if receiver_id is None:
            self._write_observation_dlq("missing_receiver_id", observation)
            return

        batch: list[TelemetryEvent] = []
        batch_last_frame_index = -1
        sequence_seed = 0
        skipped_non_originated = 0
        failed_ax25_decode = 0
        unknown_payload_format_count = 0
        failed_payload_decode = 0
        mapped_frame_count = 0
        mapped_event_count = 0
        skipped_published_frame_count = 0

        for frame in frames:
            try:
                ax25 = parse_ax25_frame(frame.frame_bytes)
            except ValueError as exc:
                failed_ax25_decode += 1
                self.dlq.write(
                    "frame",
                    {
                        "reason": "ax25_decode_failed",
                        "observation_id": observation.observation_id,
                        "ground_station_id": observation.ground_station_id,
                        "frame_index": frame.frame_index,
                        "raw_line": frame.raw_line,
                        "error_message": str(exc),
                    },
                )
                continue

            if not self.mapper.is_originated_packet(ax25):
                skipped_non_originated += 1
                continue

            try:
                decoded_packet = self.payload_decode_service.decode(
                    observation=observation,
                    frame=frame,
                    ax25_packet=ax25,
                )
            except PayloadDecodeError as exc:
                failed_payload_decode += 1
                self._write_payload_dlq(
                    observation=observation,
                    frame=frame,
                    ax25=ax25,
                    error=exc,
                )
                continue

            if decoded_packet is None:
                unknown_payload_format_count += 1
                continue

            frame_events = self.mapper.map_decoded_packet(
                observation=observation,
                frame=ax25,
                decoded_packet=decoded_packet,
                reception_time=frame.reception_time,
                sequence_seed=sequence_seed,
            )
            if not frame_events:
                continue

            mapped_frame_count += 1
            mapped_event_count += len(frame_events)
            sequence_seed = frame_events[-1].sequence or sequence_seed
            batch.extend(frame_events)
            batch_last_frame_index = frame.frame_index
            if len(batch) >= self.config.publisher.batch_size_events:
                if not self._flush_batch(batch, observation=observation, last_frame_index=batch_last_frame_index):
                    return
                batch = []

        if batch and not self._flush_batch(batch, observation=observation, last_frame_index=batch_last_frame_index):
            return

        logger.info(
            "Processed SatNOGS frames: observation_id=%s total_frames=%s mapped_frames=%s mapped_events=%s skipped_already_published=%s skipped_non_originated=%s failed_ax25_decode=%s unknown_payload_format_count=%s failed_payload_decode=%s",
            observation.observation_id,
            len(frames),
            mapped_frame_count,
            mapped_event_count,
            skipped_published_frame_count,
            skipped_non_originated,
            failed_ax25_decode,
            unknown_payload_format_count,
            failed_payload_decode,
        )


    def _flush_batch(self, batch: list[TelemetryEvent], *, observation: ObservationRecord, last_frame_index: int) -> bool:
        result = self.publisher.publish(
            batch,
            context={
                "observation_id": observation.observation_id,
                "ground_station_id": observation.ground_station_id,
                "stream_id": self.mapper.stream_id_for_observation(observation),
                "last_frame_index": last_frame_index,
            },
        )
        if not result.success:
            logger.warning(
                "Failed publishing SatNOGS telemetry batch: observation_id=%s stream_id=%s events=%s last_frame_index=%s status=%s body=%s",
                observation.observation_id,
                self.mapper.stream_id_for_observation(observation),
                len(batch),
                last_frame_index,
                result.status_code,
                result.response_body,
            )
            return False
        logger.info(
            "Published SatNOGS telemetry batch: observation_id=%s stream_id=%s events=%s last_frame_index=%s status=%s attempts=%s",
            observation.observation_id,
            self.mapper.stream_id_for_observation(observation),
            len(batch),
            last_frame_index,
            result.status_code,
            result.attempts,
        )
        return True

    def _write_observation_dlq(self, reason: str, observation: ObservationRecord, *, extra: dict[str, object] | None = None) -> None:
        if not self.config.dlq.write_observation_dlq:
            return
        payload = {
            "reason": reason,
            "observation_id": observation.observation_id,
            "ground_station_id": observation.ground_station_id,
            "status": observation.status,
            "raw_json": observation.raw_json,
        }
        if extra:
            payload.update(extra)
        self.dlq.write("observation", payload)

    def _write_payload_dlq(
        self,
        *,
        observation: ObservationRecord,
        frame: FrameRecord,
        ax25,
        error: PayloadDecodeError,
    ) -> None:
        payload: dict[str, Any] = {
            "reason": error.reason,
            "observation_id": observation.observation_id,
            "frame_index": frame.frame_index,
            "ground_station_id": observation.ground_station_id,
            "source_callsign": ax25.src_callsign,
            "destination_callsign": ax25.dest_callsign,
            "raw_line": frame.raw_line,
            "frame_hex": frame.frame_bytes.hex(),
            "payload_hex": ax25.info_bytes.hex(),
            "decoder_id": error.decoder_id,
            "decoder_strategy": error.decoder_strategy,
            "packet_name": error.packet_name,
            "exception_type": type(error).__name__,
            "error_message": error.error_message,
        }
        if error.metadata:
            payload["metadata"] = error.metadata
        self.dlq.write("frame", payload)
