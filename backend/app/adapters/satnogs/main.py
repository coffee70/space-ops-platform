"""CLI entrypoint for the SatNOGS adapter."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from app.adapters.satnogs.config import AdapterConfig, load_config
from app.adapters.satnogs.connectors import SatnogsNetworkConnector
from app.adapters.satnogs.decoders import DecoderRegistry, PayloadDecodeService
from app.adapters.satnogs.dlq import FilesystemDlq
from app.adapters.satnogs.publisher import IngestPublisher, ObservationsPublisher, SourceStatePublisher
from app.adapters.satnogs.request_coordinator import CoordinatedHttpClient, SatnogsRequestCoordinator
from app.adapters.satnogs.runner import AdapterRunner
from app.adapters.satnogs.source_resolver import BackendSourceResolver, ResolvedSource

logger = logging.getLogger(__name__)


def resolve_runtime_source(config: AdapterConfig) -> ResolvedSource:
    logger.info("Resolving source for vehicle_config_path=%s", config.vehicle.vehicle_config_path)
    resolve_retry = config.publisher.retry.model_copy(
        update={
            "max_attempts": max(config.publisher.retry.max_attempts, 12),
            "backoff_seconds": min(config.publisher.retry.backoff_seconds, 1.0),
        }
    )
    resolver = BackendSourceResolver(
        resolve_url=config.platform.source_resolve_url,
        retry=resolve_retry,
        timeout_seconds=config.publisher.timeout_seconds,
    )
    source = resolver.resolve_vehicle_source(config.vehicle)
    logger.info(
        "Resolved backend source id=%s created=%s vehicle_config_path=%s history_mode=%s",
        source.id,
        source.created,
        source.vehicle_config_path,
        source.history_mode,
    )
    return source


def build_runner(config_path: str, *, startup_cutoff_time: datetime | None = None) -> AdapterRunner:
    startup_cutoff_time = startup_cutoff_time or datetime.now(timezone.utc)
    config = load_config(config_path)
    payload_decode_service = PayloadDecodeService(
        decoder_config=config.vehicle.decoder,
        registry=DecoderRegistry(),
    )
    payload_decode_service.validate_configuration()
    source = resolve_runtime_source(config)
    logger.info(
        "SatNOGS adapter startup cutoff established: monitoring_start_time=%s last_reconciled_at=%s startup_cutoff_time=%s",
        source.monitoring_start_time.isoformat(),
        source.last_reconciled_at.isoformat() if source.last_reconciled_at else None,
        startup_cutoff_time.isoformat(),
    )
    dlq = FilesystemDlq(config.dlq.root_dir)
    coordinator = SatnogsRequestCoordinator()
    network_connector = SatnogsNetworkConnector(
        config.satnogs,
        norad_id=config.vehicle.norad_id,
        client=CoordinatedHttpClient(coordinator, owner="live"),
    )
    backfill_network_connector = SatnogsNetworkConnector(
        config.satnogs,
        norad_id=config.vehicle.norad_id,
        client=CoordinatedHttpClient(coordinator, owner="backfill"),
    )
    publisher = IngestPublisher(
        ingest_url=config.platform.ingest_url,
        config=config.publisher,
        dlq=dlq,
    )
    observations_publisher = ObservationsPublisher(
        batch_upsert_url=config.platform.observations_batch_upsert_url.format(source_id=source.id),
        config=config.publisher,
        dlq=dlq,
    )
    state_publisher = SourceStatePublisher(
        backfill_progress_url=config.platform.backfill_progress_url.format(source_id=source.id),
        live_state_url=config.platform.live_state_url.format(source_id=source.id),
        config=config.publisher,
    )
    return AdapterRunner(
        config,
        network_connector=network_connector,
        backfill_network_connector=backfill_network_connector,
        publisher=publisher,
        observations_publisher=observations_publisher,
        state_publisher=state_publisher,
        dlq=dlq,
        payload_decode_service=payload_decode_service,
        source_contract=source,
        startup_cutoff_time=startup_cutoff_time,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="SatNOGS AX.25/APRS telemetry adapter")
    parser.add_argument("--config", default="app/adapters/satnogs/config.example.yaml", help="Path to adapter YAML config")
    parser.add_argument("--mode", choices=["live", "replay-dlq", "once"], default="live")
    parser.add_argument("--max-age-seconds", type=int, default=None, help="Replay only DLQ files newer than this age")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    startup_cutoff_time = datetime.now(timezone.utc)
    runner = build_runner(args.config, startup_cutoff_time=startup_cutoff_time)

    if args.mode == "live":
        runner.run_forever()
        return
    if args.mode == "once":
        runner.run_live_once()
        return
    replayed = runner.replay_batch_dlq(max_age_seconds=args.max_age_seconds)
    logging.getLogger(__name__).info("Replayed %s DLQ batches", replayed)


if __name__ == "__main__":
    main()
