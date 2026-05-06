from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from app.adapters.satnogs.config import RetryConfig, load_config
from app.adapters.satnogs.connectors import ObservationPage, SatnogsNetworkConnector, SatnogsRateLimitError
from app.adapters.satnogs.decoders import DecoderRegistry, PayloadDecodeError, PayloadDecodeService, parse_aprs_payload, parse_ax25_frame
from app.adapters.satnogs.decoders.aprs import AprsDecoder
from app.adapters.satnogs.decoders.models import DecodedPacketResult, DecoderConfig
from app.adapters.satnogs.decoders.vehicles.lasarsat_decoder import LasarsatDecoder, _LeafValue
from app.adapters.satnogs.dlq import FilesystemDlq
from app.adapters.satnogs.main import build_runner
from app.adapters.satnogs.mapper import TelemetryMapper
from app.adapters.satnogs.models import AX25Frame, FrameRecord, ObservationRecord
from app.adapters.satnogs.publisher import IngestPublisher, ObservationsPublisher
from app.adapters.satnogs.runner import AdapterRunner
from app.adapters.satnogs.source_resolver import BackendSourceResolver, ResolvedSource, SourceResolutionError

CONFIG_EXAMPLE_PATH = "backend/app/adapters/satnogs/config.example.yaml"
LAYER2_VEHICLE_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "resources" / "vehicle-configurations"


class FakeObservationsPublisher:
    def publish(self, observations, *, provider, replace_future_scheduled=True, context):
        class Result:
            success = True
            status_code = 200
            response_body = ""

        return Result()


def _encode_callsign(callsign: str, *, last: bool) -> bytes:
    base, _, ssid_raw = callsign.partition("-")
    padded = base.ljust(6)[:6]
    ssid = int(ssid_raw) if ssid_raw else 0
    body = bytes((ord(ch) << 1) for ch in padded)
    tail = ((ssid & 0x0F) << 1) | 0x60 | (0x01 if last else 0x00)
    return body + bytes([tail])


def _build_ax25_frame(*, dest: str, src: str, info: bytes) -> bytes:
    return b"".join([_encode_callsign(dest, last=False), _encode_callsign(src, last=True), bytes([0x03, 0xF0]), info])


def _config_yaml(
    *,
    source_id: str | None = None,
    source_resolve_url: str | None = None,
    decoder_strategy: str | None = None,
    decoder_id: str | None = None,
) -> str:
    platform_lines = [
        "platform:",
        '  ingest_url: "http://backend:8000/telemetry/realtime/ingest"',
        f'  source_resolve_url: "{source_resolve_url or "http://backend:8000/telemetry/sources/resolve"}"',
        '  observations_batch_upsert_url: "http://backend:8000/telemetry/sources/{source_id}/observations:batch-upsert"',
        '  backfill_progress_url: "http://backend:8000/telemetry/sources/{source_id}/backfill-progress"',
        '  live_state_url: "http://backend:8000/telemetry/sources/{source_id}/live-state"',
    ]
    return "\n".join(
        [
            *platform_lines,
            "",
            "vehicle:",
            '  slug: "iss"',
            '  name: "International Space Station"',
            "  norad_id: 25544",
            "  allowed_source_callsigns:",
            '    - "NA1SS"',
            '    - "RS0ISS"',
            '  vehicle_config_path: "vehicles/iss.yaml"',
            *(
                [
                    "  decoder:",
                    f'    strategy: "{decoder_strategy}"',
                    *( [f'    decoder_id: "{decoder_id}"'] if decoder_id is not None else [] ),
                ]
                if decoder_strategy is not None
                else []
            ),
            "",
            "satnogs:",
            '  base_url: "https://network.satnogs.org"',
            '  api_token: ""',
            '  transmitter_uuid: "tx-uuid"',
            '  status: "good"',
            "",
        ]
    )


def _payload_decode_service(config) -> PayloadDecodeService:
    service = PayloadDecodeService(decoder_config=config.vehicle.decoder, registry=DecoderRegistry())
    service.validate_configuration()
    return service


def _resolved_source(
    *,
    source_id: str = "source-uuid",
    monitoring_start_time: datetime | None = None,
    last_reconciled_at: datetime | None = None,
    history_mode: str = "live_only",
    chunk_size_hours: int = 6,
) -> ResolvedSource:
    start = monitoring_start_time or datetime(2026, 3, 31, tzinfo=timezone.utc)
    return ResolvedSource(
        id=source_id,
        name="source",
        source_type="vehicle",
        vehicle_config_path="vehicles/lasarsat.yaml",
        created=False,
        monitoring_start_time=start,
        last_reconciled_at=last_reconciled_at,
        history_mode=history_mode,
        live_state="idle",
        backfill_state="idle",
        chunk_size_hours=chunk_size_hours,
    )


def _build_lasarsat_psu_payload() -> bytes:
    return b"PSU,1,2,3,4500,5,6,7,8,7e,9,10\x00"


def test_load_config_prefers_definition_stable_field_mappings(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(_config_yaml(source_id="source-uuid"), encoding="utf-8")
    config = load_config(str(path))

    assert config.resolve_stable_field_mappings()["latitude"] == "ISS_POS_LAT_DEG"


def test_load_config_can_use_layer2_vehicle_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(LAYER2_VEHICLE_CONFIG_ROOT))

    config = load_config(CONFIG_EXAMPLE_PATH)

    assert config.load_definition().name == "LASARSAT"


def test_default_config_has_no_layer3_paths() -> None:
    raw = Path(CONFIG_EXAMPLE_PATH).read_text(encoding="utf-8")

    assert "space-ops-apps" not in raw
    assert "../" not in raw
    assert "/app/vehicle-configurations" not in raw


def test_config_rejects_source_id_override(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        _config_yaml().replace(
            '  source_resolve_url: "http://backend:8000/telemetry/sources/resolve"\n',
            '  source_resolve_url: "http://backend:8000/telemetry/sources/resolve"\n  source_id: "source-uuid"\n',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_config(str(path))


def test_config_requires_source_resolve_url(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(_config_yaml(source_resolve_url="http://backend:8000/telemetry/sources/resolve"), encoding="utf-8")

    config = load_config(str(path))
    assert config.platform.source_resolve_url == "http://backend:8000/telemetry/sources/resolve"


def test_config_rejects_missing_source_resolve_url(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        _config_yaml().replace('  source_resolve_url: "http://backend:8000/telemetry/sources/resolve"\n', ""),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="source_resolve_url"):
        load_config(str(path))


def test_config_requires_satnogs_pair_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        _config_yaml(source_id="source-uuid").replace('  transmitter_uuid: "tx-uuid"\n', ""),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="transmitter_uuid"):
        load_config(str(path))


def test_config_rejects_old_satellite_only_shape(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "\n".join(
            [
                "platform:",
                '  ingest_url: "http://backend:8000/telemetry/realtime/ingest"',
                '  source_id: "source-uuid"',
                "",
                "vehicle:",
                '  slug: "iss"',
                '  name: "International Space Station"',
                "  norad_cat_id: 25544",
                '  vehicle_config_path: "vehicles/iss.yaml"',
                "",
                "satnogs_network:",
                '  base_url: "https://network.satnogs.org"',
                "  filters:",
                "    satellite_norad_cat_id: 25544",
                "",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_config(str(path))


def test_config_defaults_decoder_to_aprs(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(_config_yaml(source_id="source-uuid"), encoding="utf-8")

    config = load_config(str(path))

    assert config.vehicle.decoder.strategy == "aprs"
    assert config.vehicle.decoder.decoder_id is None


def test_config_rejects_missing_decoder_id_for_kaitai(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(_config_yaml(source_id="source-uuid", decoder_strategy="kaitai"), encoding="utf-8")

    with pytest.raises(ValueError, match="decoder_id"):
        load_config(str(path))


def test_config_rejects_unsupported_decoder_strategy(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(_config_yaml(source_id="source-uuid", decoder_strategy="bogus"), encoding="utf-8")

    with pytest.raises(ValueError, match="strategy"):
        load_config(str(path))


def test_aprs_decoder_returns_decoded_packet_result() -> None:
    decoder = AprsDecoder()
    observation = ObservationRecord(
        observation_id="obs-1",
        satellite_norad_cat_id=25544,
        start_time=None,
        end_time=None,
        ground_station_id="42",
    )
    frame = FrameRecord(
        frame_bytes=b"",
        reception_time="2026-04-01T00:01:00Z",
        observation_id="obs-1",
        ground_station_id="42",
        source="satnogs_network",
        frame_index=0,
    )
    ax25 = parse_ax25_frame(_build_ax25_frame(dest="APRS", src="RS0ISS", info=b"!4903.50N/07201.75W> temp=40"))

    result = decoder.decode(observation=observation, frame=frame, ax25_packet=ax25)

    assert result.decode_mode == "aprs"
    assert result.decoder_strategy == "aprs"
    assert result.decoder_name == "aprs"
    assert result.packet_name == "position"
    assert result.fields["temp"] == 40.0
    assert result.raw_payload_hex == ax25.info_bytes.hex()
    assert result.metadata["raw_payload"] == "!4903.50N/07201.75W> temp=40"
    assert result.metadata["kv_fields"] == {"temp": 40.0}


def test_extract_frames_collects_invalid_hex_without_aborting() -> None:
    class FakeClient:
        def get(self, url, params=None, headers=None):
            class Response:
                def raise_for_status(self):
                    return None

                @property
                def text(self):
                    if url.endswith("/good.txt"):
                        return "414243"
                    return "not-hex"

            return Response()

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(config.satnogs, norad_id=config.vehicle.norad_id, client=FakeClient())
    observation = ObservationRecord(
        observation_id="123",
        satellite_norad_cat_id=25544,
        start_time="2026-04-01T00:00:00Z",
        end_time="2026-04-01T00:01:00Z",
        ground_station_id="42",
        demoddata=[{"payload_demod": "/good.txt"}, {"payload_demod": "/bad.txt"}],
    )

    frames, invalid_lines = connector.extract_frames(observation)

    assert len(frames) == 1
    assert frames[0].frame_bytes == b"ABC"
    assert invalid_lines[0]["frame_index"] == 1


def test_extract_frames_prefers_explicit_timestamp_over_artifact_name() -> None:
    class FakeClient:
        def get(self, url, params=None, headers=None):
            class Response:
                content = b"414243\n"

                def raise_for_status(self):
                    return None

            return Response()

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(config.satnogs, norad_id=config.vehicle.norad_id, client=FakeClient())
    observation = ObservationRecord(
        observation_id="123",
        satellite_norad_cat_id=62391,
        start_time="2026-04-01T00:00:00Z",
        end_time="2026-04-01T00:01:00Z",
        ground_station_id="42",
        demoddata=[
            {
                "payload_demod": "/media/data/2026-04-01T00-00-30Z-demod.txt",
                "timestamp": "2026-04-01T00:00:45Z",
            }
        ],
    )

    frames, invalid_lines = connector.extract_frames(observation)

    assert invalid_lines == []
    assert len(frames) == 1
    assert frames[0].reception_time == "2026-04-01T00:00:45Z"


def test_extract_frames_uses_payload_demod_filename_timestamp() -> None:
    class FakeClient:
        def get(self, url, params=None, headers=None):
            class Response:
                content = b"414243\n"

                def raise_for_status(self):
                    return None

            return Response()

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(config.satnogs, norad_id=config.vehicle.norad_id, client=FakeClient())
    observation = ObservationRecord(
        observation_id="123",
        satellite_norad_cat_id=62391,
        start_time="2026-04-01T00:00:00Z",
        end_time="2026-04-01T00:01:00Z",
        ground_station_id="42",
        demoddata=[{"payload_demod": "/media/data/2026-04-01T00-00-30Z-demod.txt"}],
    )

    frames, invalid_lines = connector.extract_frames(observation)

    assert invalid_lines == []
    assert len(frames) == 1
    assert frames[0].reception_time == "2026-04-01T00:00:30Z"


def test_extract_frames_falls_back_to_observation_end_time() -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(config.satnogs, norad_id=config.vehicle.norad_id)
    observation = ObservationRecord(
        observation_id="123",
        satellite_norad_cat_id=62391,
        start_time="2026-04-01T00:00:00Z",
        end_time="2026-04-01T00:01:00Z",
        ground_station_id="42",
        demoddata="414243",
    )

    frames, invalid_lines = connector.extract_frames(observation)

    assert invalid_lines == []
    assert len(frames) == 1
    assert frames[0].reception_time == "2026-04-01T00:01:00Z"


def test_list_recent_observations_uses_status_filter_and_link_header() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json=[{"id": 1}, {"id": 2}],
            headers={
                "Link": '<https://network.satnogs.org/api/observations/?cursor=abc&status=good>; rel="next"',
            },
        )

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(
        config.satnogs,
        norad_id=config.vehicle.norad_id,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    observation_page = connector.list_recent_observations()

    assert [item["id"] for item in observation_page.results] == [1, 2]
    assert observation_page.next_url == "https://network.satnogs.org/api/observations/?cursor=abc&status=good"
    assert seen["params"]["satellite__norad_cat_id"] == "62391"
    assert seen["params"]["transmitter_uuid"] == "C3RnLSSuaKzWhHrtJCqUgu"
    assert seen["params"]["status"] == "good"
    assert "page" not in seen["params"]
    assert "cursor" not in seen["params"]
    assert "vetted_status" not in seen["params"]
    assert "start" not in seen["params"]
    assert "end" not in seen["params"]


def test_list_recent_observations_follows_next_link_without_reapplying_params() -> None:
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return httpx.Response(200, json=[{"id": 3}])

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(
        config.satnogs,
        norad_id=config.vehicle.norad_id,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    observation_page = connector.list_recent_observations(next_url="https://network.satnogs.org/api/observations/?cursor=abc&status=good")

    assert [item["id"] for item in observation_page.results] == [3]
    assert seen_urls == ["https://network.satnogs.org/api/observations/?cursor=abc&status=good"]


def test_list_upcoming_observations_uses_upcoming_status_and_time_bounds() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=[])

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(
        config.satnogs,
        norad_id=config.vehicle.norad_id,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    connector.list_upcoming_observations(now=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc))

    assert seen["params"]["status"] == "future"
    assert seen["params"]["start"].startswith("2026-04-07T12:00:00")
    assert seen["params"]["end"].startswith("2026-04-08T12:00:00")


def test_satnogs_connector_honors_retry_after_on_rate_limit() -> None:
    requests_seen = {"count": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        requests_seen["count"] += 1
        return httpx.Response(429, json={"detail": "throttled"}, headers={"Retry-After": "120"})

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(
        config.satnogs,
        norad_id=config.vehicle.norad_id,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(SatnogsRateLimitError) as first:
        connector.list_recent_observations()

    with pytest.raises(SatnogsRateLimitError) as second:
        connector.list_recent_observations()

    assert first.value.retry_after_seconds == 120
    assert 1 <= second.value.retry_after_seconds <= 120
    assert requests_seen["count"] == 1


def test_ax25_and_aprs_decode_position_payload() -> None:
    frame = _build_ax25_frame(dest="APRS", src="RS0ISS", info=b"!4903.50N/07201.75W>123/456/A=001234 temp=40")
    ax25 = parse_ax25_frame(frame)
    aprs = parse_aprs_payload(ax25.info_bytes)

    assert ax25.src_callsign == "RS0ISS"
    assert round(aprs.fields["latitude"], 4) == 49.0583
    assert round(aprs.fields["longitude"], 4) == -72.0292
    assert aprs.fields["course_deg"] == 123.0
    assert aprs.fields["temp"] == 40.0


def test_lasarsat_decoder_parses_known_good_payload_fixture() -> None:
    decoder = LasarsatDecoder()
    observation = ObservationRecord(
        observation_id="obs-1",
        satellite_norad_cat_id=62391,
        start_time=None,
        end_time=None,
        ground_station_id="42",
    )
    full_frame = _build_ax25_frame(dest="CQ", src="OK0LSR", info=_build_lasarsat_psu_payload())
    frame = FrameRecord(
        frame_bytes=full_frame,
        reception_time=None,
        observation_id="obs-1",
        ground_station_id="42",
        source="satnogs_network",
        frame_index=0,
    )
    ax25 = AX25Frame(
        dest_callsign="CQ",
        src_callsign="OK0LSR",
        digipeater_path=[],
        control=0x03,
        pid=0xF0,
        info_bytes=_build_lasarsat_psu_payload(),
    )

    result = decoder.decode(observation=observation, frame=frame, ax25_packet=ax25)

    assert result.decode_mode == "vehicle"
    assert result.decoder_strategy == "kaitai"
    assert result.decoder_name == "lasarsat"
    assert result.packet_name == "psu"
    assert result.fields["psu_battery"] == 4500
    assert result.fields["psu_ch_state_num"] == 0x7E
    assert result.fields["psu_ch0_state"] == 0
    assert result.fields["psu_ch6_state"] == 1
    assert "psu_battery" in result.fields
    assert "psu_bat_str" not in result.fields
    assert "ctl" not in result.fields
    assert "pid" not in result.fields
    assert result.metadata["non_numeric_fields"]["psu_pass_packet_type"] == "PSU"
    assert result.metadata["full_frame_hex"] == full_frame.hex()


def test_lasarsat_decoder_preserves_integers_and_sends_non_numeric_to_metadata() -> None:
    decoder = LasarsatDecoder()
    info = b"de ok0lsr = u185833r2t24p22 ar"
    full_frame = _build_ax25_frame(dest="CQ", src="OK0LSR", info=info)
    observation = ObservationRecord(
        observation_id="obs-1",
        satellite_norad_cat_id=62391,
        start_time=None,
        end_time=None,
        ground_station_id="42",
    )
    frame = FrameRecord(
        frame_bytes=full_frame,
        reception_time=None,
        observation_id="obs-1",
        ground_station_id="42",
        source="satnogs_network",
        frame_index=0,
    )
    ax25 = AX25Frame(
        dest_callsign="CQ",
        src_callsign="OK0LSR",
        digipeater_path=[],
        control=0x03,
        pid=0xF0,
        info_bytes=info,
    )

    result = decoder.decode(observation=observation, frame=frame, ax25_packet=ax25)

    assert isinstance(result.fields["uptime_total"], int)
    assert isinstance(result.fields["reset_number"], int)
    assert "cw_beacon" not in result.fields
    assert result.metadata["non_numeric_fields"]["cw_beacon"] == "u185833r2t24p22"


def test_lasarsat_decoder_ignores_containers_and_none_values() -> None:
    decoder = LasarsatDecoder()
    leaves = decoder._collect_leaf_values(object())

    assert leaves == []


def test_lasarsat_decoder_uses_deterministic_collision_disambiguation() -> None:
    decoder = LasarsatDecoder()
    assignments = decoder._assign_field_names(
        [
            _LeafValue(path=("id2", "dest_callsign"), value="CQ"),
            _LeafValue(path=("id4", "dest_callsign"), value="OK0LSR"),
            _LeafValue(path=("psu_battery",), value=4500),
        ]
    )

    assert assignments[("psu_battery",)] == "psu_battery"
    assert assignments[("id2", "dest_callsign")] == "id2__dest_callsign"
    assert assignments[("id4", "dest_callsign")] == "id4__dest_callsign"


def test_decode_service_returns_none_only_for_cheap_gate_non_match() -> None:
    service = PayloadDecodeService(decoder_config=DecoderConfig(strategy="kaitai", decoder_id="lasarsat"), registry=DecoderRegistry())
    observation = ObservationRecord(
        observation_id="obs-1",
        satellite_norad_cat_id=62391,
        start_time=None,
        end_time=None,
        ground_station_id="42",
    )
    frame = FrameRecord(
        frame_bytes=b"",
        reception_time=None,
        observation_id="obs-1",
        ground_station_id="42",
        source="satnogs_network",
        frame_index=0,
    )
    ax25 = AX25Frame(
        dest_callsign="CQ",
        src_callsign="OK0LSR",
        digipeater_path=[],
        control=0x13,
        pid=0xF0,
        info_bytes=b"ignored",
    )

    assert service.decode(observation=observation, frame=frame, ax25_packet=ax25) is None


def test_decode_service_raises_structured_errors_for_parse_and_normalization_failures() -> None:
    kaitai_service = PayloadDecodeService(
        decoder_config=DecoderConfig(strategy="kaitai", decoder_id="lasarsat"),
        registry=DecoderRegistry(),
    )
    aprs_service = PayloadDecodeService(decoder_config=DecoderConfig(strategy="aprs"), registry=DecoderRegistry())
    observation = ObservationRecord(
        observation_id="obs-1",
        satellite_norad_cat_id=62391,
        start_time=None,
        end_time=None,
        ground_station_id="42",
    )
    frame = FrameRecord(
        frame_bytes=b"",
        reception_time=None,
        observation_id="obs-1",
        ground_station_id="42",
        source="satnogs_network",
        frame_index=0,
    )

    with pytest.raises(PayloadDecodeError) as kaitai_error:
        kaitai_service.decode(
            observation=observation,
            frame=frame,
            ax25_packet=AX25Frame(
                dest_callsign="CQ",
                src_callsign="OK0LSR",
                digipeater_path=[],
                control=0x03,
                pid=0xF0,
                info_bytes=b"broken",
            ),
        )
    assert kaitai_error.value.reason == "vehicle_decoder_parse_failed"
    assert "requested 8 bytes" in kaitai_error.value.error_message

    with pytest.raises(PayloadDecodeError, match="APRS payload did not contain numeric telemetry"):
        aprs_service.decode(
            observation=observation,
            frame=frame,
            ax25_packet=AX25Frame(
                dest_callsign="APRS",
                src_callsign="RS0ISS",
                digipeater_path=[],
                control=0x03,
                pid=0xF0,
                info_bytes=b">>>>>",
            ),
        )


def test_unsupported_decoder_pair_raises_structured_service_error() -> None:
    service = PayloadDecodeService(
        decoder_config=DecoderConfig(strategy="kaitai", decoder_id="unknown"),
        registry=DecoderRegistry(),
    )

    with pytest.raises(PayloadDecodeError, match="Unsupported payload decoder"):
        service.validate_configuration()


def test_mapper_emits_stable_and_dynamic_events() -> None:
    mapper = TelemetryMapper(
        source_id="source-uuid",
        stable_field_mappings={"latitude": "ISS_POS_LAT_DEG"},
        allowed_source_callsigns=["RS0ISS"],
        vehicle_norad_cat_id=25544,
    )
    observation = ObservationRecord(
        observation_id="obs-1",
        satellite_norad_cat_id=25544,
        transmitter_uuid="tx-uuid",
        start_time="2026-04-01T00:00:00Z",
        end_time="2026-04-01T00:01:00Z",
        ground_station_id="42",
    )
    ax25 = parse_ax25_frame(_build_ax25_frame(dest="APRS", src="RS0ISS", info=b"!4903.50N/07201.75W> temp=40"))
    decoded_packet = DecodedPacketResult(
        decode_mode="aprs",
        decoder_strategy="aprs",
        decoder_name="aprs",
        packet_name="position",
        fields={"latitude": 49.0583, "temp": 40.0},
        raw_payload_hex=ax25.info_bytes.hex(),
        metadata={},
    )

    events = mapper.map_decoded_packet(
        observation=observation,
        frame=ax25,
        decoded_packet=decoded_packet,
        reception_time="2026-04-01T00:01:00Z",
        sequence_seed=0,
    )

    stable = next(event for event in events if event.channel_name == "ISS_POS_LAT_DEG")
    dynamic = next(event for event in events if event.channel_name is None and event.tags and event.tags["field_name"] == "temp")
    assert stable.tags is not None
    assert "decoder" not in stable.tags
    assert "satnogs.transmitter_uuid" not in stable.tags
    assert dynamic.tags is not None
    assert dynamic.tags["decoder"] == "aprs"
    assert dynamic.tags["decoder_strategy"] == "aprs"
    assert dynamic.tags["packet_name"] == "position"
    assert "satnogs.transmitter_uuid" not in dynamic.tags


def test_runner_full_path_aprs_regression_still_publishes(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(_config_yaml(source_id="source-uuid"), encoding="utf-8")
    config = load_config(str(config_path))
    config.dlq.root_dir = str(tmp_path / "dlq")
    published: list[dict[str, object]] = []

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            return ObservationPage(
                results=[
                    {
                        "id": "obs-1",
                        "status": "good",
                        "satellite__norad_cat_id": 25544,
                        "transmitter_uuid": "tx-uuid",
                        "demoddata": "ignored",
                        "ground_station_id": "42",
                    }
                ]
            )

        def list_upcoming_observations(self):
            return ObservationPage(results=[])

        def is_eligible_observation(self, payload, *, status=None, require_status=True):
            return True

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=25544,
                start_time="2026-04-01T00:00:00Z",
                end_time="2026-04-01T00:01:00Z",
                ground_station_id="42",
                demoddata=payload["demoddata"],
                raw_json=payload,
            )

        def extract_frames(self, observation, *, source="satnogs_network"):
            return (
                [
                    FrameRecord(
                        frame_bytes=_build_ax25_frame(dest="APRS", src="RS0ISS", info=b"!4903.50N/07201.75W> temp=40"),
                        reception_time="2026-04-01T00:01:00Z",
                        observation_id=observation.observation_id,
                        ground_station_id=observation.ground_station_id,
                        source=source,
                        frame_index=0,
                        raw_line="ignored",
                    )
                ],
                [],
            )

    class CapturingPublisher:
        def publish(self, events, *, context):
            published.extend([event.to_payload() for event in events])

            class Result:
                success = True
                status_code = 200
                response_body = ""
                attempts = 1

            return Result()

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=CapturingPublisher(),
        observations_publisher=FakeObservationsPublisher(),
        checkpoint_store=None,
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )

    runner.run_live_once()

    assert any(event.get("channel_name") == "ISS_POS_LAT_DEG" for event in published)
    temp_event = next(event for event in published if event.get("tags", {}).get("field_name") == "temp")
    assert temp_event["tags"]["decoder"] == "aprs"
    assert temp_event["tags"]["decoder_strategy"] == "aprs"


def test_runner_full_path_lasarsat_flow_reaches_publish(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    published: list[dict[str, object]] = []

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            return ObservationPage(
                results=[
                    {
                        "id": "obs-1",
                        "status": "good",
                        "satellite__norad_cat_id": 62391,
                        "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
                        "demoddata": "ignored",
                        "ground_station_id": "42",
                    }
                ]
            )

        def list_upcoming_observations(self):
            return ObservationPage(results=[])

        def is_eligible_observation(self, payload, *, status=None, require_status=True):
            return True

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                start_time="2026-04-01T00:00:00Z",
                end_time="2026-04-01T00:01:00Z",
                ground_station_id="42",
                demoddata=payload["demoddata"],
                raw_json=payload,
            )

        def extract_frames(self, observation, *, source="satnogs_network"):
            return (
                [
                    FrameRecord(
                        frame_bytes=_build_ax25_frame(dest="CQ", src="OK0LSR", info=_build_lasarsat_psu_payload()),
                        reception_time="2026-04-01T00:01:00Z",
                        observation_id=observation.observation_id,
                        ground_station_id=observation.ground_station_id,
                        source=source,
                        frame_index=0,
                        raw_line="ignored",
                    )
                ],
                [],
            )

    class CapturingPublisher:
        def publish(self, events, *, context):
            published.extend([event.to_payload() for event in events])

            class Result:
                success = True
                status_code = 200
                response_body = ""
                attempts = 1

            return Result()

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=CapturingPublisher(),
        observations_publisher=FakeObservationsPublisher(),
        checkpoint_store=None,
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )

    runner.run_live_once()

    psu_event = next(event for event in published if event.get("tags", {}).get("field_name") == "psu_battery")
    assert psu_event["tags"]["decoder"] == "lasarsat"
    assert psu_event["tags"]["decoder_strategy"] == "kaitai"
    assert psu_event["tags"]["packet_name"] == "psu"
    assert psu_event["tags"]["field_name"] == "psu_battery"
    sequences = [event["sequence"] for event in published]
    assert sequences == list(range(1, len(published) + 1))


def test_runner_non_matching_frames_do_not_create_payload_dlq_noise(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            return ObservationPage(
                results=[
                    {
                        "id": "obs-1",
                        "status": "good",
                        "satellite__norad_cat_id": 62391,
                        "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
                        "demoddata": "ignored",
                        "ground_station_id": "42",
                    }
                ]
            )

        def list_upcoming_observations(self):
            return ObservationPage(results=[])

        def is_eligible_observation(self, payload, *, status=None, require_status=True):
            return True

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                start_time="2026-04-01T00:00:00Z",
                end_time="2026-04-01T00:01:00Z",
                ground_station_id="42",
                demoddata=payload["demoddata"],
                raw_json=payload,
            )

        def extract_frames(self, observation, *, source="satnogs_network"):
            frame_bytes = _build_ax25_frame(dest="CQ", src="OK0LSR", info=b"ignored")
            broken = bytearray(frame_bytes)
            broken[-len(b"ignored") - 2] = 0x13
            return (
                [
                    FrameRecord(
                        frame_bytes=bytes(broken),
                        reception_time="2026-04-01T00:01:00Z",
                        observation_id=observation.observation_id,
                        ground_station_id=observation.ground_station_id,
                        source=source,
                        frame_index=0,
                        raw_line="ignored",
                    )
                ],
                [],
            )

    class CapturingPublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called for unknown payloads")

    dlq = FilesystemDlq(config.dlq.root_dir)
    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=CapturingPublisher(),
        observations_publisher=FakeObservationsPublisher(),
        checkpoint_store=None,
        dlq=dlq,
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )

    runner.run_live_once()

    assert dlq.iter_kind("frame") == []

def test_runner_skips_missing_ground_station_and_writes_observation_dlq(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    checkpoint_store = None
    dlq = FilesystemDlq(config.dlq.root_dir)

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            if next_url is None:
                return ObservationPage(
                    results=[
                        {
                            "id": 123,
                            "status": "good",
                            "satellite__norad_cat_id": 62391,
                            "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
                            "demoddata": "414243",
                        }
                    ]
                )
            return ObservationPage(results=[])

        def is_eligible_observation(self, payload):
            return True

        def get_observation_detail(self, observation_id):
            raise AssertionError("detail should not be requested")

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                transmitter_uuid="C3RnLSSuaKzWhHrtJCqUgu",
                start_time="2026-04-01T00:00:00Z",
                end_time="2026-04-01T00:01:00Z",
                ground_station_id=None,
                demoddata=payload["demoddata"],
                raw_json=payload,
            )

        def extract_frames(self, observation, *, source="satnogs_network"):
            raise AssertionError("frames should not be extracted without ground_station_id")

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        checkpoint_store=checkpoint_store,
        dlq=dlq,
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )

    runner.run_live_once()

    observation_dlq = dlq.iter_kind("observation")
    assert len(observation_dlq) == 1
    payload = json.loads(observation_dlq[0].read_text(encoding="utf-8"))
    assert payload["reason"] == "missing_ground_station_id"


def test_runner_uses_link_pagination_without_local_observation_dedupe(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    dlq = FilesystemDlq(config.dlq.root_dir)
    next_urls_seen: list[str | None] = []

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            next_urls_seen.append(next_url)
            if next_url is None:
                return ObservationPage(
                    results=[
                        {
                            "id": "obs-1",
                            "status": "good",
                            "satellite__norad_cat_id": 62391,
                            "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
                            "demoddata": [{"payload_demod": "/frame.txt"}],
                            "ground_station_id": "42",
                        }
                    ],
                    next_url="https://network.satnogs.org/api/observations/?cursor=abc",
                )
            if next_url == "https://network.satnogs.org/api/observations/?cursor=abc":
                return ObservationPage(
                    results=[
                        {
                            "id": "obs-1",
                            "status": "good",
                            "satellite__norad_cat_id": 62391,
                            "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
                            "demoddata": [{"payload_demod": "/frame.txt"}],
                            "ground_station_id": "42",
                        }
                    ]
                )
            raise AssertionError(f"unexpected next_url={next_url}")

        def is_eligible_observation(self, payload):
            return False

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                start_time="2026-04-01T00:00:00Z",
                end_time="2026-04-01T00:01:00Z",
                ground_station_id="42",
                transmitter_uuid="C3RnLSSuaKzWhHrtJCqUgu",
                raw_json=payload,
            )

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called for ineligible observations")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        dlq=dlq,
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )

    runner.run_live_once()

    assert next_urls_seen == [None, "https://network.satnogs.org/api/observations/?cursor=abc"]


def test_runner_syncs_upcoming_observations_with_replacement_payload(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    captured: dict[str, object] = {}

    class FakeNetworkConnector:
        def list_upcoming_observations(self):
            return ObservationPage(
                results=[
                    {
                        "id": "future-1",
                        "status": "future",
                        "satellite__norad_cat_id": 62391,
                        "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
                        "start": "2026-04-07T12:00:00Z",
                        "end": "2026-04-07T12:10:00Z",
                        "ground_station_id": "42",
                        "station_callsign": "GS42",
                        "max_elevation": 51.5,
                    }
                ]
            )

        def is_eligible_observation(self, payload, *, status=None, require_status=True):
            return status == "future" and require_status is False

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                transmitter_uuid="C3RnLSSuaKzWhHrtJCqUgu",
                start_time=payload["start"],
                end_time=payload["end"],
                ground_station_id=payload["ground_station_id"],
                station_callsign=payload["station_callsign"],
                status=payload["status"],
                raw_json=payload,
            )

        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            return ObservationPage(results=[])

    class CapturingObservationsPublisher:
        def publish(self, observations, *, provider, replace_future_scheduled=True, context):
            captured["observations"] = observations
            captured["provider"] = provider
            captured["replace_future_scheduled"] = replace_future_scheduled

            class Result:
                success = True
                status_code = 200
                response_body = ""

            return Result()

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("telemetry publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=CapturingObservationsPublisher(),
        checkpoint_store=None,
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=datetime(2026, 3, 31, tzinfo=timezone.utc),
    )

    runner.run_live_once()

    assert captured["provider"] == "satnogs"
    assert captured["replace_future_scheduled"] is True
    assert captured["observations"] == [
        {
            "external_id": "future-1",
            "status": "scheduled",
            "start_time": "2026-04-07T12:00:00Z",
            "end_time": "2026-04-07T12:10:00Z",
            "station_name": "GS42",
            "station_id": "42",
            "receiver_id": "satnogs-station-42",
            "details": {
                "satnogs_status": "future",
                "satellite_norad_cat_id": 62391,
                "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
            },
            "max_elevation_deg": 51.5,
        }
    ]


def test_live_poll_filters_by_startup_cutoff_and_stops_pagination(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    cutoff = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    calls: list[tuple[str | None, str | None, str | None]] = []
    processed: list[str] = []

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            del now
            calls.append((next_url, start_time, end_time))
            if next_url is not None:
                raise AssertionError("live pagination should stop at the cutoff")
            return ObservationPage(
                results=[
                    {"id": "new", "start": "2026-04-10T12:05:00Z", "end": "2026-04-10T12:10:00Z"},
                    {"id": "span", "start": "2026-04-10T11:55:00Z", "end": "2026-04-10T12:02:00Z"},
                    {"id": "old", "start": "2026-04-10T11:50:00Z", "end": "2026-04-10T11:59:00Z"},
                ],
                next_url="https://network.satnogs.org/api/observations/?cursor=old",
            )

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                start_time=payload.get("start"),
                end_time=payload.get("end"),
                ground_station_id="42",
                transmitter_uuid="C3RnLSSuaKzWhHrtJCqUgu",
                raw_json=payload,
            )

        def list_upcoming_observations(self):
            return ObservationPage(results=[])

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(),
        startup_cutoff_time=cutoff,
    )
    runner._process_observation_payload = lambda raw, *, connector=None: processed.append(str(raw["id"]))

    runner.run_live_once()

    assert calls == [(None, None, None)]
    assert processed == ["new", "span"]


def test_backfill_filters_chunk_window_and_skips_bad_timestamps(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    start = datetime(2026, 4, 10, 10, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 10, 11, tzinfo=timezone.utc)
    progress: list[dict[str, object]] = []
    processed: list[str] = []
    calls: list[tuple[str | None, str | None, str | None]] = []

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            del now
            calls.append((next_url, start_time, end_time))
            if next_url is None:
                return ObservationPage(
                    results=[
                        {"id": "inside", "start": "2026-04-10T10:10:00Z", "end": "2026-04-10T10:20:00Z"},
                        {"id": "bad", "start": "not-a-date", "end": "2026-04-10T10:25:00Z"},
                    ],
                    next_url="https://network.satnogs.org/api/observations/?cursor=next",
                )
            return ObservationPage(
                results=[
                    {"id": "outside", "start": "2026-04-10T10:50:00Z", "end": "2026-04-10T11:01:00Z"},
                    {"id": "inside-next", "start": "2026-04-10T10:30:00Z", "end": "2026-04-10T10:40:00Z"},
                ]
            )

        def normalize_observation(self, payload):
            return ObservationRecord(
                observation_id=str(payload["id"]),
                satellite_norad_cat_id=62391,
                start_time=payload.get("start"),
                end_time=payload.get("end"),
                ground_station_id="42",
                transmitter_uuid="C3RnLSSuaKzWhHrtJCqUgu",
                raw_json=payload,
            )

    class FakeStatePublisher:
        def publish_backfill_progress(self, payload):
            progress.append(payload)

            class Result:
                success = True
                status_code = 200
                response_body = ""

            return Result()

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        state_publisher=FakeStatePublisher(),
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(
            monitoring_start_time=start,
            last_reconciled_at=start,
            history_mode="time_window_replay",
        ),
        startup_cutoff_time=cutoff,
    )
    runner._process_observation_payload = lambda raw, *, connector=None: processed.append(str(raw["id"]))

    runner.run_backfill_snapshot()

    assert calls == [
        (None, start.isoformat(), cutoff.isoformat()),
        ("https://network.satnogs.org/api/observations/?cursor=next", None, None),
    ]
    assert processed == ["inside", "inside-next"]
    assert [item["status"] for item in progress] == ["started", "completed"]
    assert progress[-1]["chunk_end"] == cutoff.isoformat()


def test_backfill_snapshot_uses_platform_chunk_bounds(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    seen: list[tuple[str | None, str | None, str | None]] = []
    progress: list[dict[str, object]] = []
    start = datetime(2026, 4, 10, 10, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 10, 11, tzinfo=timezone.utc)

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            seen.append((next_url, start_time, end_time))
            if next_url is None:
                return ObservationPage(results=[], next_url=None)
            raise AssertionError("backfill should stop on the empty first page")

    class FakeStatePublisher:
        def publish_backfill_progress(self, payload):
            progress.append(payload)

            class Result:
                success = True
                status_code = 200
                response_body = ""

            return Result()

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        state_publisher=FakeStatePublisher(),
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(
            monitoring_start_time=start,
            last_reconciled_at=start,
            history_mode="time_window_replay",
        ),
        startup_cutoff_time=cutoff,
    )

    runner.run_backfill_snapshot()

    assert len(seen) == 1
    assert seen[0][0] is None
    assert seen[0][1] == start.isoformat()
    assert seen[0][2] == cutoff.isoformat()
    assert progress[0]["status"] == "started"
    assert progress[-1]["status"] == "completed"
    assert progress[-1]["backlog_drained"] is True


def test_backfill_snapshot_never_starts_before_monitoring_start(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    seen: list[tuple[str | None, str | None]] = []
    monitoring_start = datetime(2026, 4, 10, 10, tzinfo=timezone.utc)
    stale_checkpoint = datetime(2026, 4, 9, 10, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 10, 11, tzinfo=timezone.utc)

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            del next_url, now
            seen.append((start_time, end_time))
            return ObservationPage(results=[], next_url=None)

    class FakeStatePublisher:
        def publish_backfill_progress(self, payload):
            class Result:
                success = True
                status_code = 200
                response_body = ""

            return Result()

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        state_publisher=FakeStatePublisher(),
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(
            monitoring_start_time=monitoring_start,
            last_reconciled_at=stale_checkpoint,
            history_mode="time_window_replay",
        ),
        startup_cutoff_time=cutoff,
    )

    runner.run_backfill_snapshot()

    assert seen == [(monitoring_start.isoformat(), cutoff.isoformat())]


def test_backfill_snapshot_retries_same_chunk_after_satnogs_rate_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    seen: list[tuple[str | None, str | None]] = []
    progress: list[dict[str, object]] = []
    sleeps: list[int] = []
    start = datetime(2026, 4, 10, 10, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 10, 11, tzinfo=timezone.utc)

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            del next_url, now
            seen.append((start_time, end_time))
            if len(seen) == 1:
                raise SatnogsRateLimitError(7)
            return ObservationPage(results=[], next_url=None)

    class FakeStatePublisher:
        def publish_backfill_progress(self, payload):
            progress.append(payload)

            class Result:
                success = True
                status_code = 200
                response_body = ""

            return Result()

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    monkeypatch.setattr("app.adapters.satnogs.runner.time.sleep", lambda seconds: sleeps.append(seconds))
    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        state_publisher=FakeStatePublisher(),
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(
            monitoring_start_time=start,
            last_reconciled_at=start,
            history_mode="time_window_replay",
        ),
        startup_cutoff_time=cutoff,
    )

    runner.run_backfill_snapshot()

    assert sleeps == [7]
    assert len(seen) == 2
    assert seen[0] == seen[1]
    assert [item["status"] for item in progress] == ["started", "completed"]


def test_backfill_snapshot_reports_failed_without_completion_on_non_rate_limit_error(tmp_path: Path) -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.dlq.root_dir = str(tmp_path / "dlq")
    progress: list[dict[str, object]] = []
    start = datetime(2026, 4, 10, 10, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 10, 11, tzinfo=timezone.utc)

    class FakeNetworkConnector:
        def list_recent_observations(self, *, next_url=None, start_time=None, end_time=None, now=None):
            del next_url, start_time, end_time, now
            raise RuntimeError("satnogs exploded")

    class FakeStatePublisher:
        def publish_backfill_progress(self, payload):
            progress.append(payload)

            class Result:
                success = True
                status_code = 200
                response_body = ""

            return Result()

    class FakePublisher:
        def publish(self, events, *, context):
            raise AssertionError("publisher should not be called")

    runner = AdapterRunner(
        config,
        network_connector=FakeNetworkConnector(),
        publisher=FakePublisher(),
        observations_publisher=FakeObservationsPublisher(),
        state_publisher=FakeStatePublisher(),
        dlq=FilesystemDlq(config.dlq.root_dir),
        payload_decode_service=_payload_decode_service(config),
        source_contract=_resolved_source(
            monitoring_start_time=start,
            last_reconciled_at=start,
            history_mode="time_window_replay",
        ),
        startup_cutoff_time=cutoff,
    )

    with pytest.raises(RuntimeError, match="satnogs exploded"):
        runner.run_backfill_snapshot()

    assert [item["status"] for item in progress] == ["started", "failed"]
    assert "completed" not in {item["status"] for item in progress}


def test_satnogs_connector_rejects_mismatched_status() -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    connector = SatnogsNetworkConnector(config.satnogs, norad_id=config.vehicle.norad_id)

    assert connector.is_eligible_observation(
        {
            "id": "obs-1",
            "satellite__norad_cat_id": 62391,
            "transmitter_uuid": "C3RnLSSuaKzWhHrtJCqUgu",
            "status": "bad",
        }
    ) is False


def test_publisher_retries_timeout_then_succeeds(tmp_path: Path) -> None:
    attempts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise httpx.TimeoutException("timed out")
        return httpx.Response(200, json={"accepted": 1})

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    config.publisher.retry.max_attempts = 2
    config.publisher.retry.backoff_seconds = 0
    publisher = IngestPublisher(
        ingest_url="http://backend:8000/telemetry/realtime/ingest",
        config=config.publisher,
        dlq=FilesystemDlq(str(tmp_path / "dlq")),
        client=client,
    )
    event = TelemetryMapper(
        source_id="source-uuid",
        stable_field_mappings={"latitude": "ISS_POS_LAT_DEG"},
        allowed_source_callsigns=["RS0ISS"],
        vehicle_norad_cat_id=25544,
    ).map_packet(
        observation=ObservationRecord(
            observation_id="obs-1",
            satellite_norad_cat_id=25544,
            start_time="2026-04-01T00:00:00Z",
            end_time="2026-04-01T00:01:00Z",
            ground_station_id="42",
        ),
        frame=parse_ax25_frame(_build_ax25_frame(dest="APRS", src="RS0ISS", info=b"!4903.50N/07201.75W>")),
        aprs_packet=parse_aprs_payload(b"!4903.50N/07201.75W>"),
        reception_time="2026-04-01T00:01:00Z",
        sequence_seed=0,
    )[0]

    result = publisher.publish([event], context={"observation_id": "obs-1"})

    assert result.success is True
    assert attempts["count"] == 2


def test_observations_publisher_posts_batch_upsert_payload(tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, json={"inserted": 1, "deleted": 0})

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    publisher = ObservationsPublisher(
        batch_upsert_url="http://backend:8000/telemetry/sources/source-uuid/observations:batch-upsert",
        config=config.publisher,
        dlq=FilesystemDlq(str(tmp_path / "dlq")),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = publisher.publish(
        [{"external_id": "future-1", "status": "scheduled", "start_time": "2026-04-07T12:00:00Z", "end_time": "2026-04-07T12:10:00Z"}],
        provider="satnogs",
        replace_future_scheduled=True,
        context={"source_id": "source-uuid"},
    )

    assert result.success is True
    assert seen["json"] == {
        "provider": "satnogs",
        "replace_future_scheduled": True,
        "observations": [
            {
                "external_id": "future-1",
                "status": "scheduled",
                "start_time": "2026-04-07T12:00:00Z",
                "end_time": "2026-04-07T12:10:00Z",
            }
        ],
    }


def test_source_resolver_posts_vehicle_request_and_parses_response() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "id": "resolved-source",
                "name": "LASARSAT",
                "description": None,
                "source_type": "vehicle",
                "base_url": None,
                "vehicle_config_path": "vehicles/lasarsat.yaml",
                "created": False,
                "monitoring_start_time": "2026-04-10T00:00:00Z",
                "last_reconciled_at": None,
                "history_mode": "time_window_replay",
                "live_state": "idle",
                "backfill_state": "idle",
                "active_backfill_target_time": None,
                "chunk_size_hours": 6,
            },
        )

    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    resolver = BackendSourceResolver(
        resolve_url="http://backend:8000/telemetry/sources/resolve",
        retry=RetryConfig(max_attempts=1, backoff_seconds=0),
        timeout_seconds=1,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    source = resolver.resolve_vehicle_source(config.vehicle)

    assert source.id == "resolved-source"
    assert source.created is False
    assert source.history_mode == "time_window_replay"
    assert source.chunk_size_hours == 6
    assert seen["json"] == {
        "source_type": "vehicle",
        "name": "LASARSAT",
        "description": "Auto-resolved from vehicle configuration: vehicles/lasarsat.yaml",
        "vehicle_config_path": "vehicles/lasarsat.yaml",
        "monitoring_start_time": "2026-04-10T00:00:00+00:00",
    }


def test_source_resolver_fails_on_non_success_response() -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    resolver = BackendSourceResolver(
        resolve_url="http://backend:8000/telemetry/sources/resolve",
        retry=RetryConfig(max_attempts=1, backoff_seconds=0),
        timeout_seconds=1,
        client=httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(400, text="bad path"))),
    )

    with pytest.raises(SourceResolutionError, match="status=400"):
        resolver.resolve_vehicle_source(config.vehicle)


def test_source_resolver_fails_on_malformed_response() -> None:
    config = load_config("backend/app/adapters/satnogs/config.example.yaml")
    resolver = BackendSourceResolver(
        resolve_url="http://backend:8000/telemetry/sources/resolve",
        retry=RetryConfig(max_attempts=1, backoff_seconds=0),
        timeout_seconds=1,
        client=httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={"id": "missing"}))),
    )

    with pytest.raises(SourceResolutionError, match="Malformed source resolve response"):
        resolver.resolve_vehicle_source(config.vehicle)


def test_build_runner_rejects_source_id_override(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        _config_yaml().replace(
            '  source_resolve_url: "http://backend:8000/telemetry/sources/resolve"\n',
            '  source_resolve_url: "http://backend:8000/telemetry/sources/resolve"\n  source_id: "override-source"\n',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        build_runner(str(path))


def test_build_runner_resolves_source_id_when_override_absent(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(_config_yaml(source_resolve_url="http://backend:8000/telemetry/sources/resolve"), encoding="utf-8")
    calls = {"count": 0}

    class FakeResolver:
        def __init__(self, **kwargs):
            assert kwargs["resolve_url"] == "http://backend:8000/telemetry/sources/resolve"

        def resolve_vehicle_source(self, vehicle):
            calls["count"] += 1

            class Source:
                id = "resolved-source"
                created = False
                vehicle_config_path = vehicle.vehicle_config_path
                history_mode = "live_only"
                live_state = "idle"
                backfill_state = "complete"
                monitoring_start_time = datetime.now(timezone.utc)
                last_reconciled_at = None
                chunk_size_hours = 6

            return Source()

    monkeypatch.setattr("app.adapters.satnogs.main.BackendSourceResolver", FakeResolver)

    runner = build_runner(str(path))

    assert runner.mapper.source_id == "resolved-source"
    assert calls["count"] == 1


def test_build_runner_fails_fast_on_unsupported_decoder_pair(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        _config_yaml(source_id="source-uuid", decoder_strategy="kaitai", decoder_id="unknown"),
        encoding="utf-8",
    )

    with pytest.raises(PayloadDecodeError, match="Unsupported payload decoder"):
        build_runner(str(path))
