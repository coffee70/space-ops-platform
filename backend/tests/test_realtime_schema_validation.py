"""Schema validation tests for realtime ingest events."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models.schemas import MeasurementEventBatch


def test_measurement_event_rejects_legacy_source_id_only_payload() -> None:
    with pytest.raises(ValidationError):
        MeasurementEventBatch.model_validate(
            {
                "events": [
                    {
                        "source_id": "source-a",
                        "generation_time": "2026-03-26T12:00:00Z",
                        "value": 1.23,
                        "tags": {"decoder": "APRS", "field_name": "Payload Temp"},
                    }
                ]
            }
        )


def test_measurement_event_requires_channel_identifier() -> None:
    with pytest.raises(ValidationError):
        MeasurementEventBatch.model_validate(
            {
                "events": [
                    {
                        "source_id": "source-a",
                        "stream_id": "source-a-stream",
                        "generation_time": "2026-03-26T12:00:00Z",
                        "value": 1.23,
                    }
                ]
            }
        )


def test_measurement_event_accepts_dynamic_field_tags_without_channel_name() -> None:
    batch = MeasurementEventBatch.model_validate(
        {
            "events": [
                {
                    "source_id": "source-a",
                    "stream_id": "source-a-stream",
                    "generation_time": "2026-03-26T12:00:00Z",
                    "value": 1.23,
                    "tags": {"decoder": "APRS", "field_name": "Payload Temp"},
                }
            ]
        }
    )

    assert len(batch.events) == 1


def test_measurement_event_accepts_reception_time_without_generation_time() -> None:
    batch = MeasurementEventBatch.model_validate(
        {
            "events": [
                {
                    "source_id": "source-a",
                    "stream_id": "source-a-stream",
                    "reception_time": "2026-03-26T12:00:01Z",
                    "value": 1.23,
                    "tags": {"decoder": "APRS", "field_name": "Payload Temp"},
                }
            ]
        }
    )

    assert len(batch.events) == 1
    assert batch.events[0].generation_time is None
    assert batch.events[0].reception_time == "2026-03-26T12:00:01Z"


def test_measurement_event_rejects_missing_generation_and_reception_time() -> None:
    with pytest.raises(ValidationError):
        MeasurementEventBatch.model_validate(
            {
                "events": [
                    {
                        "source_id": "source-a",
                        "stream_id": "source-a-stream",
                        "value": 1.23,
                        "tags": {"decoder": "APRS", "field_name": "Payload Temp"},
                    }
                ]
            }
        )


def test_measurement_event_rejects_field_only_tags_without_namespace_context() -> None:
    with pytest.raises(ValidationError):
        MeasurementEventBatch.model_validate(
            {
                "events": [
                    {
                        "source_id": "source-a",
                        "stream_id": "source-a-stream",
                        "generation_time": "2026-03-26T12:00:00Z",
                        "value": 1.23,
                        "tags": {"field_name": "Payload Temp"},
                    }
                ]
            }
        )


def test_measurement_event_rejects_legacy_vehicle_id_runtime_contract() -> None:
    with pytest.raises(ValidationError):
        MeasurementEventBatch.model_validate(
            {
                "events": [
                    {
                        "vehicle_id": "source-a",
                        "stream_id": "source-a-stream",
                        "generation_time": "2026-03-26T12:00:00Z",
                        "value": 1.23,
                        "tags": {"decoder": "APRS", "field_name": "Payload Temp"},
                    }
                ]
            }
        )
