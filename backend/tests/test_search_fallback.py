"""Tests for semantic-search fallback behavior."""

from __future__ import annotations

from unittest.mock import MagicMock

from uuid import uuid4

from app.models.telemetry import TelemetryMetadata
from app.services.telemetry_service import TelemetryService


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


def test_semantic_search_falls_back_to_lexical_matches_when_embeddings_missing(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    lexical_meta = TelemetryMetadata(
        source_id="source-a",
        name="PROP_TANK_B_PRESS",
        units="psi",
        description="Tank B pressure channel",
        subsystem_tag="prop",
        embedding=None,
    )
    db.execute.side_effect = [
        _FakeResult([]),
        _FakeResult([lexical_meta]),
        _FakeResult([]),
        _FakeResult([]),
    ]
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    results = service.semantic_search("tank b pressure", source_id="source-a")

    assert len(results) == 1
    assert results[0].name == "PROP_TANK_B_PRESS"
    assert 0.0 < results[0].match_confidence < 1.0


def test_semantic_search_matches_alias_and_returns_canonical_channel(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="PWR_MAIN_BUS_VOLT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        embedding=None,
    )
    db.execute.side_effect = [
        _FakeResult([]),  # vector results
        _FakeResult([]),  # lexical metadata results
        _FakeResult([meta]),  # alias lexical results
        _FakeResult([(meta.id, "VBAT")]),  # alias lookup
    ]
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    results = service.semantic_search("VBAT", source_id="source-a")

    assert len(results) == 1
    assert results[0].name == "PWR_MAIN_BUS_VOLT"
    assert results[0].aliases == ["VBAT"]


def test_semantic_search_deduplicates_alias_hits_before_limit(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    first_meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="PWR_MAIN_BUS_VOLT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        embedding=None,
    )
    second_meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="PWR_BACKUP_BUS_VOLT",
        units="V",
        description="Backup bus voltage",
        subsystem_tag="power",
        embedding=None,
    )
    db.execute.side_effect = [
        _FakeResult([]),  # vector results
        _FakeResult([]),  # lexical metadata results
        _FakeResult([first_meta, first_meta, second_meta]),  # duplicate alias lexical rows
        _FakeResult(
            [
                (first_meta.id, "VBAT"),
                (first_meta.id, "BATTERY_VOLT"),
                (second_meta.id, "VBUS"),
            ]
        ),  # merged alias lookup
    ]
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    results = service.semantic_search("VB", limit=2, source_id="source-a")

    assert [result.name for result in results] == [
        "PWR_MAIN_BUS_VOLT",
        "PWR_BACKUP_BUS_VOLT",
    ]


def test_semantic_search_populates_aliases_for_vector_only_results(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="PWR_MAIN_BUS_VOLT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        embedding=[0.1, 0.2, 0.3],
    )
    db.execute.side_effect = [
        _FakeResult([(meta, 0.2)]),  # vector results
        _FakeResult([(meta.id, "VBAT")]),  # aliases for vector ids
        _FakeResult([]),  # lexical metadata results
        _FakeResult([]),  # alias lexical results
        _FakeResult([(meta.id, "VBAT")]),  # merged alias refresh
    ]
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    results = service.semantic_search("power bus voltage", source_id="source-a")

    assert len(results) == 1
    assert results[0].name == "PWR_MAIN_BUS_VOLT"
    assert results[0].aliases == ["VBAT"]


def test_semantic_search_keeps_exact_lexical_match_even_when_vector_results_are_full(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    vector_rows = [
        (
            TelemetryMetadata(
                source_id="source-a",
                name=f"CATALOG_{idx}",
                units="V",
                description=f"Catalog result {idx}",
                subsystem_tag="power",
                embedding=[0.1, 0.2, 0.3],
            ),
            0.25 + idx * 0.01,
        )
        for idx in range(10)
    ]
    lexical_meta = TelemetryMetadata(
        source_id="source-a",
        name="decoder.aprs.payload_temp",
        units="",
        description="Payload temperature",
        subsystem_tag="dynamic",
        channel_origin="discovered",
        discovery_namespace="decoder.aprs",
        embedding=None,
    )

    db.execute.side_effect = [
        _FakeResult(vector_rows),
        _FakeResult([]),
        _FakeResult([lexical_meta]),
        _FakeResult([]),
        _FakeResult([]),
        _FakeResult([]),
    ]
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    results = service.semantic_search("decoder.aprs.payload_temp", source_id="source-a")

    assert len(results) == 10
    assert results[0].name == "decoder.aprs.payload_temp"
    assert results[0].channel_origin == "discovered"


def test_semantic_search_orders_lexical_fallback_before_limit(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    statements = []

    def fake_execute(statement):
        statements.append(statement)
        return _FakeResult([])

    db.execute.side_effect = fake_execute
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    service.semantic_search("decoder.aprs.payload_temp", source_id="source-a")

    lexical_sql = str(statements[1])
    assert "ORDER BY" in lexical_sql
    assert "lower(telemetry_metadata.name) =" in lexical_sql
    assert "lower(telemetry_metadata.name) LIKE" in lexical_sql


def test_semantic_search_deduplicates_alias_matches_before_limit(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    statements = []

    def fake_execute(statement):
        statements.append(statement)
        return _FakeResult([])

    db.execute.side_effect = fake_execute
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    service.semantic_search("VBAT", source_id="source-a")

    alias_sql = str(statements[-1])
    assert "GROUP BY telemetry_metadata.id, telemetry_metadata.name" in alias_sql
    assert "LIMIT" in alias_sql


def test_semantic_search_orders_alias_matches_by_alias_relevance(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    statements = []

    def fake_execute(statement):
        statements.append(statement)
        return _FakeResult([])

    db.execute.side_effect = fake_execute
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    service.semantic_search("VBAT", source_id="source-a")

    alias_sql = str(statements[-1])
    assert "lower(telemetry_channel_aliases.alias_name) =" in alias_sql
    assert "min(lower(telemetry_channel_aliases.alias_name))" in alias_sql
    assert "ORDER BY anon_1.alias_priority, anon_1.lowered_alias, anon_1.lowered_name" in alias_sql


def test_semantic_search_reranks_exact_lexical_match_already_in_vector_results(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    llm_provider = MagicMock()
    service = TelemetryService(db, embedding_provider, llm_provider)
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]

    exact_meta = TelemetryMetadata(
        source_id="source-a",
        name="decoder.aprs.payload_temp",
        units="",
        description="Payload temperature",
        subsystem_tag="dynamic",
        channel_origin="discovered",
        discovery_namespace="decoder.aprs",
        embedding=[0.1, 0.2, 0.3],
    )
    vector_rows = [
        (
            TelemetryMetadata(
                source_id="source-a",
                name="near_match",
                units="V",
                description="Near match",
                subsystem_tag="power",
                embedding=[0.1, 0.2, 0.3],
            ),
            0.02,
        ),
        (exact_meta, 0.8),
    ]

    db.execute.side_effect = [
        _FakeResult(vector_rows),
        _FakeResult([]),
        _FakeResult([exact_meta]),
        _FakeResult([]),
        _FakeResult([]),
        _FakeResult([]),
    ]
    monkeypatch.setattr(
        service,
        "get_recent_value_with_timestamp",
        lambda *args, **kwargs: None,
    )

    results = service.semantic_search("decoder.aprs.payload_temp", source_id="source-a")

    assert len(results) == 2
    assert results[0].name == "decoder.aprs.payload_temp"
    assert results[0].match_confidence == 0.99
