"""Tests for the baseline migration layout."""

from __future__ import annotations

from pathlib import Path


def test_only_single_baseline_migration_exists() -> None:
    versions_dir = Path(__file__).resolve().parents[1] / "app" / "migrations" / "versions"
    migration_names = sorted(
        path.name
        for path in versions_dir.glob("*.py")
        if path.name != "__init__.py"
    )

    assert migration_names == ["001_initial_schema.py"]


def test_legacy_stream_identity_split_migration_is_removed() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "migrations"
        / "versions"
        / "015_stream_identity_split.py"
    )
    assert migration_path.exists() is False


def test_baseline_migration_is_static_and_does_not_import_live_metadata() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "migrations"
        / "versions"
        / "001_initial_schema.py"
    )
    contents = migration_path.read_text(encoding="utf-8")

    assert "Base.metadata.create_all" not in contents
    assert "Base.metadata.drop_all" not in contents
    assert "import app.models.telemetry" not in contents
    assert "from app.database import Base" not in contents


def test_alert_table_is_stream_scoped_not_source_fk_scoped() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "migrations"
        / "versions"
        / "001_initial_schema.py"
    )
    contents = migration_path.read_text(encoding="utf-8")
    start = contents.index('    op.create_table(\n        "telemetry_alerts",')
    end = contents.index('    op.create_index(\n        op.f("ix_telemetry_alerts_source_id"),', start)
    alert_block = contents[start:end]

    assert 'sa.Column("source_id", sa.Text(), nullable=False)' in alert_block
    assert 'sa.ForeignKey("telemetry_sources.id", ondelete="CASCADE")' not in alert_block
