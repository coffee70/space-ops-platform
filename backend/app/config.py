"""Application configuration."""

import os
from functools import lru_cache

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = ConfigDict(
        env_file=".env",
        extra="ignore",
    )

    database_url: str = "postgresql://telemetry:telemetry@localhost:5432/telemetry_db"
    openai_api_key: str = ""
    openai_base_url: str = ""
    control_plane_url: str = "http://localhost:8100"
    nats_url: str = "nats://localhost:4222"
    # Comma-separated list of allowed CORS origins (e.g. https://app.example.com). Default: localhost for dev.
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    # Optional full-match regex for browser Origin (e.g. Docker/LAN UI on port 3000). Empty = disabled.
    # Example: ^http://[\w\.-]+:3000$
    cors_origin_regex: str = ""
    source_reconciliation_chunk_size_hours: int = 6
    code_indexer_poll_interval_seconds: int = 5
    code_indexer_startup_roots: str = "project/space-ops-platform,project/space-ops-apps"
    code_indexer_default_branch: str = "main"
    code_indexer_max_failed_file_preview: int = 10

    def get_cors_origins_list(self) -> list[str]:
        """Return CORS origins as a list, stripping whitespace."""
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    def get_code_indexer_startup_roots(self) -> list[str]:
        """Comma-separated managed code roots the indexer worker checks on startup."""
        return [r.strip() for r in self.code_indexer_startup_roots.split(",") if r.strip()]


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
