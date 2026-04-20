from __future__ import annotations

from app.config import Settings


def test_settings_parse_cors_origins_list() -> None:
    settings = Settings(cors_origins=" http://localhost:3000 , https://example.com ,, ")

    assert settings.get_cors_origins_list() == [
        "http://localhost:3000",
        "https://example.com",
    ]
