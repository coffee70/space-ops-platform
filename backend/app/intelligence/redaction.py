"""Simple redaction helpers for logs/events."""

from __future__ import annotations

from collections.abc import Mapping

SENSITIVE_KEYS = {"authorization", "api_key", "token", "password", "cookie", "set-cookie", "secret"}


def redact(value):
    if isinstance(value, Mapping):
        out = {}
        for k, v in value.items():
            if str(k).lower() in SENSITIVE_KEYS:
                out[k] = "***REDACTED***"
            else:
                out[k] = redact(v)
        return out
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str) and len(value) > 2000:
        return value[:2000] + "...<truncated>"
    return value
