"""Structured JSON logging setup for backend services."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonLogFormatter(logging.Formatter):
    """Format logs as single-line JSON for easier querying."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
        }

        event = getattr(record, "event", None)
        if isinstance(event, dict):
            payload.update(event)
        else:
            payload["message"] = record.getMessage()

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging() -> None:
    """Configure process-wide JSON logging and tune noisy loggers."""
    root = logging.getLogger()
    root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    # Keep framework noise low so ingest traces are easy to read.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Route audit events through root JSON handler.
    audit_logger = logging.getLogger("app.audit")
    audit_logger.handlers.clear()
    audit_logger.propagate = True
    audit_logger.setLevel(logging.INFO)
