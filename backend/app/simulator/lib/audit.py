"""Structured audit logging for simulator actions."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

AUDIT_LOGGER_NAME = "simulator.audit"
_audit_logger: logging.Logger | None = None


def _get_audit_logger() -> logging.Logger:
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = logging.getLogger(AUDIT_LOGGER_NAME)
        _audit_logger.setLevel(logging.INFO)
        _audit_logger.propagate = False
        _handler = logging.StreamHandler(sys.stdout)
        _handler.setFormatter(logging.Formatter("%(message)s"))
        _audit_logger.addHandler(_handler)
    return _audit_logger


def audit_log(action: str, level: str = "info", **kwargs: Any) -> None:
    """Emit a structured audit log entry as JSON to stdout."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "audit": True,
        "action": action,
        "component": "simulator",
        **kwargs,
    }

    def _serialize(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "__dict__") and not isinstance(
            obj, (str, int, float, bool, type(None))
        ):
            return str(obj)
        return obj

    sanitized = {}
    for k, v in entry.items():
        try:
            sanitized[k] = _serialize(v)
        except (TypeError, ValueError):
            sanitized[k] = str(v)

    logger = _get_audit_logger()
    msg = json.dumps(sanitized)
    if level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.info(msg)
