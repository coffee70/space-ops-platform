"""Structured audit logging for backend actions."""

import logging
from datetime import datetime, timezone
from typing import Any

AUDIT_LOGGER_NAME = "app.audit"
_audit_logger: logging.Logger | None = None


def _get_audit_logger() -> logging.Logger:
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = logging.getLogger(AUDIT_LOGGER_NAME)
    return _audit_logger


def audit_log(action: str, level: str = "info", **kwargs: Any) -> None:
    """Emit a structured audit log entry."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "audit": True,
        "action": action,
        "component": "backend",
        **kwargs,
    }
    # Ensure JSON-serializable values
    def _serialize(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "__dict__") and not isinstance(obj, (str, int, float, bool, type(None))):
            return str(obj)
        return obj

    sanitized = {}
    for k, v in entry.items():
        try:
            sanitized[k] = _serialize(v)
        except (TypeError, ValueError):
            sanitized[k] = str(v)

    logger = _get_audit_logger()
    if level == "warning":
        logger.warning("audit", extra={"event": sanitized})
    elif level == "error":
        logger.error("audit", extra={"event": sanitized})
    else:
        logger.info("audit", extra={"event": sanitized})
