"""Realtime telemetry processing: event bus, processor, WebSocket hub."""

from app.realtime.bus import get_realtime_bus
from app.realtime.processor import RealtimeProcessor

_processor: RealtimeProcessor | None = None


def get_realtime_processor() -> RealtimeProcessor:
    """Get or create the singleton RealtimeProcessor."""
    global _processor
    if _processor is None:
        _processor = RealtimeProcessor()
        _processor.start()
    return _processor


__all__ = ["get_realtime_bus", "get_realtime_processor", "RealtimeProcessor"]
