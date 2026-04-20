"""Feed health tracking per source: last reception, rate, state transitions."""

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEGRADED_SEC = 15.0
DISCONNECTED_SEC = 60.0


@dataclass
class SourceHealth:
    """Per-source health state."""

    source_id: str
    last_reception_time: float = 0.0
    reception_times: deque = field(default_factory=lambda: deque(maxlen=100))
    prev_state: str = "disconnected"

    def record_reception(self) -> None:
        now = time.time()
        self.last_reception_time = now
        self.reception_times.append(now)

    def approx_rate_hz(self) -> Optional[float]:
        if len(self.reception_times) < 2:
            return None
        span = self.reception_times[-1] - self.reception_times[0]
        if span <= 0:
            return None
        return (len(self.reception_times) - 1) / span

    def current_state(self) -> str:
        now = time.time()
        age = now - self.last_reception_time
        if age <= DEGRADED_SEC:
            return "connected"
        if age <= DISCONNECTED_SEC:
            return "degraded"
        return "disconnected"

    def check_transition(self) -> Optional[tuple[str, str]]:
        """Return (old_state, new_state) if transitioned, else None."""
        new_state = self.current_state()
        if new_state != self.prev_state:
            old = self.prev_state
            self.prev_state = new_state
            return (old, new_state)
        return None


class FeedHealthTracker:
    """Tracks feed health per source, notifies on transitions."""

    def __init__(self) -> None:
        self._sources: dict[str, SourceHealth] = {}
        self._lock = threading.Lock()
        self._on_transition: Optional[Callable[[str, str, str], None]] = None

    def set_on_transition(self, cb: Callable[[str, str, str], None]) -> None:
        """Set callback(source_id, old_state, new_state) for state transitions."""
        self._on_transition = cb

    def record_reception(self, source_id: str) -> None:
        """Record a reception for the source."""
        transition: Optional[tuple[str, str]] = None
        with self._lock:
            if source_id not in self._sources:
                self._sources[source_id] = SourceHealth(source_id=source_id)
            sh = self._sources[source_id]
            sh.record_reception()
            transition = sh.check_transition()
        # Emit callbacks outside lock to avoid re-entrant deadlock when callback
        # asks tracker for status (get_status/get_all_statuses).
        if transition:
            old_state, new_state = transition
            self._emit_transition(source_id, old_state, new_state)

    def _emit_transition(self, source_id: str, old_state: str, new_state: str) -> None:
        if self._on_transition:
            try:
                self._on_transition(source_id, old_state, new_state)
            except Exception as e:
                logger.exception("Feed health transition callback error: %s", e)

    def get_status(self, source_id: str) -> dict:
        """Get current status for a source."""
        with self._lock:
            return self._status_for(source_id)

    def get_all_statuses(self) -> list[dict]:
        """Get status for all known sources."""
        with self._lock:
            return [self._status_for(sid) for sid in list(self._sources.keys())]

    def _status_for(self, source_id: str) -> dict:
        """Get status for a source (caller must hold _lock)."""
        sh = self._sources.get(source_id)
        if not sh:
            return {
                "source_id": source_id,
                "connected": False,
                "state": "disconnected",
                "last_reception_time": None,
                "approx_rate_hz": None,
            }
        state = sh.current_state()
        return {
            "source_id": source_id,
            "connected": state == "connected",
            "state": state,
            "last_reception_time": sh.last_reception_time,
            "approx_rate_hz": sh.approx_rate_hz(),
        }


_tracker: Optional[FeedHealthTracker] = None


def get_feed_health_tracker() -> FeedHealthTracker:
    global _tracker
    if _tracker is None:
        _tracker = FeedHealthTracker()
    return _tracker
