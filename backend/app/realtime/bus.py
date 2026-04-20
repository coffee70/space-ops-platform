"""In-process event bus for realtime telemetry and alerts.

Explicit interface allows swapping to Redis/NATS/Kafka later without
changing business logic. Uses a dedicated thread pool for measurement
handlers so processing does not compete with FastAPI's request handlers.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable
from typing import Any

from app.models.schemas import MeasurementEvent

logger = logging.getLogger(__name__)

# Dedicated pool for measurement processing; avoids exhausting FastAPI's default pool
_PROCESSOR_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="realtime-processor")
_MEASUREMENT_CONSUMER_COUNT = 4

# Type aliases for handlers
MeasurementHandler = Callable[[MeasurementEvent], None]
AlertHandler = Callable[[dict[str, Any]], None]


class InProcessEventBus:
    """In-memory async pub/sub for measurements and alerts."""

    def __init__(self) -> None:
        self._measurement_handlers: list[MeasurementHandler] = []
        self._alert_handlers: list[AlertHandler] = []
        self._measurement_queue: asyncio.Queue[MeasurementEvent] = asyncio.Queue()
        self._alert_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._measurement_tasks: list[asyncio.Task[None]] = []
        self._alert_task: asyncio.Task[None] | None = None

    def publish_measurement(self, event: MeasurementEvent) -> bool:
        """Publish a measurement event (non-blocking). Returns False if dropped."""
        try:
            self._measurement_queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            logger.warning("Measurement queue full, dropping event for %s", event.channel_name)
            return False

    def measurement_queue_size(self) -> int:
        """Current number of events waiting for processing."""
        return self._measurement_queue.qsize()

    def measurement_queue_maxsize(self) -> int:
        """Configured queue max size (0 means unbounded)."""
        return self._measurement_queue.maxsize

    def subscribe_measurements(self, handler: MeasurementHandler) -> None:
        """Register a handler for measurement events."""
        self._measurement_handlers.append(handler)

    def unsubscribe_measurements(self, handler: MeasurementHandler) -> None:
        """Remove a measurement handler."""
        if handler in self._measurement_handlers:
            self._measurement_handlers.remove(handler)

    def publish_alert(self, event: dict[str, Any]) -> None:
        """Publish an alert lifecycle event."""
        try:
            self._alert_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Alert queue full, dropping alert event")

    def subscribe_alerts(self, handler: AlertHandler) -> None:
        """Register a handler for alert events."""
        self._alert_handlers.append(handler)

    def unsubscribe_alerts(self, handler: AlertHandler) -> None:
        """Remove an alert handler."""
        if handler in self._alert_handlers:
            self._alert_handlers.remove(handler)

    async def _process_measurements(self) -> None:
        """Process measurement queue and fan out to handlers (handlers run in dedicated pool)."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                event = await self._measurement_queue.get()
                try:
                    futures = [
                        loop.run_in_executor(_PROCESSOR_EXECUTOR, handler, event)
                        for handler in self._measurement_handlers
                    ]
                    if futures:
                        results = await asyncio.gather(*futures, return_exceptions=True)
                        for result in results:
                            if isinstance(result, Exception):
                                logger.exception("Measurement handler error: %s", result)
                finally:
                    self._measurement_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Measurement processing error: %s", e)

    async def _process_alerts(self) -> None:
        """Process alert queue and fan out to handlers."""
        while True:
            try:
                event = await self._alert_queue.get()
                try:
                    for h in self._alert_handlers:
                        try:
                            h(event)
                        except Exception as e:
                            logger.exception("Alert handler error: %s", e)
                finally:
                    self._alert_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Alert processing error: %s", e)

    def start(self) -> None:
        """Start background consumers."""
        if self._measurement_tasks or self._alert_task is not None:
            return
        self._measurement_tasks = [
            asyncio.create_task(self._process_measurements())
            for _ in range(_MEASUREMENT_CONSUMER_COUNT)
        ]
        self._alert_task = asyncio.create_task(self._process_alerts())
        logger.info("Realtime event bus started")

    def stop(self) -> None:
        """Stop background consumers."""
        if self._alert_task is not None:
            self._alert_task.cancel()
            self._alert_task = None
        if self._measurement_tasks:
            for task in self._measurement_tasks:
                task.cancel()
            self._measurement_tasks = []
        logger.info("Realtime event bus stopped")


_bus: InProcessEventBus | None = None


def get_realtime_bus() -> InProcessEventBus:
    """Get or create the singleton event bus."""
    global _bus
    if _bus is None:
        _bus = InProcessEventBus()
        _bus.start()
    return _bus
