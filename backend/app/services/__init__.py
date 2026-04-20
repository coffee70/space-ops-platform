"""Business logic services.

Keep package imports lazy so test collection can import individual service modules
without pulling in heavyweight model dependencies unless they are actually needed.
"""

from importlib import import_module
from typing import Any

__all__ = [
    "SentenceTransformerEmbeddingProvider",
    "MockLLMProvider",
    "OpenAICompatibleLLMProvider",
    "TelemetryService",
    "StatisticsService",
]


def __getattr__(name: str) -> Any:
    if name == "SentenceTransformerEmbeddingProvider":
        return import_module("app.services.embedding_service").SentenceTransformerEmbeddingProvider
    if name in {"MockLLMProvider", "OpenAICompatibleLLMProvider"}:
        module = import_module("app.services.llm_service")
        return getattr(module, name)
    if name == "TelemetryService":
        return import_module("app.services.telemetry_service").TelemetryService
    if name == "StatisticsService":
        return import_module("app.services.statistics_service").StatisticsService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
