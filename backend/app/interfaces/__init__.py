"""Extensibility interfaces for swappable providers."""

from app.interfaces.embedding_provider import EmbeddingProvider
from app.interfaces.llm_provider import LLMProvider

__all__ = ["EmbeddingProvider", "LLMProvider"]
