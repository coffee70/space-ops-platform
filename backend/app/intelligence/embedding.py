"""Embedding helpers."""

from __future__ import annotations

from app.services.embedding_service import SentenceTransformerEmbeddingProvider

DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

_provider: SentenceTransformerEmbeddingProvider | None = None


def get_embedding_provider() -> SentenceTransformerEmbeddingProvider:
    global _provider
    if _provider is None:
        _provider = SentenceTransformerEmbeddingProvider()
    return _provider
