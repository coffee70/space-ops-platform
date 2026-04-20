"""SentenceTransformer-based embedding provider."""

import logging
from typing import List

from sentence_transformers import SentenceTransformer  # type: ignore

from app.interfaces.embedding_provider import EmbeddingProvider

logger = logging.getLogger(__name__)


class SentenceTransformerEmbeddingProvider(EmbeddingProvider):
    """Embedding provider using sentence-transformers (all-MiniLM-L6-v2, 384 dims)."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        """Initialize the model.

        Args:
            model_name: HuggingFace model identifier.
        """
        self._model = SentenceTransformer(model_name)
        logger.info("Loaded embedding model: %s", model_name)

    def embed(self, text: str) -> List[float]:
        """Generate 384-dimensional embedding for the given text."""
        if not text or not text.strip():
            text = " "
        embedding = self._model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
