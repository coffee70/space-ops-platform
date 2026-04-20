"""Abstract interface for embedding providers.

Extensibility hook: allows swapping to different embedding backends
(e.g., OpenAI, Cohere, local models) without changing business logic.
"""

from abc import ABC, abstractmethod


class EmbeddingProvider(ABC):
    """Abstract base class for embedding generation."""

    @abstractmethod
    def embed(self, text: str) -> list[float]:
        """Generate embedding vector for the given text.

        Args:
            text: Input text to embed.

        Returns:
            List of floats representing the embedding vector.
        """
        ...
