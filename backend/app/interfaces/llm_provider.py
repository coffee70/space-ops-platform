"""Abstract interface for LLM providers.

Extensibility hook: allows swapping to different LLM backends
(e.g., OpenAI, Anthropic, local Ollama) without changing business logic.
"""

from abc import ABC, abstractmethod


class LLMProvider(ABC):
    """Abstract base class for LLM text generation."""

    @abstractmethod
    def generate(self, prompt: str) -> str:
        """Generate text from the given prompt.

        Args:
            prompt: The prompt to send to the LLM.

        Returns:
            The generated text response.
        """
        ...
