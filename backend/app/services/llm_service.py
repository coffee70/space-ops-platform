"""LLM provider implementations."""

import logging
from typing import Optional

from openai import OpenAI

from app.config import get_settings
from app.interfaces.llm_provider import LLMProvider

logger = logging.getLogger(__name__)


class MockLLMProvider(LLMProvider):
    """Mock LLM provider for local dev without API key."""

    def generate(self, prompt: str) -> str:
        """Return a static explanation string."""
        return (
            "This is a mock explanation. Configure OPENAI_API_KEY and use "
            "OpenAICompatibleLLMProvider for real LLM-generated explanations."
        )


class OpenAICompatibleLLMProvider(LLMProvider):
    """LLM provider using OpenAI-compatible API (OpenAI, Ollama, etc.)."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: str = "gpt-4o-mini",
    ) -> None:
        """Initialize the OpenAI client.

        Args:
            api_key: API key (defaults to OPENAI_API_KEY env).
            base_url: Base URL for API (defaults to OPENAI_BASE_URL env).
            model: Model name to use.
        """
        settings = get_settings()
        self._api_key = api_key or settings.openai_api_key
        self._base_url = base_url or settings.openai_base_url or None
        self._model = model
        self._client = OpenAI(
            api_key=self._api_key or "not-needed",
            base_url=self._base_url,
        )

    def generate(self, prompt: str) -> str:
        """Generate text using the configured model."""
        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            logger.exception("LLM API error: %s", e)
            raise
