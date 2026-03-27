"""
audit_ingestion_v03/audit_ingestion/providers/base.py
"""
from __future__ import annotations
from abc import ABC
from typing import Optional


class AIProvider(ABC):
    def _call(self, system: str, user: str, max_tokens: int = 3000) -> str:
        raise NotImplementedError

    def extract_text_from_pdf_vision(self, pdf_bytes: bytes, max_pages: int = 6) -> str:
        return ""


def get_provider(
    provider_name: str = "stub",
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> AIProvider:
    if provider_name == "openai":
        from .openai_provider import OpenAIProvider
        return OpenAIProvider(api_key=api_key, model=model or "gpt-4o-mini")
    elif provider_name in ("anthropic", "claude"):
        from .anthropic_provider import AnthropicProvider
        return AnthropicProvider(api_key=api_key, model=model or "claude-sonnet-4-20250514")
    else:
        from .stub_provider import StubProvider
        return StubProvider()
