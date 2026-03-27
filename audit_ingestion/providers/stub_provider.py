"""
audit_ingestion_v03/audit_ingestion/providers/stub_provider.py
"""
from .base import AIProvider


class StubProvider(AIProvider):
    def _call(self, system: str, user: str, max_tokens: int = 3000) -> str:
        return ""

    def extract_text_from_pdf_vision(self, pdf_bytes: bytes, max_pages: int = 6) -> str:
        return ""
