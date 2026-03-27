"""
audit_ingestion_v03/audit_ingestion/router.py
Main pipeline orchestrator.

Stage 1: Extract (unified extractor with fallback chain)
Stage 2: Canonical AI extraction (single pass → AuditEvidence)
Stage 3: Score and return
"""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional
from .models import AuditEvidence, IngestionResult, ExtractionMeta, Flag
from .extractor import extract

logger = logging.getLogger(__name__)


def ingest_one(
    path: str,
    provider_name: str = "stub",
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> IngestionResult:
    """
    Ingest a single document through the v03 pipeline.
    Returns one IngestionResult containing a canonical AuditEvidence object.
    """
    from .providers import get_provider

    input_p = Path(path)
    engine_chain = []
    errors = []

    if not input_p.exists():
        return IngestionResult(
            status="failed",
            errors=["File not found"],
            evidence=AuditEvidence(
                source_file=input_p.name,
                flags=[Flag(type="file_not_found", description="File not found", severity="critical")]
            )
        )

    # Get provider
    try:
        provider = get_provider(provider_name, api_key=api_key, model=model)
    except Exception as e:
        logger.warning(f"Provider init failed: {e} — using stub")
        from .providers.stub_provider import StubProvider
        provider = StubProvider()

    # Stage 1: Extract
    raw = extract(path, provider=provider)
    engine_chain.append(raw.extractor)

    if raw.errors:
        errors.extend(raw.errors)

    extraction_meta = ExtractionMeta(
        extractor=raw.extractor,
        pages_processed=raw.page_count,
        text_chars=len(raw.text or ""),
        overall_confidence=raw.confidence,
        needs_human_review=not raw.is_sufficient,
        warnings=raw.warnings,
        errors=raw.errors,
    )

    # Stage 2: Canonical AI extraction
    if provider_name != "stub" and raw.text:
        from .canonical import extract_canonical
        evidence = extract_canonical(
            raw_text=raw.text,
            tables=raw.tables,
            source_file=input_p.name,
            page_count=raw.page_count,
            provider=provider,
            extraction_meta=extraction_meta,
        )
        engine_chain.append("canonical_ai")
    else:
        # No AI — return minimal evidence with raw text
        evidence = AuditEvidence(
            source_file=input_p.name,
            raw_text=raw.text,
            tables=raw.tables,
            extraction_meta=extraction_meta,
        )
        engine_chain.append("no_ai")

    # Stage 3: Score
    has_overview = evidence.audit_overview is not None and bool(evidence.audit_overview.summary)
    has_amounts  = len(evidence.amounts) > 0
    has_parties  = len(evidence.parties) > 0
    has_dates    = len(evidence.dates) > 0
    has_claims   = len(evidence.claims) > 0

    score = sum([
        0.20 if has_overview else 0,
        0.20 if has_amounts  else 0,
        0.20 if has_parties  else 0,
        0.15 if has_dates    else 0,
        0.15 if has_claims   else 0,
        0.10 if raw.is_sufficient else 0,
    ])

    evidence.extraction_meta.overall_confidence = score
    evidence.extraction_meta.needs_human_review = score < 0.70

    status = "success" if score >= 0.70 else ("partial" if score >= 0.30 else "failed")

    return IngestionResult(
        evidence=evidence,
        status=status,
        errors=errors,
        engine_chain=engine_chain,
    )
