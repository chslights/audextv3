"""
audit_ingestion_v03/audit_ingestion/canonical.py
Single AI pass that returns canonical AuditEvidence JSON.

One prompt. One response. Everything an auditor needs.
- Document family + subtype classification
- Audit overview (summary, areas, assertions, match targets)
- Universal facts (parties, amounts, dates, identifiers, assets)
- Atomic facts with provenance
- Auditor-readable claims
- Audit flags
- Link keys for cross-document matching
- Optional document-specific extras
"""
from __future__ import annotations
import json
import logging
from typing import Optional
from .models import (
    AuditEvidence, AuditOverview, AuditPeriod, LinkKeys,
    ExtractionMeta, DocumentFamily,
    Party, Amount, DateItem, Identifier, AssetItem,
    Fact, Claim, Flag, Provenance,
)

logger = logging.getLogger(__name__)

CANONICAL_SYSTEM_PROMPT = """You are a senior CPA auditor extracting audit evidence from financial documents.

Your job is to produce a canonical audit evidence record — one structured JSON object that captures everything an auditor needs from this document, regardless of document type.

This system handles leases, invoices, checks, bank statements, grants, payroll, board minutes, contracts, and any other financial document. Do not assume a specific document type.

EXTRACTION RULES:
1. Extract ONLY facts explicitly stated in the document — never infer or calculate
2. Every non-trivial fact must include: page number, short source quote (≤20 words), confidence (0.0-1.0)
3. party_role should describe what the party IS in this document (lessor, vendor, grantor, payer, client, etc.)
4. normalized names must be UPPERCASE with no punctuation (for matching)
5. amounts must be numeric values, never strings with $ signs
6. dates must be YYYY-MM-DD format
7. Use null for any field you cannot confidently extract — do not guess
8. facts[] contains atomic matchable items; claims[] contains auditor-readable interpretations
9. link_keys contains normalized values for cross-document matching

AUDIT AREAS (use these exact strings):
cash, receivables, payables, inventory, fixed_assets, leases, prepaid, 
investments, debt, equity, revenue, expenses, payroll, grants, taxes,
insurance, commitments, contingencies, disclosures, governance, other

ASSERTIONS (use these exact strings):
existence, completeness, accuracy, cutoff, classification, 
rights_and_obligations, valuation, presentation, disclosure

Return ONLY valid JSON matching this exact structure. No markdown, no explanation."""

CANONICAL_USER_PROMPT = """Extract a canonical audit evidence record from this document.

Filename: {filename}
Pages processed: {pages}
Extracted text:

{text}

{table_section}

Return this exact JSON structure (fill all sections, use null for missing values):

{{
  "family": "contract_agreement | invoice_receipt | payment_proof | bank_cash_activity | payroll_support | accounting_report | governance_approval | grant_donor_funding | tax_regulatory | correspondence | schedule_listing | other",
  "subtype": "specific subtype e.g. vehicle_lease, vendor_invoice, check_payment, grant_award_letter",
  "title": "document title if identifiable, else null",
  "audit_overview": {{
    "summary": "2-3 sentence plain English summary of what this document is and why it matters for audit",
    "audit_areas": ["list", "of", "audit", "areas"],
    "assertions": ["list", "of", "assertions", "supported"],
    "period": {{
      "effective_date": "YYYY-MM-DD or null",
      "start": "YYYY-MM-DD or null",
      "end": "YYYY-MM-DD or null",
      "term_months": null
    }},
    "match_targets": ["list of GL accounts, records, or documents this should be matched against"]
  }},
  "parties": [
    {{"role": "lessor/vendor/grantor/payer/etc", "name": "Name as written", "normalized": "NAME UPPERCASE", "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "amounts": [
    {{"type": "amount_type", "value": 0.00, "currency": "USD", "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "dates": [
    {{"type": "date_type", "value": "YYYY-MM-DD", "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "identifiers": [
    {{"type": "id_type", "value": "id_value", "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "assets": [
    {{"type": "asset_type", "description": "description", "value": null, "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "facts": [
    {{"label": "snake_case_label", "value": "value", "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "claims": [
    {{"statement": "Plain English auditor claim", "audit_area": "area", "basis_fact_labels": ["fact_label"], "provenance": {{"page": 1, "quote": "short quote", "confidence": 0.95}}}}
  ],
  "flags": [
    {{"type": "flag_type", "description": "description", "severity": "info | warning | critical"}}
  ],
  "link_keys": {{
    "party_names": ["NORMALIZED UPPERCASE NAMES"],
    "document_numbers": ["doc numbers"],
    "agreement_numbers": ["agreement numbers"],
    "invoice_numbers": ["invoice numbers"],
    "asset_descriptions": ["ASSET DESCRIPTIONS UPPERCASE"],
    "recurring_amounts": [0.00],
    "key_dates": ["YYYY-MM-DD"],
    "other_ids": ["other identifiers"]
  }},
  "document_specific": {{}}
}}"""


def build_table_section(tables: list[dict]) -> str:
    if not tables:
        return ""
    lines = ["\nTables extracted from document:"]
    for i, tbl in enumerate(tables[:5]):
        lines.append(f"\nTable {i+1} (page {tbl.get('page', '?')}):")
        headers = tbl.get("headers", [])
        rows = tbl.get("rows", [])
        if headers:
            lines.append("Headers: " + " | ".join(str(h) for h in headers))
        for row in rows[:10]:
            lines.append("  " + " | ".join(str(v) for v in row.values()))
    return "\n".join(lines)


def _parse_provenance(d: Optional[dict]) -> Optional[Provenance]:
    if not d:
        return None
    return Provenance(
        page=d.get("page"),
        quote=d.get("quote"),
        confidence=float(d.get("confidence", 0.0)),
    )


def _parse_canonical_json(data: dict, source_file: str, raw_text: str,
                           tables: list, extraction_meta: ExtractionMeta) -> AuditEvidence:
    """Parse the AI response dict into a validated AuditEvidence object."""

    # Family
    family_val = data.get("family", "other")
    try:
        family = DocumentFamily(family_val)
    except ValueError:
        family = DocumentFamily.OTHER

    # Audit overview
    overview_data = data.get("audit_overview") or {}
    period_data = overview_data.get("period") or {}
    overview = AuditOverview(
        summary=overview_data.get("summary", ""),
        audit_areas=overview_data.get("audit_areas", []),
        assertions=overview_data.get("assertions", []),
        period=AuditPeriod(
            effective_date=period_data.get("effective_date"),
            start=period_data.get("start"),
            end=period_data.get("end"),
            term_months=period_data.get("term_months"),
        ) if period_data else None,
        match_targets=overview_data.get("match_targets", []),
    )

    # Parties
    parties = [
        Party(
            role=p.get("role", "unknown"),
            name=p.get("name", ""),
            normalized=p.get("normalized", "").upper(),
            provenance=_parse_provenance(p.get("provenance")),
        )
        for p in (data.get("parties") or [])
        if p.get("name")
    ]

    # Amounts
    amounts = [
        Amount(
            type=a.get("type", "amount"),
            value=float(a.get("value", 0)),
            currency=a.get("currency", "USD"),
            provenance=_parse_provenance(a.get("provenance")),
        )
        for a in (data.get("amounts") or [])
        if a.get("value") is not None
    ]

    # Dates
    dates = [
        DateItem(
            type=d.get("type", "date"),
            value=str(d.get("value", "")),
            provenance=_parse_provenance(d.get("provenance")),
        )
        for d in (data.get("dates") or [])
        if d.get("value")
    ]

    # Identifiers
    identifiers = [
        Identifier(
            type=i.get("type", "id"),
            value=str(i.get("value", "")),
            provenance=_parse_provenance(i.get("provenance")),
        )
        for i in (data.get("identifiers") or [])
        if i.get("value")
    ]

    # Assets
    assets = [
        AssetItem(
            type=a.get("type", "asset"),
            description=a.get("description", ""),
            value=float(a["value"]) if a.get("value") is not None else None,
            provenance=_parse_provenance(a.get("provenance")),
        )
        for a in (data.get("assets") or [])
        if a.get("description")
    ]

    # Facts
    facts = [
        Fact(
            label=f.get("label", "fact"),
            value=f.get("value"),
            provenance=_parse_provenance(f.get("provenance")),
        )
        for f in (data.get("facts") or [])
        if f.get("label") and f.get("value") is not None
    ]

    # Claims
    claims = [
        Claim(
            statement=c.get("statement", ""),
            audit_area=c.get("audit_area", "other"),
            basis_fact_labels=c.get("basis_fact_labels", []),
            provenance=_parse_provenance(c.get("provenance")),
        )
        for c in (data.get("claims") or [])
        if c.get("statement")
    ]

    # Flags
    flags = [
        Flag(
            type=f.get("type", "flag"),
            description=f.get("description", ""),
            severity=f.get("severity", "info"),
        )
        for f in (data.get("flags") or [])
        if f.get("description")
    ]

    # Link keys
    lk_data = data.get("link_keys") or {}
    link_keys = LinkKeys(
        party_names=[str(n).upper() for n in lk_data.get("party_names", [])],
        document_numbers=[str(n) for n in lk_data.get("document_numbers", [])],
        agreement_numbers=[str(n) for n in lk_data.get("agreement_numbers", [])],
        invoice_numbers=[str(n) for n in lk_data.get("invoice_numbers", [])],
        asset_descriptions=[str(n).upper() for n in lk_data.get("asset_descriptions", [])],
        recurring_amounts=[float(a) for a in lk_data.get("recurring_amounts", []) if a],
        key_dates=[str(d) for d in lk_data.get("key_dates", [])],
        other_ids=[str(i) for i in lk_data.get("other_ids", [])],
    )

    return AuditEvidence(
        source_file=source_file,
        family=family,
        subtype=data.get("subtype"),
        title=data.get("title"),
        audit_overview=overview,
        parties=parties,
        amounts=amounts,
        dates=dates,
        identifiers=identifiers,
        assets=assets,
        facts=facts,
        claims=claims,
        flags=flags,
        link_keys=link_keys,
        document_specific=data.get("document_specific") or {},
        raw_text=raw_text,
        tables=tables,
        extraction_meta=extraction_meta,
    )


def extract_canonical(
    raw_text: str,
    tables: list,
    source_file: str,
    page_count: int,
    provider,
    extraction_meta: ExtractionMeta,
) -> Optional[AuditEvidence]:
    """Run the single AI canonical extraction pass."""

    if not raw_text or not raw_text.strip():
        logger.warning(f"No text to extract from {source_file}")
        return AuditEvidence(
            source_file=source_file,
            extraction_meta=extraction_meta,
            flags=[Flag(type="no_text", description="No text extracted from document", severity="critical")]
        )

    table_section = build_table_section(tables)
    user_prompt = CANONICAL_USER_PROMPT.format(
        filename=source_file,
        pages=page_count,
        text=raw_text[:6000],  # ~6k chars fits well in context
        table_section=table_section,
    )

    try:
        raw_response = provider._call(
            system=CANONICAL_SYSTEM_PROMPT,
            user=user_prompt,
            max_tokens=3000,
        )

        # Clean response
        clean = raw_response.strip()
        if "```json" in clean:
            clean = clean.split("```json")[1].split("```")[0].strip()
        elif "```" in clean:
            clean = clean.split("```")[1].split("```")[0].strip()

        data = json.loads(clean)
        return _parse_canonical_json(data, source_file, raw_text, tables, extraction_meta)

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in canonical extraction: {e}")
        return AuditEvidence(
            source_file=source_file,
            raw_text=raw_text,
            tables=tables,
            extraction_meta=extraction_meta,
            flags=[Flag(type="parse_error", description=f"AI response could not be parsed: {e}", severity="warning")]
        )
    except Exception as e:
        logger.error(f"Canonical extraction failed: {e}")
        return AuditEvidence(
            source_file=source_file,
            raw_text=raw_text,
            tables=tables,
            extraction_meta=extraction_meta,
            flags=[Flag(type="extraction_error", description=str(e), severity="critical")]
        )
