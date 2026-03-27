"""
audit_ingestion_v03/audit_ingestion/models.py
Canonical audit evidence schema — Pydantic models.

Every document produces one AuditEvidence object.
No type-specific schemas. No fragmented field dicts.
Facts + Claims + LinkKeys drive matching.
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Any, Literal
from enum import Enum


# ── Document Family ───────────────────────────────────────────────────────────

class DocumentFamily(str, Enum):
    CONTRACT       = "contract_agreement"
    INVOICE        = "invoice_receipt"
    PAYMENT        = "payment_proof"
    BANK           = "bank_cash_activity"
    PAYROLL        = "payroll_support"
    ACCOUNTING     = "accounting_report"
    GOVERNANCE     = "governance_approval"
    GRANT          = "grant_donor_funding"
    TAX_REG        = "tax_regulatory"
    CORRESPONDENCE = "correspondence"
    SCHEDULE       = "schedule_listing"
    OTHER          = "other"


# ── Provenance ────────────────────────────────────────────────────────────────

class Provenance(BaseModel):
    """Source evidence for any extracted item."""
    page:       Optional[int]   = None
    quote:      Optional[str]   = None   # Short verbatim excerpt from document
    confidence: float           = 0.0    # 0.0–1.0


# ── Core Evidence Objects ─────────────────────────────────────────────────────

class Party(BaseModel):
    role:       str            # lessor, lessee, vendor, client, grantor, grantee, payer, payee, etc.
    name:       str
    normalized: str            # UPPERCASE normalized for matching
    provenance: Optional[Provenance] = None


class Amount(BaseModel):
    type:       str            # monthly_fixed_charge, total_award, invoice_total, payment_amount, etc.
    value:      float
    currency:   str = "USD"
    provenance: Optional[Provenance] = None


class DateItem(BaseModel):
    type:       str            # effective_date, schedule_date, invoice_date, period_start, period_end, etc.
    value:      str            # YYYY-MM-DD
    provenance: Optional[Provenance] = None


class Identifier(BaseModel):
    type:       str            # invoice_number, schedule_number, grant_number, check_number, etc.
    value:      str
    provenance: Optional[Provenance] = None


class AssetItem(BaseModel):
    type:        str           # vehicle, equipment, property, program, service, etc.
    description: str
    value:       Optional[float] = None
    provenance:  Optional[Provenance] = None


class Fact(BaseModel):
    """Atomic extracted fact — drives matching engine."""
    label:      str            # snake_case label: term_months, mileage_rate, interest_rate, etc.
    value:      Any            # str, float, int, bool
    provenance: Optional[Provenance] = None


class Claim(BaseModel):
    """Auditor-readable interpretation built from facts."""
    statement:        str       # Plain English: "72-month lease at $2,273/month"
    audit_area:       str       # leases, expenses, revenue, payroll, etc.
    basis_fact_labels: list[str] = Field(default_factory=list)
    provenance:       Optional[Provenance] = None


class Flag(BaseModel):
    """Audit exception, risk, or attention item."""
    type:        str            # variable_cost, missing_signature, unusual_amount, etc.
    description: str
    severity:    Literal["info", "warning", "critical"] = "info"


# ── Audit Overview ────────────────────────────────────────────────────────────

class AuditPeriod(BaseModel):
    effective_date: Optional[str] = None   # YYYY-MM-DD
    start:          Optional[str] = None   # YYYY-MM-DD
    end:            Optional[str] = None   # YYYY-MM-DD
    term_months:    Optional[int] = None


class AuditOverview(BaseModel):
    summary:       str                          # One paragraph — what is this and why does it matter
    audit_areas:   list[str] = Field(default_factory=list)   # leases, expenses, revenue, payroll, etc.
    assertions:    list[str] = Field(default_factory=list)   # existence, accuracy, completeness, etc.
    period:        Optional[AuditPeriod] = None
    match_targets: list[str] = Field(default_factory=list)   # lease_expense_gl, ap_recurring, etc.


# ── Link Keys ─────────────────────────────────────────────────────────────────

class LinkKeys(BaseModel):
    """Normalized keys for cross-document matching."""
    party_names:      list[str] = Field(default_factory=list)   # UPPERCASE normalized
    document_numbers: list[str] = Field(default_factory=list)
    agreement_numbers:list[str] = Field(default_factory=list)
    invoice_numbers:  list[str] = Field(default_factory=list)
    asset_descriptions:list[str] = Field(default_factory=list)
    recurring_amounts: list[float] = Field(default_factory=list)
    key_dates:        list[str] = Field(default_factory=list)    # YYYY-MM-DD
    other_ids:        list[str] = Field(default_factory=list)


# ── Extraction Metadata ───────────────────────────────────────────────────────

class ExtractionMeta(BaseModel):
    extractor:          str     # pdfplumber, ocr, vision, direct
    pages_processed:    int = 0
    text_chars:         int = 0
    overall_confidence: float = 0.0
    needs_human_review: bool = True
    warnings:           list[str] = Field(default_factory=list)
    errors:             list[str] = Field(default_factory=list)


# ── Canonical Audit Evidence — The Core Object ────────────────────────────────

class AuditEvidence(BaseModel):
    """
    One canonical object per document. Always the same shape.
    Works for leases, invoices, grants, payroll, bank statements,
    board minutes, or any other audit document.
    """
    # Identity
    source_file:    str
    family:         DocumentFamily = DocumentFamily.OTHER
    subtype:        Optional[str] = None    # vehicle_lease, grant_agreement, vendor_invoice, etc.
    title:          Optional[str] = None    # Document title if identifiable

    # Auditor-facing
    audit_overview: Optional[AuditOverview] = None

    # Universal fact buckets
    parties:        list[Party]     = Field(default_factory=list)
    amounts:        list[Amount]    = Field(default_factory=list)
    dates:          list[DateItem]  = Field(default_factory=list)
    identifiers:    list[Identifier]= Field(default_factory=list)
    assets:         list[AssetItem] = Field(default_factory=list)
    facts:          list[Fact]      = Field(default_factory=list)
    claims:         list[Claim]     = Field(default_factory=list)
    flags:          list[Flag]      = Field(default_factory=list)

    # Matching engine
    link_keys:      LinkKeys        = Field(default_factory=LinkKeys)

    # Document-specific extras (optional, never primary)
    document_specific: dict[str, Any] = Field(default_factory=dict)

    # Raw content
    raw_text:       Optional[str]   = None
    tables:         list[dict]      = Field(default_factory=list)

    # Meta
    extraction_meta: ExtractionMeta = Field(
        default_factory=lambda: ExtractionMeta(extractor="none")
    )


# ── Pipeline Result ───────────────────────────────────────────────────────────

class IngestionResult(BaseModel):
    """Top-level result returned by the v03 router."""
    evidence:   Optional[AuditEvidence] = None
    status:     Literal["success", "partial", "failed"] = "partial"
    errors:     list[str] = Field(default_factory=list)
    engine_chain: list[str] = Field(default_factory=list)
