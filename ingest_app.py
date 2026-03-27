"""
audit_ingestion_v03/ingest_app.py
Audit Ingestion Pipeline v03 — Streamlit UI
Canonical audit evidence view.
"""
import streamlit as st
import json
import sys
import os
import tempfile
import shutil
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

st.set_page_config(
    page_title="Audit Ingestion v03",
    page_icon="📋",
    layout="wide",
)

st.markdown("""
<style>
.family-badge {
    background: #1A335C; color: white; padding: 3px 10px;
    border-radius: 4px; font-size: 0.8rem; font-weight: 600;
}
.audit-area {
    background: #e8f4fd; color: #1A335C; padding: 2px 8px;
    border-radius: 3px; font-size: 0.78rem; margin: 2px;
    display: inline-block;
}
.claim-box {
    background: #f0fdf4; border-left: 3px solid #1a7a5e;
    padding: 8px 12px; margin: 4px 0; border-radius: 0 4px 4px 0;
}
.flag-info     { background: #eff6ff; border-left: 3px solid #3b82f6; padding: 8px 12px; margin: 4px 0; border-radius: 0 4px 4px 0; }
.flag-warning  { background: #fffbeb; border-left: 3px solid #f59e0b; padding: 8px 12px; margin: 4px 0; border-radius: 0 4px 4px 0; }
.flag-critical { background: #fef2f2; border-left: 3px solid #dc2626; padding: 8px 12px; margin: 4px 0; border-radius: 0 4px 4px 0; }
.match-target {
    background: #f3e8ff; color: #6b21a8; padding: 2px 8px;
    border-radius: 3px; font-size: 0.78rem; margin: 2px;
    display: inline-block;
}
.section-title {
    font-size: 1rem; font-weight: 700; color: #1A335C;
    border-bottom: 2px solid #1A335C; padding-bottom: 4px; margin-bottom: 10px;
}
.confidence-high   { color: #1a7a5e; font-weight: 600; }
.confidence-medium { color: #b45309; font-weight: 600; }
.confidence-low    { color: #dc2626; font-weight: 600; }
</style>
""", unsafe_allow_html=True)


def conf_class(c):
    if c >= 0.80: return "confidence-high"
    if c >= 0.50: return "confidence-medium"
    return "confidence-low"


def run_pipeline(uploaded_files, provider_name, api_key):
    from audit_ingestion.router import ingest_one
    results = []
    progress = st.progress(0)
    status_text = st.empty()

    tmpdir = tempfile.mkdtemp(prefix="audit_v03_")
    tmp_work = Path(tmpdir) / ".tmp"
    tmp_work.mkdir(exist_ok=True)

    try:
        for i, uf in enumerate(uploaded_files):
            status_text.text(f"Processing {uf.name}... ({i+1}/{len(uploaded_files)})")
            progress.progress((i + 1) / len(uploaded_files))

            tmp_path = Path(tmpdir) / uf.name
            with open(tmp_path, "wb") as f:
                f.write(uf.read())

            try:
                result = ingest_one(
                    str(tmp_path),
                    provider_name=provider_name,
                    api_key=api_key or None,
                )
                results.append(result)
            except Exception as e:
                from audit_ingestion.models import IngestionResult, AuditEvidence, Flag
                results.append(IngestionResult(
                    status="failed",
                    errors=[str(e)],
                    evidence=AuditEvidence(
                        source_file=uf.name,
                        flags=[Flag(type="fatal_error", description=str(e), severity="critical")]
                    )
                ))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    progress.empty()
    status_text.empty()
    return results


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Settings")

    ai_mode = st.selectbox(
        "AI Mode",
        ["Keyword Only (no AI)", "OpenAI GPT-4o-mini", "OpenAI GPT-4o (best quality)"],
        index=1,
    )

    api_key = ""
    if "OpenAI" in ai_mode:
        st.markdown("**OpenAI API Key**")
        key_file = Path("openai_key.txt")
        default_key = key_file.read_text().strip() if key_file.exists() else ""
        api_key = st.text_input("API Key", value=default_key, type="password",
                                placeholder="sk-...", label_visibility="collapsed")
        if api_key:
            st.success("✅ Key ready")
        else:
            st.warning("⚠️ Enter API key to enable AI extraction")

    st.markdown("---")
    st.markdown("### 📋 About")
    st.markdown("""
**Audit Ingestion Pipeline v03**

Canonical audit evidence extraction.
Every document produces the same structured output:
- Document identity & family
- Audit overview & areas
- Parties, amounts, dates
- Atomic facts with provenance
- Auditor-readable claims
- Link keys for matching
    """)
    st.caption("v03 — canonical architecture")


# ── Main ──────────────────────────────────────────────────────────────────────
st.markdown("# 📋 Audit Ingestion Pipeline")
st.markdown("Upload any audit document — lease, invoice, grant, bank statement, minutes, or anything else.")
st.markdown("---")

uploaded_files = st.file_uploader(
    "Drop documents here",
    accept_multiple_files=True,
    type=["pdf", "csv", "xlsx", "xls", "txt", "docx", "png", "jpg", "jpeg"],
)

c1, c2 = st.columns([2, 1])
with c1:
    run_btn = st.button("▶ Run Pipeline", type="primary", disabled=not uploaded_files)
with c2:
    if st.button("🗑 Clear"):
        st.session_state.pop("v03_results", None)
        st.rerun()

if run_btn and uploaded_files:
    if "OpenAI GPT-4o (best)" in ai_mode:
        provider_name = "openai"
        model_override = "gpt-4o"
    elif "OpenAI" in ai_mode:
        provider_name = "openai"
        model_override = "gpt-4o-mini"
    else:
        provider_name = "stub"
        model_override = None

    with st.spinner("Running canonical audit extraction..."):
        results = run_pipeline(uploaded_files, provider_name, api_key)

    st.session_state["v03_results"] = [r.model_dump() for r in results]
    st.rerun()


# ── Results ───────────────────────────────────────────────────────────────────
if "v03_results" in st.session_state:
    raw_results = st.session_state["v03_results"]

    # Summary metrics
    total   = len(raw_results)
    success = sum(1 for r in raw_results if r["status"] == "success")
    partial = sum(1 for r in raw_results if r["status"] == "partial")
    failed  = sum(1 for r in raw_results if r["status"] == "failed")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total", total)
    m2.metric("✅ Success", success)
    m3.metric("⚠️ Partial", partial)
    m4.metric("❌ Failed", failed)

    st.markdown("---")

    # Summary table
    st.markdown('<div class="section-title">Document Summary</div>', unsafe_allow_html=True)
    rows = []
    for r in raw_results:
        ev = r.get("evidence") or {}
        meta = ev.get("extraction_meta") or {}
        overview = ev.get("audit_overview") or {}
        rows.append({
            "File": ev.get("source_file", "?"),
            "Status": r["status"].upper(),
            "Family": ev.get("family", "?"),
            "Subtype": ev.get("subtype") or "—",
            "Extractor": meta.get("extractor", "?"),
            "Text Chars": f"{meta.get('text_chars', 0):,}",
            "Confidence": f"{meta.get('overall_confidence', 0):.2f}",
            "Audit Areas": ", ".join(overview.get("audit_areas", [])) or "—",
            "Needs Review": "⚠️ Yes" if meta.get("needs_human_review") else "✅ No",
        })

    df = pd.DataFrame(rows)

    def highlight(row):
        status = row["Status"]
        if status == "SUCCESS":
            return ["background-color: #f0fdf4"] * len(row)
        elif status == "PARTIAL":
            return ["background-color: #fffbeb"] * len(row)
        return ["background-color: #fef2f2"] * len(row)

    st.dataframe(df.style.apply(highlight, axis=1), use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False)
    st.download_button("⬇️ Export Summary CSV", data=csv,
                       file_name="audit_evidence_summary.csv", mime="text/csv")

    st.markdown("---")

    # Detail view
    st.markdown('<div class="section-title">Document Detail</div>', unsafe_allow_html=True)
    file_names = [r.get("evidence", {}).get("source_file", f"File {i}") for i, r in enumerate(raw_results)]
    selected = st.selectbox("Select document", file_names)

    r = next((x for x in raw_results if x.get("evidence", {}).get("source_file") == selected), None)
    if not r:
        st.stop()

    ev = r.get("evidence") or {}
    meta = ev.get("extraction_meta") or {}
    overview = ev.get("audit_overview") or {}

    # Header row
    col_a, col_b, col_c, col_d = st.columns(4)
    family = ev.get("family", "other").replace("_", " ").title()
    subtype = ev.get("subtype", "")
    conf = meta.get("overall_confidence", 0)

    col_a.markdown(f"**Family:** `{family}`")
    col_b.markdown(f"**Subtype:** `{subtype or '—'}`")
    col_c.markdown(f"**Confidence:** <span class='{conf_class(conf)}'>{conf:.2f}</span>", unsafe_allow_html=True)
    col_d.markdown(f"**Extractor:** `{meta.get('extractor', '?')}`")

    st.markdown(f"**Engine:** `{' → '.join(r.get('engine_chain', []))}`")
    st.markdown(f"**Pages:** {meta.get('pages_processed', '?')} &nbsp;|&nbsp; **Text:** {meta.get('text_chars', 0):,} chars")

    # ── Section 1: Auditor Snapshot ───────────────────────────────────────────
    if overview.get("summary"):
        st.markdown("---")
        st.markdown('<div class="section-title">🔍 Auditor Snapshot</div>', unsafe_allow_html=True)
        st.markdown(overview["summary"])

        if overview.get("audit_areas"):
            areas_html = " ".join(f'<span class="audit-area">{a}</span>' for a in overview["audit_areas"])
            st.markdown(f"**Audit Areas:** {areas_html}", unsafe_allow_html=True)

        if overview.get("assertions"):
            st.markdown(f"**Assertions:** {', '.join(overview['assertions'])}")

        if overview.get("match_targets"):
            targets_html = " ".join(f'<span class="match-target">{t}</span>' for t in overview["match_targets"])
            st.markdown(f"**Match Targets:** {targets_html}", unsafe_allow_html=True)

    # ── Section 2: Key Audit Facts ────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div class="section-title">📊 Key Audit Facts</div>', unsafe_allow_html=True)

    fact_col1, fact_col2 = st.columns(2)

    with fact_col1:
        # Parties
        parties = ev.get("parties") or []
        if parties:
            st.markdown("**Parties**")
            party_rows = [{"Role": p["role"], "Name": p["name"],
                           "Confidence": f"{p.get('provenance', {}).get('confidence', 0):.2f}"}
                          for p in parties]
            st.dataframe(pd.DataFrame(party_rows), use_container_width=True, hide_index=True)

        # Amounts
        amounts = ev.get("amounts") or []
        if amounts:
            st.markdown("**Amounts**")
            amt_rows = [{"Type": a["type"], "Amount": f"${a['value']:,.2f}",
                         "Quote": a.get("provenance", {}).get("quote", ""),
                         "Conf": f"{a.get('provenance', {}).get('confidence', 0):.2f}"}
                        for a in amounts]
            st.dataframe(pd.DataFrame(amt_rows), use_container_width=True, hide_index=True)

    with fact_col2:
        # Dates
        dates = ev.get("dates") or []
        if dates:
            st.markdown("**Dates**")
            date_rows = [{"Type": d["type"], "Value": d["value"],
                          "Quote": d.get("provenance", {}).get("quote", "")}
                         for d in dates]
            st.dataframe(pd.DataFrame(date_rows), use_container_width=True, hide_index=True)

        # Identifiers
        identifiers = ev.get("identifiers") or []
        if identifiers:
            st.markdown("**Identifiers**")
            id_rows = [{"Type": i["type"], "Value": i["value"],
                        "Quote": i.get("provenance", {}).get("quote", "")}
                       for i in identifiers]
            st.dataframe(pd.DataFrame(id_rows), use_container_width=True, hide_index=True)

    # Assets
    assets = ev.get("assets") or []
    if assets:
        st.markdown("**Assets / Items**")
        asset_rows = [{"Type": a["type"], "Description": a["description"],
                       "Value": f"${a['value']:,.2f}" if a.get("value") else "—"}
                      for a in assets]
        st.dataframe(pd.DataFrame(asset_rows), use_container_width=True, hide_index=True)

    # Facts
    facts = ev.get("facts") or []
    if facts:
        with st.expander(f"📌 Atomic Facts ({len(facts)})", expanded=False):
            fact_rows = [{"Label": f["label"], "Value": str(f["value"]),
                          "Page": f.get("provenance", {}).get("page", ""),
                          "Quote": f.get("provenance", {}).get("quote", ""),
                          "Conf": f"{f.get('provenance', {}).get('confidence', 0):.2f}"}
                         for f in facts]
            st.dataframe(pd.DataFrame(fact_rows), use_container_width=True, hide_index=True)

    # ── Section 3: Claims ─────────────────────────────────────────────────────
    claims = ev.get("claims") or []
    if claims:
        st.markdown("---")
        st.markdown('<div class="section-title">📝 Auditor Claims</div>', unsafe_allow_html=True)
        for c in claims:
            prov = c.get("provenance") or {}
            quote = f' *"{prov.get("quote")}"*' if prov.get("quote") else ""
            conf = prov.get("confidence", 0)
            st.markdown(
                f'<div class="claim-box">'
                f'<strong>{c["statement"]}</strong><br>'
                f'<small>Area: {c.get("audit_area", "?")} &nbsp;|&nbsp; '
                f'Conf: <span class="{conf_class(conf)}">{conf:.2f}</span>'
                f'{quote}</small>'
                f'</div>',
                unsafe_allow_html=True
            )

    # ── Section 4: Flags ──────────────────────────────────────────────────────
    flags = ev.get("flags") or []
    if flags:
        st.markdown("---")
        st.markdown('<div class="section-title">🚩 Flags & Exceptions</div>', unsafe_allow_html=True)
        for flag in flags:
            severity = flag.get("severity", "info")
            st.markdown(
                f'<div class="flag-{severity}">'
                f'<strong>[{severity.upper()}] {flag.get("type", "flag")}</strong><br>'
                f'{flag.get("description", "")}'
                f'</div>',
                unsafe_allow_html=True
            )

    # ── Section 5: Link Keys ──────────────────────────────────────────────────
    link_keys = ev.get("link_keys") or {}
    has_links = any(link_keys.get(k) for k in link_keys)
    if has_links:
        st.markdown("---")
        with st.expander("🔗 Link Keys (Cross-Document Matching)", expanded=False):
            st.caption("These normalized keys will be used to match this document against GL, AP, and other evidence.")
            lk_rows = []
            for field, values in link_keys.items():
                if values:
                    lk_rows.append({"Key Type": field.replace("_", " ").title(),
                                    "Values": ", ".join(str(v) for v in values)})
            if lk_rows:
                st.dataframe(pd.DataFrame(lk_rows), use_container_width=True, hide_index=True)

    # ── Section 6: Document Specific ─────────────────────────────────────────
    doc_specific = ev.get("document_specific") or {}
    if doc_specific:
        with st.expander("📄 Document-Specific Fields", expanded=False):
            st.json(doc_specific)

    # ── Section 7: Tables ─────────────────────────────────────────────────────
    tables = ev.get("tables") or []
    if tables:
        with st.expander(f"📊 Extracted Tables ({len(tables)})", expanded=False):
            for i, tbl in enumerate(tables):
                st.markdown(f"**Table {i+1}** (page {tbl.get('page', '?')})")
                rows = tbl.get("rows") or []
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Section 8: Raw Text ───────────────────────────────────────────────────
    raw_text = ev.get("raw_text") or ""
    if raw_text:
        with st.expander("📝 Raw Text", expanded=False):
            st.text(raw_text[:3000])

    # ── Section 9: Full JSON ──────────────────────────────────────────────────
    with st.expander("🔧 Full Canonical JSON", expanded=False):
        st.json(ev)
