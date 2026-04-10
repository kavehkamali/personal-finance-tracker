from __future__ import annotations

import re
from collections import Counter
from typing import Any

import pandas as pd

from personal_finance.categories import normalize_text


def _norm_desc_key(description: str) -> str:
    t = normalize_text(description).lower()
    t = re.sub(r"[^a-z0-9]+", "", t)
    return t[:56] if t else ""


def _row_fingerprint(row: pd.Series) -> tuple[str, float, str]:
    ts = row["transaction_date"]
    if pd.isna(ts):
        d = ""
    else:
        d = pd.Timestamp(ts).strftime("%Y-%m-%d")
    amt = round(float(row["amount"]), 2)
    dk = _norm_desc_key(str(row.get("description", "") or ""))
    return (d, amt, dk)


def merge_extraction_dataframes(
    sources: list[tuple[str, pd.DataFrame]],
    ingested_file: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Combine rows from native PDF + multiple OCR parses by consensus on
    (transaction_date, amount, normalized description). Improves accuracy when
    sources agree; surfaces disagreement via flags.
    """
    n_sources = len(sources)
    if n_sources == 0:
        return pd.DataFrame(), {"file": ingested_file, "sources": [], "rows_out": 0}

    if n_sources == 1:
        label, df = sources[0]
        out = df.copy()
        out["extraction_confidence"] = 1.0
        out["extraction_sources"] = label
        out["extraction_disagreement"] = False
        report = {
            "file": ingested_file,
            "sources": [label],
            "rows_out": int(len(out)),
            "avg_confidence": 1.0,
            "low_confidence_rows": 0,
            "disagreement_rows": 0,
            "merged": False,
        }
        return out, report

    rows: list[dict[str, Any]] = []
    for label, df in sources:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            rows.append({"_label": label, "_row": row})

    if not rows:
        return pd.DataFrame(), {
            "file": ingested_file,
            "sources": [s[0] for s in sources],
            "rows_out": 0,
            "avg_confidence": 0.0,
            "low_confidence_rows": 0,
            "disagreement_rows": 0,
            "merged": True,
        }

    clusters: dict[tuple[str, float, str], list[dict[str, Any]]] = {}
    for item in rows:
        r = item["_row"]
        fp = _row_fingerprint(r)
        clusters.setdefault(fp, []).append(item)

    merged_records: list[dict[str, Any]] = []
    disagreement_count = 0
    confidences: list[float] = []
    denom = float(n_sources)

    def desc_norm_key(g: dict[str, Any]) -> str:
        return normalize_text(str(g["_row"].get("description", "") or "")).lower()

    for _fp, group in clusters.items():
        labels_in_cluster = sorted({g["_label"] for g in group})
        votes = len(labels_in_cluster)
        norm_keys = [desc_norm_key(g) for g in group]
        had_disagreement = len(set(norm_keys)) > 1
        if had_disagreement:
            disagreement_count += 1

        key_counts = Counter(norm_keys)
        top_key, _top_ct = key_counts.most_common(1)[0]
        candidates = [g for g in group if desc_norm_key(g) == top_key]
        winner = max(candidates, key=lambda g: len(str(g["_row"].get("description", "") or "")))

        out = winner["_row"].to_dict()
        ref_vals = [str(g["_row"].get("reference", "") or "").strip() for g in group]
        ref_vals = [x for x in ref_vals if x]
        if ref_vals:
            out["reference"] = Counter(ref_vals).most_common(1)[0][0]

        confidence = min(1.0, votes / denom)
        confidences.append(confidence)
        out["extraction_confidence"] = round(confidence, 3)
        out["extraction_sources"] = "+".join(labels_in_cluster)
        out["extraction_disagreement"] = had_disagreement

        merged_records.append(out)

    out_df = pd.DataFrame.from_records(merged_records)
    if not out_df.empty and "transaction_date" in out_df.columns:
        out_df["transaction_date"] = pd.to_datetime(out_df["transaction_date"], errors="coerce")
    if not out_df.empty and "posting_date" in out_df.columns:
        out_df["posting_date"] = pd.to_datetime(out_df["posting_date"], errors="coerce")

    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    low_thr = max(0.5, (n_sources - 1) / denom) if n_sources > 1 else 1.0
    low_conf = sum(1 for c in confidences if c < low_thr - 1e-6)

    report = {
        "file": ingested_file,
        "sources": [s[0] for s in sources],
        "rows_out": int(len(out_df)),
        "avg_confidence": round(avg_conf, 3),
        "low_confidence_rows": int(low_conf),
        "disagreement_rows": int(disagreement_count),
        "merged": True,
    }
    return out_df, report


def summarize_extraction_reports(
    reports: list[dict[str, Any]],
    *,
    backends_configured: list[str],
    mode: str,
) -> dict[str, Any]:
    if not reports:
        return {
            "mode": mode,
            "backends_configured": backends_configured,
            "files": [],
            "avg_confidence_global": None,
            "total_low_confidence": 0,
            "total_disagreement_rows": 0,
            "files_with_merge": 0,
        }
    merged_files = [r for r in reports if r.get("merged")]
    all_conf = [r["avg_confidence"] for r in reports if r.get("avg_confidence") is not None]
    global_avg = round(sum(all_conf) / len(all_conf), 3) if all_conf else None
    return {
        "mode": mode,
        "backends_configured": backends_configured,
        "files": reports,
        "avg_confidence_global": global_avg,
        "total_low_confidence": int(sum(r.get("low_confidence_rows", 0) for r in reports)),
        "total_disagreement_rows": int(sum(r.get("disagreement_rows", 0) for r in reports)),
        "files_with_merge": len(merged_files),
    }
