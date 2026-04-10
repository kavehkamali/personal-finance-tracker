from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, cast

import pandas as pd

from personal_finance.categories import classify_transaction, infer_flow_type, load_category_rules, normalize_merchant, normalize_text
from personal_finance.transaction_overrides import apply_transaction_overrides, transaction_key_series
from personal_finance.merchant_aliases import invalidate_merchant_aliases_cache
from personal_finance.config import (
    DATA_DIR,
    INPUT_STATEMENTS_DIR,
    OCR_ENSEMBLE_MODE,
    OCR_OUTPUT_DIR,
    PIPELINE_META_JSON,
    PROCESSED_DIR,
    SETTINGS_DIR,
    SUPPORTED_UPLOAD_SUFFIXES,
    TRANSACTIONS_CSV,
    UPLOAD_DIR,
    ocr_ensemble_backends_list,
)
from personal_finance.extraction_merge import merge_extraction_dataframes, summarize_extraction_reports
from personal_finance.ocr import MINERU_AVAILABLE, batch_ensure_pdf_markdown, ocr_pdf_to_markdown
from personal_finance.parsers.rbc import parse_rbc_markdown
from personal_finance.parsers.rbc_pdf import parse_rbc_pdf


def _migrate_legacy_cache_layout() -> None:
    """Move pre-2025 layout ``data/{uploads,ocr_output,processed}`` into ``.cache/`` once."""
    legacy_map = [
        (DATA_DIR / "uploads", UPLOAD_DIR),
        (DATA_DIR / "ocr_output", OCR_OUTPUT_DIR),
        (DATA_DIR / "processed", PROCESSED_DIR),
    ]
    for legacy, dest in legacy_map:
        if not legacy.is_dir() or legacy.resolve() == dest.resolve():
            continue
        if dest.exists() and any(p for p in dest.iterdir() if p.name != ".gitkeep"):
            continue
        dest.mkdir(parents=True, exist_ok=True)
        for item in legacy.iterdir():
            if item.name == ".gitkeep":
                continue
            target = dest / item.name
            if not target.exists():
                shutil.move(str(item), str(target))


def ensure_directories() -> None:
    _migrate_legacy_cache_layout()
    for path in (INPUT_STATEMENTS_DIR, UPLOAD_DIR, OCR_OUTPUT_DIR, TRANSACTIONS_CSV.parent, SETTINGS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def clear_regenerable_cache() -> dict[str, int]:
    """
    Remove everything under ``.cache/uploads``, ``.cache/ocr_output``, and ``.cache/processed``.

    Preserves ``.gitkeep`` placeholders, ``input_statements/``, and ``data/settings`` (rules, overrides).
    """
    ensure_directories()
    invalidate_merchant_aliases_cache()
    cleared = 0
    for root in (UPLOAD_DIR, OCR_OUTPUT_DIR, PROCESSED_DIR):
        if not root.is_dir():
            continue
        for child in list(root.iterdir()):
            if child.name == ".gitkeep":
                continue
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
                cleared += 1
            except OSError:
                continue
    return {"cleared_items": cleared}


def discover_statement_files() -> list[Path]:
    ensure_directories()
    discovered = [
        path
        for root in (INPUT_STATEMENTS_DIR, UPLOAD_DIR)
        for path in root.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_UPLOAD_SUFFIXES
    ]
    return sorted(discovered, key=lambda path: (path.parent.name, path.name))


def save_uploaded_files(files: Iterable[tuple[str, bytes]]) -> list[Path]:
    ensure_directories()
    saved_paths: list[Path] = []
    for original_name, payload in files:
        target = UPLOAD_DIR / Path(original_name).name
        base = target.stem
        suffix = target.suffix
        counter = 1
        while target.exists():
            target = UPLOAD_DIR / f"{base}-{counter}{suffix}"
            counter += 1
        target.write_bytes(payload)
        saved_paths.append(target)
    return saved_paths


def _cash_flow_amount(row: pd.Series) -> float:
    amount = float(row["amount"])
    if row["statement_type"] == "credit_card":
        return -amount
    return amount


def _expense_amount(row: pd.Series) -> float:
    if row["is_internal"]:
        return 0.0
    if row["flow_type"] in {"Income", "Refund", "Credit"}:
        return 0.0
    if row["flow_type"] == "Fees":
        return abs(float(row["amount"]))
    if row["statement_type"] == "credit_card":
        return max(float(row["amount"]), 0.0)
    return abs(float(row["amount"])) if float(row["amount"]) < 0 else 0.0


def _is_internal_candidate(description: str, flow_type: str) -> bool:
    text = normalize_text(description).lower()
    if flow_type == "Internal Transfer":
        return True
    # Phrase-based — avoid bare "payment" / "transfer" (matches retail and app copy).
    phrases = (
        "e-transfer",
        "etransfer",
        "interac",
        "bill payment",
        "credit card payment",
        "online banking payment",
        "pre-authorized payment",
        "preauthorized payment",
        "payment - thank",
        "paiement",
        "autopay",
        "automatic payment",
        "funds transfer",
        "account transfer",
        "online transfer",
        "wire transfer",
        "tfr ",
        " tfr",
        "withdrawal",
        "deposit",
        "savings",
        "chequing",
        "checking",
        "investment",
        "visa payment",
        "mc payment",
        "line of credit",
        "loc payment",
    )
    return any(p in text for p in phrases)


def _score_transfer_match(left: pd.Series, right: pd.Series) -> float:
    score = 100.0
    score -= abs((left["transaction_date"] - right["transaction_date"]).days) * 10
    if left["reference"] and left["reference"] == right["reference"]:
        score += 35
    if left["account_label"] == right["account_label"]:
        score -= 25
    if left["owner"] == right["owner"]:
        score += 5
    return score


def _match_internal_transfers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["match_id"] = ""
    df["internal_match_status"] = "None"
    candidates = df[df["is_internal_candidate"]].copy()
    positives = candidates[candidates["cash_flow_amount"] > 0].index.tolist()
    negatives = candidates[candidates["cash_flow_amount"] < 0].index.tolist()
    used_positive: set[int] = set()
    match_counter = 1

    for neg_idx in negatives:
        if neg_idx not in df.index:
            continue
        neg_row = df.loc[neg_idx]
        best_idx = None
        best_score = None
        for pos_idx in positives:
            if pos_idx in used_positive:
                continue
            pos_row = df.loc[pos_idx]
            if abs(abs(float(pos_row["cash_flow_amount"])) - abs(float(neg_row["cash_flow_amount"]))) > 0.01:
                continue
            if abs((pos_row["transaction_date"] - neg_row["transaction_date"]).days) > 7:
                continue
            score = _score_transfer_match(neg_row, pos_row)
            if best_score is None or score > best_score:
                best_idx = pos_idx
                best_score = score

        if best_idx is None:
            continue

        match_id = f"match-{match_counter:04d}"
        match_counter += 1
        used_positive.add(best_idx)
        df.loc[[neg_idx, best_idx], "match_id"] = match_id
        df.loc[[neg_idx, best_idx], "internal_match_status"] = "Matched"

    unmatched_mask = df["is_internal_candidate"] & (df["internal_match_status"] == "None")
    df.loc[unmatched_mask, "internal_match_status"] = "Unmatched"
    return df


def _amount_timing_hints(text: str) -> bool:
    t = normalize_text(text).lower()
    hints = (
        "payment",
        "transfer",
        "deposit",
        "withdrawal",
        "savings",
        "chequing",
        "checking",
        "investment",
        "visa",
        "mastercard",
        "mortgage",
        "loan",
        "credit bal",
        "pre-auth",
        "preauth",
    )
    return any(h in t for h in hints)


def _match_internal_transfers_second_pass(df: pd.DataFrame) -> pd.DataFrame:
    """Pair opposite cash-flow rows with matching amounts across different accounts (timing heuristic)."""
    if df.empty:
        return df

    df = df.copy()
    already = df["internal_match_status"] == "Matched"
    matched_idx = set(df.index[already])
    pool_idx = [i for i in df.index if i not in matched_idx and float(df.loc[i, "cash_flow_amount"]) != 0.0]
    positives = [i for i in pool_idx if float(df.loc[i, "cash_flow_amount"]) > 0]
    negatives = [i for i in pool_idx if float(df.loc[i, "cash_flow_amount"]) < 0]
    used_positive: set[int] = set()
    existing = 0
    for mid in df["match_id"].unique():
        if isinstance(mid, str) and mid.startswith("match-"):
            try:
                existing = max(existing, int(mid.split("-", 1)[1]))
            except (ValueError, IndexError):
                pass
    match_counter = existing + 1

    for neg_idx in negatives:
        if neg_idx in matched_idx:
            continue
        neg_row = df.loc[neg_idx]
        neg_amt = abs(float(neg_row["cash_flow_amount"]))
        if neg_amt < 5.0:
            continue
        best_idx = None
        best_score = None
        for pos_idx in positives:
            if pos_idx in used_positive or pos_idx in matched_idx:
                continue
            pos_row = df.loc[pos_idx]
            if abs(abs(float(pos_row["cash_flow_amount"])) - neg_amt) > 0.01:
                continue
            if abs((pos_row["transaction_date"] - neg_row["transaction_date"]).days) > 7:
                continue
            if pos_row["account_label"] == neg_row["account_label"]:
                continue
            if not (
                _amount_timing_hints(str(neg_row["description"]))
                or _amount_timing_hints(str(pos_row["description"]))
            ):
                continue
            score = _score_transfer_match(neg_row, pos_row)
            if best_score is None or score > best_score:
                best_idx = pos_idx
                best_score = score

        if best_idx is None:
            continue

        match_id = f"match-{match_counter:04d}"
        match_counter += 1
        used_positive.add(best_idx)
        matched_idx.update({neg_idx, best_idx})
        df.loc[[neg_idx, best_idx], "match_id"] = match_id
        df.loc[[neg_idx, best_idx], "internal_match_status"] = "Matched"
        df.loc[[neg_idx, best_idx], "is_internal_candidate"] = True
        for idx in (neg_idx, best_idx):
            if str(df.loc[idx, "matched_keyword"] or "").strip() == "":
                df.loc[idx, "matched_keyword"] = "amount+timing"

    return df


def _apply_internal_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Align category and flow for paired / keyword internal rows."""
    df = df.copy()
    paired = df["internal_match_status"] == "Matched"
    keyword_before_pairing = df["flow_type"] == "Internal Transfer"
    df.loc[paired, "flow_type"] = "Internal Transfer"
    df.loc[paired, "category"] = "Internal Transfer"
    df.loc[paired, "necessity"] = "Savings & Transfers"
    df.loc[paired, "beneficiary"] = "Shared"
    blank_kw = paired & (df["matched_keyword"].fillna("") == "")
    df.loc[blank_kw, "matched_keyword"] = "paired transfer"  # keyword-candidate pairs with no rule match

    df["internal_detection"] = "none"
    cand_unmatched = df["is_internal_candidate"] & (df["internal_match_status"] == "Unmatched")
    df.loc[cand_unmatched, "internal_detection"] = "unmatched_candidate"
    df.loc[keyword_before_pairing & ~paired, "internal_detection"] = "keyword"
    df.loc[paired, "internal_detection"] = "paired"
    if "category_source" in df.columns:
        df.loc[paired, "category_source"] = "internal_pair"
        df.loc[keyword_before_pairing & ~paired, "category_source"] = "internal_keyword"
    return df


def _dedupe_transactions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["transaction_date_key"] = df["transaction_date"].dt.strftime("%Y-%m-%d").fillna("")
    df["row_key"] = (
        df["statement_id"].fillna("")
        + "|"
        + df["account_last4"].fillna("")
        + "|"
        + df["transaction_date_key"]
        + "|"
        + df["description"].fillna("")
        + "|"
        + df["reference"].fillna("")
        + "|"
        + df["amount"].round(2).astype(str)
    )
    df = df.drop_duplicates(subset=["row_key"]).drop(columns=["transaction_date_key", "row_key"])
    return df


def _enrich_transactions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["description"] = df["description"].map(normalize_text)
    df["reference"] = df["reference"].fillna("").astype(str)
    df["owner"] = df["owner"].fillna("Household")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["merchant"] = df["description"].map(normalize_merchant)
    category_rules = load_category_rules()
    df["flow_type"] = df.apply(
        lambda row: infer_flow_type(row["description"], float(row["amount"]), str(row["statement_type"])),
        axis=1,
    )
    df["is_internal_candidate"] = df.apply(
        lambda row: _is_internal_candidate(row["description"], row["flow_type"]),
        axis=1,
    )
    df["cash_flow_amount"] = df.apply(_cash_flow_amount, axis=1)
    df["match_id"] = ""
    df["internal_match_status"] = "None"
    df["is_internal"] = df["flow_type"].eq("Internal Transfer") | df["internal_match_status"].eq("Matched")
    classifications = df.apply(
        lambda row: classify_transaction(
            row["description"],
            row["merchant"],
            row["owner"],
            row["flow_type"],
            category_rules,
        ),
        axis=1,
        result_type="expand",
    )
    df["category"] = classifications["category"]
    df["necessity"] = classifications["necessity"]
    df["beneficiary"] = classifications["beneficiary"]
    df["matched_keyword"] = classifications["matched_keyword"]
    df["tx_key"] = transaction_key_series(df)
    df["category_source"] = df["matched_keyword"].fillna("").astype(str).map(lambda s: "rule" if s.strip() else "auto")
    df["expense_amount"] = df.apply(_expense_amount, axis=1)
    df["month"] = df["transaction_date"].dt.to_period("M").astype(str)
    df["weekday"] = df["transaction_date"].dt.day_name()
    df["statement_count_hint"] = 1
    return df


def _write_outputs(
    df: pd.DataFrame,
    warnings: list[str],
    processed_files: list[str],
    extraction_ensemble: dict[str, object] | None = None,
) -> None:
    ensure_directories()
    df.to_csv(TRANSACTIONS_CSV, index=False)
    meta: dict[str, object] = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "warnings": warnings,
        "processed_files": processed_files,
    }
    if extraction_ensemble is not None:
        meta["extraction_ensemble"] = extraction_ensemble
    PIPELINE_META_JSON.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def rebuild_dataset(
    progress_callback: Callable[[dict[str, object]], None] | None = None,
    source_paths: list[Path] | None = None,
    ocr_backends: list[str] | None = None,
    ocr_ensemble_mode: str | None = None,
) -> dict[str, object]:
    ensure_directories()
    invalidate_merchant_aliases_cache()
    eff_backends: list[str] = list(ocr_backends) if ocr_backends is not None else ocr_ensemble_backends_list()
    eff_mode = (ocr_ensemble_mode or OCR_ENSEMBLE_MODE or "when_empty").strip().lower()

    def run_ocr_ensemble(native_empty: bool) -> bool:
        if not MINERU_AVAILABLE:
            return False
        if eff_mode == "never":
            return False
        if eff_mode == "always":
            return True
        return native_empty
    warnings: list[str] = []
    frames: list[pd.DataFrame] = []
    processed_files: list[str] = []
    extraction_reports: list[dict[str, object]] = []
    candidate_sources = source_paths or discover_statement_files()
    total_files = max(len(candidate_sources), 1)

    # Never decrease progress (avoids bar jumping backward when OCR phase used lower constants than "Reading").
    _progress_floor = 0.0

    def emit(
        stage: str,
        message: str,
        progress: float,
        current_file: str | None = None,
        *,
        ocr_backend: str | None = None,
        ocr_backend_index: int | None = None,
        ocr_backends_total: int | None = None,
    ) -> None:
        nonlocal _progress_floor
        if progress_callback is None:
            return
        p = max(float(progress), _progress_floor)
        _progress_floor = p
        progress_callback(
            {
                "stage": stage,
                "message": message,
                "progress": round(p, 4),
                "current_file": current_file,
                "processed_files": len(processed_files),
                "total_files": len(candidate_sources),
                "ocr_backend": ocr_backend,
                "ocr_backend_index": ocr_backend_index,
                "ocr_backends_total": ocr_backends_total,
            }
        )

    emit("discovering", "Scanning statement files", 0.02)

    # Monotonic timeline: ingest 0.05→0.24 · OCR 0.26→0.52 · merge PDFs 0.54→0.70 · post 0.72→1.0
    P_INGEST_LO, P_INGEST_HI = 0.05, 0.24
    P_OCR_LO, P_OCR_HI = 0.26, 0.52
    P_MERGE_LO, P_MERGE_HI = 0.54, 0.70
    P_DEDUPE, P_CATEGORIZE, P_MATCH, P_WRITE = 0.72, 0.78, 0.84, 0.92

    pdf_round: list[dict[str, object]] = []

    for index, source_path in enumerate(candidate_sources, start=1):
        try:
            p_ingest = P_INGEST_LO + (index / total_files) * (P_INGEST_HI - P_INGEST_LO)
            emit(
                "parsing",
                f"Reading {source_path.name}",
                p_ingest,
                source_path.name,
                ocr_backend=None,
                ocr_backend_index=None,
                ocr_backends_total=None,
            )
            if source_path.suffix.lower() == ".pdf":
                native_df = parse_rbc_pdf(source_path)
                native_empty = native_df is None or native_df.empty
                backends = list(eff_backends) if run_ocr_ensemble(native_empty) else []
                pdf_round.append(
                    {
                        "path": source_path,
                        "native_df": native_df,
                        "backends": backends,
                    }
                )
                continue

            emit(
                "parsing",
                f"Parsing markdown · {source_path.name}",
                p_ingest,
                source_path.name,
                ocr_backend=None,
                ocr_backend_index=None,
                ocr_backends_total=None,
            )
            frame = parse_rbc_markdown(source_path)
            if not frame.empty:
                extraction_reports.append(
                    {
                        "file": source_path.name,
                        "sources": ["markdown_upload"],
                        "rows_out": int(len(frame)),
                        "avg_confidence": 1.0,
                        "low_confidence_rows": 0,
                        "disagreement_rows": 0,
                        "merged": False,
                    }
                )
            if frame.empty:
                warnings.append(f"No transactions extracted from {source_path.name}")
                continue
            frame["ingested_file"] = source_path.name
            frames.append(frame)
            processed_files.append(source_path.name)
        except Exception as exc:  # pragma: no cover - user-facing warning path
            warnings.append(f"{source_path.name}: {exc}")

    n_pdf = len(pdf_round)
    need_ocr = sum(1 for x in pdf_round if cast(list[str], x["backends"]))
    with_native_rows = sum(
        1
        for x in pdf_round
        if (nd := cast(pd.DataFrame | None, x["native_df"])) is not None and not nd.empty
    )
    native_empty_no_ocr = sum(
        1
        for x in pdf_round
        if not cast(list[str], x["backends"])
        and (
            (nd := cast(pd.DataFrame | None, x["native_df"])) is None or nd.empty
        )
    )
    if n_pdf:
        if need_ocr:
            scan_msg = (
                f"PDF scan done — {with_native_rows} with usable embedded text; "
                f"{need_ocr} need MinerU OCR (slow step)."
            )
        elif native_empty_no_ocr:
            scan_msg = (
                f"PDF scan done — {native_empty_no_ocr} PDF(s) have no parsed rows from embedded text "
                f"and OCR is off or unavailable; {with_native_rows} had rows from text."
            )
        else:
            scan_msg = f"PDF scan done — all {n_pdf} PDF(s) parsed from embedded text; skipping MinerU."
        emit(
            "parsing",
            scan_msg,
            P_INGEST_HI + 0.01,
            None,
            ocr_backend=None,
            ocr_backend_index=None,
            ocr_backends_total=None,
        )

    ocr_jobs: list[tuple[int, str, list[Path]]] = []
    if pdf_round and MINERU_AVAILABLE:
        for bi, backend in enumerate(eff_backends, start=1):
            targets = [cast(Path, x["path"]) for x in pdf_round if backend in cast(list[str], x["backends"])]
            if targets:
                ocr_jobs.append((bi, backend, targets))

    n_ocr = len(ocr_jobs)
    ocr_span = max(P_OCR_HI - P_OCR_LO, 0.001)
    if ocr_jobs:
        for si, (bi, backend, targets) in enumerate(ocr_jobs, start=1):
            ocr_p0 = P_OCR_LO + (si - 0.35) / max(n_ocr, 1) * ocr_span
            emit(
                "ocr",
                (
                    f"MinerU OCR — {len(targets)} PDF(s) had no rows from embedded text — "
                    f"model {si}/{n_ocr}: {backend}"
                ),
                min(ocr_p0, P_OCR_HI),
                None,
                ocr_backend=backend,
                ocr_backend_index=si,
                ocr_backends_total=n_ocr,
            )
            batch_ensure_pdf_markdown(targets, OCR_OUTPUT_DIR, backend=backend)
            ocr_p1 = P_OCR_LO + (si / max(n_ocr, 1)) * ocr_span
            emit(
                "ocr",
                f"MinerU finished — model {si}/{n_ocr}: {backend}",
                ocr_p1,
                None,
                ocr_backend=backend,
                ocr_backend_index=si,
                ocr_backends_total=n_ocr,
            )
    elif need_ocr and not MINERU_AVAILABLE:
        warnings.append("PDFs need OCR but MinerU is not installed (`uv sync --extra ocr`).")

    n_merge = max(len(pdf_round), 1)
    merge_span = P_MERGE_HI - P_MERGE_LO
    for mi, item in enumerate(pdf_round, start=1):
        source_path = cast(Path, item["path"])
        native_df = cast(pd.DataFrame | None, item["native_df"])
        backends = cast(list[str], item["backends"])
        p_merge = P_MERGE_LO + (mi / n_merge) * merge_span
        try:
            parts: list[tuple[str, pd.DataFrame]] = []
            if native_df is not None and not native_df.empty:
                parts.append(("native_pdf", native_df))

            if backends:
                emit(
                    "parsing",
                    f"Merging extraction · {source_path.name}",
                    p_merge,
                    source_path.name,
                    ocr_backend=None,
                    ocr_backend_index=None,
                    ocr_backends_total=None,
                )
                for bi, backend in enumerate(backends, start=1):
                    try:
                        markdown_path = ocr_pdf_to_markdown(source_path, OCR_OUTPUT_DIR, backend=backend)
                        odf = parse_rbc_markdown(markdown_path)
                        if odf is not None and not odf.empty:
                            parts.append((f"ocr_{backend}", odf))
                    except Exception as ocr_exc:  # pragma: no cover - optional stack
                        warnings.append(f"{source_path.name} OCR [{backend}]: {ocr_exc}")
            else:
                if native_df is not None and not native_df.empty:
                    emit(
                        "parsing",
                        f"Using embedded PDF text · {source_path.name}",
                        p_merge,
                        source_path.name,
                        ocr_backend=None,
                        ocr_backend_index=None,
                        ocr_backends_total=None,
                    )

            if not parts:
                frame = pd.DataFrame()
            else:
                frame, file_report = merge_extraction_dataframes(parts, source_path.name)
                extraction_reports.append(file_report)

            if frame.empty:
                warnings.append(f"No transactions extracted from {source_path.name}")
                continue
            frame["ingested_file"] = source_path.name
            frames.append(frame)
            processed_files.append(source_path.name)
        except Exception as exc:  # pragma: no cover - user-facing warning path
            warnings.append(f"{source_path.name}: {exc}")

    if not frames:
        empty = pd.DataFrame(
            columns=[
                "statement_id",
                "source_file",
                "source_path",
                "statement_type",
                "account_name",
                "account_last4",
                "account_label",
                "owner",
                "role",
                "household",
                "transaction_date",
                "posting_date",
                "description",
                "reference",
                "amount",
                "currency",
                "statement_start",
                "statement_end",
                "notes",
                "ingested_file",
            ]
        )
        emit("writing", "Writing empty dataset", P_WRITE)
        ensemble_meta = summarize_extraction_reports(
            extraction_reports,
            backends_configured=eff_backends,
            mode=eff_mode,
        )
        _write_outputs(empty, warnings, processed_files, extraction_ensemble=ensemble_meta)
        emit("complete", "Finished processing", 1.0)
        return {"warnings": warnings, "processed_files": processed_files}

    combined = pd.concat(frames, ignore_index=True)
    emit("dedupe", "Deduplicating transactions", P_DEDUPE)
    combined = _dedupe_transactions(combined)
    emit("categorizing", "Applying category rules", P_CATEGORIZE)
    combined = _enrich_transactions(combined)
    emit("matching", "Reconciling internal transfers", P_MATCH)
    combined = _match_internal_transfers(combined)
    combined = _match_internal_transfers_second_pass(combined)
    combined = _apply_internal_labels(combined)
    combined["is_internal"] = combined["flow_type"].eq("Internal Transfer") | combined["internal_match_status"].eq("Matched")
    combined = apply_transaction_overrides(combined)
    combined["expense_amount"] = combined.apply(_expense_amount, axis=1)
    combined = combined.sort_values(["transaction_date", "account_label", "amount"], ascending=[True, True, False])
    emit("writing", "Writing processed outputs", P_WRITE)
    ensemble_meta = summarize_extraction_reports(
        extraction_reports,
        backends_configured=eff_backends,
        mode=eff_mode,
    )
    _write_outputs(combined, warnings, processed_files, extraction_ensemble=ensemble_meta)
    emit("complete", "Finished processing", 1.0)
    return {"warnings": warnings, "processed_files": processed_files}


def load_transactions() -> pd.DataFrame:
    ensure_directories()
    if not TRANSACTIONS_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(TRANSACTIONS_CSV)
    if df.empty:
        return df
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")
    if "internal_detection" not in df.columns:
        df["internal_detection"] = "none"
    if "tx_key" not in df.columns and not df.empty:
        try:
            df["tx_key"] = transaction_key_series(df)
        except (TypeError, ValueError, KeyError):
            df["tx_key"] = ""
    if "category_source" not in df.columns:
        df["category_source"] = "auto"
    if "extraction_confidence" not in df.columns:
        df["extraction_confidence"] = 1.0
    if "extraction_sources" not in df.columns:
        df["extraction_sources"] = ""
    if "extraction_disagreement" not in df.columns:
        df["extraction_disagreement"] = False
    return df


def load_pipeline_meta() -> dict[str, object]:
    if not PIPELINE_META_JSON.exists():
        return {"updated_at": None, "warnings": [], "processed_files": []}
    return json.loads(PIPELINE_META_JSON.read_text(encoding="utf-8"))
