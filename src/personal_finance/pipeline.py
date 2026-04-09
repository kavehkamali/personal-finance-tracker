from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from personal_finance.categories import classify_transaction, infer_flow_type, load_category_rules, normalize_merchant, normalize_text
from personal_finance.transaction_overrides import apply_transaction_overrides, transaction_key_series
from personal_finance.merchant_aliases import invalidate_merchant_aliases_cache
from personal_finance.config import (
    DATA_DIR,
    INPUT_STATEMENTS_DIR,
    OCR_OUTPUT_DIR,
    PIPELINE_META_JSON,
    PROCESSED_DIR,
    SETTINGS_DIR,
    SUPPORTED_UPLOAD_SUFFIXES,
    TRANSACTIONS_CSV,
    UPLOAD_DIR,
)
from personal_finance.ocr import ocr_pdf_to_markdown
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


def _write_outputs(df: pd.DataFrame, warnings: list[str], processed_files: list[str]) -> None:
    ensure_directories()
    df.to_csv(TRANSACTIONS_CSV, index=False)
    PIPELINE_META_JSON.write_text(
        json.dumps(
            {
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "warnings": warnings,
                "processed_files": processed_files,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def rebuild_dataset(
    progress_callback: Callable[[dict[str, object]], None] | None = None,
    source_paths: list[Path] | None = None,
) -> dict[str, object]:
    ensure_directories()
    invalidate_merchant_aliases_cache()
    warnings: list[str] = []
    frames: list[pd.DataFrame] = []
    processed_files: list[str] = []
    candidate_sources = source_paths or discover_statement_files()
    total_files = max(len(candidate_sources), 1)

    def emit(stage: str, message: str, progress: float, current_file: str | None = None) -> None:
        if progress_callback is None:
            return
        progress_callback(
            {
                "stage": stage,
                "message": message,
                "progress": round(progress, 4),
                "current_file": current_file,
                "processed_files": len(processed_files),
                "total_files": len(candidate_sources),
            }
        )

    emit("discovering", "Scanning statement files", 0.02)

    for index, source_path in enumerate(candidate_sources, start=1):
        try:
            emit(
                "parsing",
                f"Parsing {source_path.name}",
                0.08 + (index / total_files) * 0.5,
                source_path.name,
            )
            frame = pd.DataFrame()
            if source_path.suffix.lower() == ".pdf":
                frame = parse_rbc_pdf(source_path)
                if frame.empty:
                    try:
                        emit("ocr", f"OCR {source_path.name}", 0.05 + ((index - 1) / total_files) * 0.35, source_path.name)
                        markdown_path = ocr_pdf_to_markdown(source_path, OCR_OUTPUT_DIR)
                        frame = parse_rbc_markdown(markdown_path)
                    except Exception:
                        frame = pd.DataFrame()
            else:
                frame = parse_rbc_markdown(source_path)
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
        emit("writing", "Writing empty dataset", 0.95)
        _write_outputs(empty, warnings, processed_files)
        emit("complete", "Finished processing", 1.0)
        return {"warnings": warnings, "processed_files": processed_files}

    combined = pd.concat(frames, ignore_index=True)
    emit("dedupe", "Deduplicating transactions", 0.62)
    combined = _dedupe_transactions(combined)
    emit("categorizing", "Applying category rules", 0.74)
    combined = _enrich_transactions(combined)
    emit("matching", "Reconciling internal transfers", 0.88)
    combined = _match_internal_transfers(combined)
    combined = _match_internal_transfers_second_pass(combined)
    combined = _apply_internal_labels(combined)
    combined["is_internal"] = combined["flow_type"].eq("Internal Transfer") | combined["internal_match_status"].eq("Matched")
    combined = apply_transaction_overrides(combined)
    combined["expense_amount"] = combined.apply(_expense_amount, axis=1)
    combined = combined.sort_values(["transaction_date", "account_label", "amount"], ascending=[True, True, False])
    emit("writing", "Writing processed outputs", 0.96)
    _write_outputs(combined, warnings, processed_files)
    emit("complete", "Finished processing", 1.0)
    return {"warnings": warnings, "processed_files": processed_files}


def load_transactions() -> pd.DataFrame:
    ensure_directories()
    if not TRANSACTIONS_CSV.exists():
        rebuild_dataset()
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
    return df


def load_pipeline_meta() -> dict[str, object]:
    if not PIPELINE_META_JSON.exists():
        return {"updated_at": None, "warnings": [], "processed_files": []}
    return json.loads(PIPELINE_META_JSON.read_text(encoding="utf-8"))
