from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from personal_finance.categories import (
    BENEFICIARY_OPTIONS,
    CATEGORY_OPTIONS,
    NECESSITY_OPTIONS,
    AUTO_VALUE,
    normalize_text,
    resolve_rule_targets,
)
from personal_finance.config import SETTINGS_DIR, TRANSACTION_OVERRIDES_JSON


def _normalize_override_entry(entry: dict[str, Any]) -> dict[str, str] | None:
    if not isinstance(entry, dict):
        return None
    category = normalize_text(entry.get("category", ""))
    if category not in CATEGORY_OPTIONS:
        return None
    necessity = normalize_text(entry.get("necessity", AUTO_VALUE)) or AUTO_VALUE
    if necessity not in NECESSITY_OPTIONS:
        necessity = AUTO_VALUE
    beneficiary = normalize_text(entry.get("beneficiary", AUTO_VALUE)) or AUTO_VALUE
    if beneficiary not in BENEFICIARY_OPTIONS:
        beneficiary = AUTO_VALUE
    resolved = resolve_rule_targets("Household", category, necessity, beneficiary)
    return {"category": category, "necessity": resolved["necessity"], "beneficiary": resolved["beneficiary"]}


def load_transaction_overrides() -> dict[str, dict[str, str]]:
    SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    if not TRANSACTION_OVERRIDES_JSON.exists():
        return {}
    try:
        raw = json.loads(TRANSACTION_OVERRIDES_JSON.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for key, val in raw.items():
        k = normalize_text(str(key))
        if not k:
            continue
        norm = _normalize_override_entry(val if isinstance(val, dict) else {})
        if norm:
            out[k] = norm
    return out


def save_transaction_overrides(overrides: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    cleaned: dict[str, dict[str, str]] = {}
    for key, val in overrides.items():
        k = normalize_text(str(key))
        if not k:
            continue
        norm = _normalize_override_entry(val)
        if norm:
            cleaned[k] = norm
    TRANSACTION_OVERRIDES_JSON.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")
    return cleaned


def merge_transaction_overrides(updates: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    current = load_transaction_overrides()
    for item in updates:
        if not isinstance(item, dict):
            continue
        tx_key = normalize_text(str(item.get("tx_key", "")))
        if not tx_key:
            continue
        norm = _normalize_override_entry(item)
        if norm:
            current[tx_key] = norm
    return save_transaction_overrides(current)


def transaction_key_series(df: pd.DataFrame) -> pd.Series:
    """Stable key aligned with dedupe logic (post-normalized description)."""
    td = df["transaction_date"].dt.strftime("%Y-%m-%d").fillna("")
    return (
        df["statement_id"].fillna("")
        + "|"
        + df["account_last4"].fillna("")
        + "|"
        + td
        + "|"
        + df["description"].fillna("")
        + "|"
        + df["reference"].fillna("")
        + "|"
        + df["amount"].round(2).astype(str)
    )


def apply_transaction_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "tx_key" not in df.columns:
        return df
    overrides = load_transaction_overrides()
    if not overrides:
        return df
    df = df.copy()
    if "category_source" not in df.columns:
        df["category_source"] = "auto"

    for idx in df.index:
        key = str(df.at[idx, "tx_key"])
        if key not in overrides:
            continue
        o = overrides[key]
        df.at[idx, "category"] = o["category"]
        df.at[idx, "necessity"] = o["necessity"]
        df.at[idx, "beneficiary"] = o["beneficiary"]
        df.at[idx, "category_source"] = "override"
        df.at[idx, "matched_keyword"] = ""

    return df
