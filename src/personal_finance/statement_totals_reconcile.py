"""
Compare RBC statement summary totals (first-page style blocks) to sums of rows
from ``parse_rbc_pdf`` for each unique PDF on disk.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pandas as pd

from personal_finance.parsers.rbc_pdf import extract_pdf_text, parse_rbc_pdf
from personal_finance.statement_coverage import STEM_RE, unique_statement_pdf_paths

TOLERANCE = 0.02

# Chequing: summary block
RE_BANK_DEP = re.compile(
    r"total\s+deposits\s+(?:into\s+your\s+account\s*)?\+?\s*([\d,]+\.\d{2})",
    re.IGNORECASE,
)
RE_BANK_WD = re.compile(
    r"total\s+withdrawals\s+(?:from\s+your\s+account\s*)?-?\s*([\d,]+\.\d{2})",
    re.IGNORECASE,
)

# Visa / MasterCard: CALCULATING YOUR BALANCE section
RE_CC_PURCHASES = re.compile(r"Purchases\s*&\s*debits\s*\$?\s*([\d,]+\.\d{2})", re.IGNORECASE)
RE_CC_PAYMENTS = re.compile(r"Payments\s*&\s*credits\s*-\s*\$?\s*([\d,]+\.\d{2})", re.IGNORECASE)

# Royal Credit Line summary (amount may appear as "$0. 00" with a space; wording varies by statement)
RE_LOC_LINE_WD = re.compile(
    r"Sum\s+of\s+withdrawals\s+including\s+adjustments\s+on\s+your\s+account\s*\$?\s*([\d,\s\.]+)",
    re.IGNORECASE,
)
RE_LOC_LINE_PAY = re.compile(
    r"Sum\s+of\s+payments\s+including\s+adjustments\s+on\s+your\s+account\s*\$?\s*([\d,\s\.]+)",
    re.IGNORECASE,
)
RE_LOC_LINE_WD_LOOSE = re.compile(
    r"Sum\s+of\s+withdrawals\s+including\s+adjustments.{0,220}?\$\s*([\d,\s\.]+)",
    re.IGNORECASE | re.DOTALL,
)
RE_LOC_LINE_PAY_LOOSE = re.compile(
    r"Sum\s+of\s+payments\s+including\s+adjustments.{0,220}?\$\s*([\d,\s\.]+)",
    re.IGNORECASE | re.DOTALL,
)


def _fm(s: str) -> float | None:
    if not s or not str(s).strip():
        return None
    t = re.sub(r"\s+", "", str(s).strip()).replace(",", "")
    if not t or not re.fullmatch(r"\d+\.\d{2}", t):
        return None
    return float(t)


def _extract_bank_totals(text: str) -> tuple[float | None, float | None]:
    md, mw = RE_BANK_DEP.search(text), RE_BANK_WD.search(text)
    dep = _fm(md.group(1)) if md else None
    wd = _fm(mw.group(1)) if mw else None
    return dep, wd


def _extract_cc_totals(text: str) -> tuple[float | None, float | None]:
    """Returns (purchases_and_debits, payments_and_credits) as positive magnitudes."""
    mp = RE_CC_PURCHASES.search(text)
    mc = RE_CC_PAYMENTS.search(text)
    pur = _fm(mp.group(1)) if mp else None
    pay = _fm(mc.group(1)) if mc else None
    return pur, pay


def _extract_loc_totals(text: str) -> tuple[float | None, float | None]:
    """Returns (withdrawals magnitude, payments magnitude)."""
    head = text[:12000]
    flat = re.sub(r"\s+", " ", head)

    def pick(tight: re.Pattern[str], loose: re.Pattern[str], blob: str) -> float | None:
        m = tight.search(blob)
        if m:
            v = _fm(m.group(1))
            if v is not None:
                return v
        m2 = loose.search(blob)
        if m2:
            return _fm(m2.group(1))
        return None

    wd = pick(RE_LOC_LINE_WD, RE_LOC_LINE_WD_LOOSE, flat)
    pay = pick(RE_LOC_LINE_PAY, RE_LOC_LINE_PAY_LOOSE, flat)
    return wd, pay


def _classify_path_and_text(path: Path, text: str) -> str:
    fn = unquote(path.name).lower()
    if "credit line" in fn:
        return "credit_line"
    if "chequing" in fn:
        return "bank_account"
    if "visa" in fn or "mastercard" in fn:
        return "credit_card"
    u = text.upper()
    if "POSTING" in u and "ACTIVITY DESCRIPTION" in u and "TRANSACTION" in u:
        return "credit_card"
    if "DETAILS OF YOUR ACCOUNT ACTIVITY" in u and "WITHDRAWALS" in u:
        return "bank_account"
    if "SUM OF PAYMENTS INCLUDING ADJUSTMENTS ON YOUR ACCOUNT" in u.replace("\n", " ").upper():
        return "credit_line"
    return "unknown"


def _parsed_bank_sums(df: pd.DataFrame) -> tuple[float, float]:
    if df.empty or "amount" not in df.columns:
        return 0.0, 0.0
    amt = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    dep = float(amt[amt > 0].sum())
    wd = float(-amt[amt < 0].sum())
    return dep, wd


def _parsed_cc_sums(df: pd.DataFrame) -> tuple[float, float]:
    """Match RBC labels: purchases & debits = positive amounts; payments & credits = abs(negatives)."""
    if df.empty or "amount" not in df.columns:
        return 0.0, 0.0
    amt = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    purchases = float(amt[amt > 0].sum())
    payments = float(-amt[amt < 0].sum())
    return purchases, payments


def _reconcile_out_stub(path: Path) -> dict[str, Any]:
    name = unquote(path.name)
    stem = unquote(path.stem)
    stem_meta = STEM_RE.match(stem)
    last4 = stem_meta.group("last4") if stem_meta else None
    return {
        "filename": name,
        "stem": stem,
        "last4": last4,
        "kind": "unknown",
        "row_count": 0,
        "statement_in": None,
        "statement_out": None,
        "parsed_in": None,
        "parsed_out": None,
        "diff_in": None,
        "diff_out": None,
        "ok": False,
        "notes": "",
    }


def _apply_totals_compare(path: Path, text: str, df: pd.DataFrame, out: dict[str, Any]) -> None:
    kind = _classify_path_and_text(path, text)
    out["kind"] = kind
    out["row_count"] = int(len(df))

    if kind == "bank_account":
        stmt_in, stmt_out = _extract_bank_totals(text)
        pin, pout = _parsed_bank_sums(df)
        if stmt_in is not None and stmt_out is not None and not df.empty and "amount" in df.columns:
            amt = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
            cra_rev = (amt > 0) & df["description"].astype(str).str.contains(
                r"CRA-REV", case=False, na=False, regex=True
            )
            cra_pos = float(amt[cra_rev].sum())
            if cra_pos > 0:
                pin_alt, pout_alt = pin - cra_pos, pout + cra_pos
                if abs(pin_alt - stmt_in) <= TOLERANCE and abs(pout_alt - stmt_out) <= TOLERANCE:
                    pin, pout = pin_alt, pout_alt
        out["statement_in"] = stmt_in
        out["statement_out"] = stmt_out
        out["parsed_in"] = round(pin, 2)
        out["parsed_out"] = round(pout, 2)
        if stmt_in is None or stmt_out is None:
            out["notes"] = "Could not find Total deposits / Total withdrawals in PDF text."
        else:
            out["diff_in"] = round(pin - stmt_in, 2)
            out["diff_out"] = round(pout - stmt_out, 2)
            out["ok"] = abs(pin - stmt_in) <= TOLERANCE and abs(pout - stmt_out) <= TOLERANCE
            if out["ok"]:
                out["notes"] = "Parsed row sums match statement totals (±$0.02)."
            else:
                out["notes"] = "Mismatch: summed transaction rows ≠ statement summary (see diffs)."

    elif kind == "credit_card":
        stmt_pur, stmt_pay = _extract_cc_totals(text)
        pin, pout = _parsed_cc_sums(df)
        out["statement_in"] = stmt_pur
        out["statement_out"] = stmt_pay
        out["parsed_in"] = round(pin, 2)
        out["parsed_out"] = round(pout, 2)
        if stmt_pur is None or stmt_pay is None:
            out["notes"] = "Could not find Purchases & debits / Payments & credits in PDF text."
        else:
            out["diff_in"] = round(pin - stmt_pur, 2)
            out["diff_out"] = round(pout - stmt_pay, 2)
            out["ok"] = abs(pin - stmt_pur) <= TOLERANCE and abs(pout - stmt_pay) <= TOLERANCE
            if out["ok"]:
                out["notes"] = "Parsed row sums match CALCULATING YOUR BALANCE (±$0.02)."
            else:
                out["notes"] = "Mismatch: summed rows ≠ summary block."

    elif kind == "credit_line":
        stmt_wd, stmt_pay = _extract_loc_totals(text)
        out["statement_in"] = stmt_pay
        out["statement_out"] = stmt_wd
        pin, pout = _parsed_bank_sums(df)
        out["parsed_in"] = round(pin, 2)
        out["parsed_out"] = round(pout, 2)
        if stmt_wd is None and stmt_pay is None:
            out["notes"] = "Could not find LOC sum lines in PDF text."
        else:
            if stmt_pay is not None:
                out["diff_in"] = round(pin - stmt_pay, 2)
            if stmt_wd is not None:
                out["diff_out"] = round(pout - stmt_wd, 2)
            if df.empty:
                out["ok"] = False
                out["notes"] = "No rows extracted for this LOC layout; totals shown for reference only."
            elif stmt_pay is not None and stmt_wd is not None:
                out["ok"] = abs(pin - stmt_pay) <= TOLERANCE and abs(pout - stmt_wd) <= TOLERANCE
                out["notes"] = (
                    "Parsed row sums match statement (±$0.02)."
                    if out["ok"]
                    else "Mismatch or LOC parser does not match activity table."
                )
            else:
                out["ok"] = False
                miss = []
                if stmt_pay is None:
                    miss.append("payments")
                if stmt_wd is None:
                    miss.append("withdrawals")
                out["notes"] = f"Partial LOC summary (missing: {', '.join(miss)} line in PDF)."
    else:
        out["notes"] = "Unknown statement layout; skipped totals check."


def reconcile_pdf_totals(path: Path) -> dict[str, Any]:
    out = _reconcile_out_stub(path)

    try:
        text = extract_pdf_text(path)
    except Exception as exc:
        out["notes"] = f"Could not read PDF: {exc}"
        return out

    if not text or len(text.strip()) < 50:
        out["notes"] = "No embedded text (or nearly empty)."
        return out

    try:
        df = parse_rbc_pdf(path)
    except Exception as exc:
        out["notes"] = f"Parse error: {exc}"
        return out

    _apply_totals_compare(path, text, df, out)
    return out


def reconcile_markdown_totals(path: Path) -> dict[str, Any]:
    from personal_finance.parsers.rbc import parse_rbc_markdown

    out = _reconcile_out_stub(path)
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:
        out["notes"] = f"Could not read markdown: {exc}"
        return out

    if not text or len(text.strip()) < 50:
        out["notes"] = "Markdown file empty or too short."
        return out

    try:
        df = parse_rbc_markdown(path)
    except Exception as exc:
        out["notes"] = f"Parse error: {exc}"
        return out

    _apply_totals_compare(path, text, df, out)
    return out


def reconcile_statement_path(path: Path) -> dict[str, Any]:
    suf = path.suffix.lower()
    if suf == ".pdf":
        return reconcile_pdf_totals(path)
    if suf == ".md":
        return reconcile_markdown_totals(path)
    o = _reconcile_out_stub(path)
    o["notes"] = f"Unsupported extension {path.suffix!r}; expected .pdf or .md."
    return o


def build_statement_totals_check() -> dict[str, Any]:
    rows = [reconcile_pdf_totals(p) for p in unique_statement_pdf_paths()]
    rows.sort(key=lambda r: (r.get("stem") or ""))
    ok_n = sum(1 for r in rows if r.get("ok"))
    return {
        "tolerance": TOLERANCE,
        "files": rows,
        "ok_count": ok_n,
        "file_count": len(rows),
        "caption": (
            "Chequing: statement IN = Total deposits, OUT = Total withdrawals. "
            "Visa/MasterCard: IN = Purchases & debits, OUT = Payments & credits (CALCULATING YOUR BALANCE). "
            "Credit line: IN = Sum of payments, OUT = Sum of withdrawals. "
            f"Parsed = sum of signed rows from extraction; match within ±${TOLERANCE:.2f}."
        ),
    }
