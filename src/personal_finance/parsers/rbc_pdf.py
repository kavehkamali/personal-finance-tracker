"""
Parse RBC statement PDFs from embedded text (no MinerU/OCR).

RBC exports selectable text for most statements; this path runs first so the
dashboard works without optional OCR dependencies.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from pypdf import PdfReader

from personal_finance.parsers.rbc import (
    DATE_FRAGMENT_RE,
    _clean_cell,
    _extract_metadata,
    _extract_reference_and_description,
    _is_probable_name,
    _parse_amount,
    _parse_date_fragment,
)


def extract_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    parts: list[str] = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)


_CC_TXN_START = re.compile(r"^([A-Z]{3}\s+\d{1,2})\s+([A-Z]{3}\s+\d{1,2})\s+(.+)$")
_CC_AMOUNT = re.compile(r"^-?\$[\d,]+\.\d{2}$")
_CC_REF = re.compile(r"^\d{12,}$")
_CC_CARD_LINE = re.compile(
    r"(\d{4})\s+\d{2}\*+\s*\*+\*+\s*(\d{4})\s*-\s*(PRIMARY|CO-APPLICANT|JOINT|AUTHORIZED USER)",
    re.IGNORECASE,
)

_BANK_DATE_LINE = re.compile(r"^(\d{1,2}\s+[A-Z][a-z]{2})\s+(.+)$")
_MONEY = re.compile(r"([\d,]+\.\d{2})")

_POSITIVE_WORDS = (
    "deposit",
    "payroll",
    "refund",
    "rebate",
    "reward",
    "interest",
    "e-transfer received",
    "transfer received",
)
_NEGATIVE_WORDS = (
    "payment",
    "purchase",
    "withdrawal",
    "loan payment",
    "etransfer sent",
    "e-transfer sent",
    "interac purchase",
    "contactless",
    "atm withdrawal",
    "monthly fee",
    "partial monthly fee",
    "bill payment",
    "online banking transfer",
)


def _bank_amount_sign(description: str) -> float:
    d = description.lower()
    if any(w in d for w in _POSITIVE_WORDS):
        return 1.0
    if any(w in d for w in _NEGATIVE_WORDS):
        return -1.0
    return -1.0


def _parse_bank_day_month(
    fragment: str,
    statement_start: date | None,
    statement_end: date | None,
    fallback_year: int | None,
) -> date | None:
    fragment = fragment.strip()
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]{3})$", fragment, re.IGNORECASE)
    if not m or fallback_year is None:
        return None
    day, mon = int(m.group(1)), m.group(2).upper()
    try:
        parsed = datetime.strptime(f"{mon} {day} {fallback_year}", "%b %d %Y").date()
    except ValueError:
        return None
    if statement_start and statement_end:
        if parsed > statement_end:
            parsed = parsed.replace(year=parsed.year - 1)
        if parsed < statement_start and statement_start.year != statement_end.year:
            parsed = parsed.replace(year=parsed.year + 1)
    return parsed


def _parse_credit_card_pdf_text(text: str, source_path: Path) -> list[dict[str, object]]:
    metadata = _extract_metadata(text, source_path)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    records: list[dict[str, object]] = []
    i = 0
    current_owner = str(metadata["owners"][0])
    current_role = "Holder"
    current_last4 = str(metadata["account_last4"])

    while i < len(lines):
        line = lines[i]

        m_card = _CC_CARD_LINE.search(line)
        if m_card:
            current_last4 = m_card.group(2)
            current_role = m_card.group(3).title()
            if i > 0 and _is_probable_name(lines[i - 1]):
                current_owner = lines[i - 1].title()
            i += 1
            continue

        if "SUBTOTAL" in line.upper() or re.search(r"\b\d+\s+OF\s+\d+\b", line, re.I):
            i += 1
            continue

        m_txn = _CC_TXN_START.match(line)
        if not m_txn:
            i += 1
            continue

        trans_raw, post_raw, desc_start = m_txn.groups()
        if not DATE_FRAGMENT_RE.match(trans_raw):
            i += 1
            continue

        i += 1
        desc_parts = [desc_start]
        reference = ""
        amount_str: str | None = None

        while i < len(lines):
            ln = lines[i]
            if _CC_REF.match(ln):
                reference = ln
                i += 1
                continue
            if _CC_AMOUNT.match(ln):
                amount_str = ln
                i += 1
                break
            next_txn = _CC_TXN_START.match(ln)
            if next_txn and DATE_FRAGMENT_RE.match(next_txn.group(1)):
                break
            desc_parts.append(ln)
            i += 1

        if amount_str is None:
            continue

        description = _clean_cell(" ".join(desc_parts))
        amount = _parse_amount(amount_str)
        emb_ref, normalized = _extract_reference_and_description(description)
        if not reference and emb_ref:
            reference = emb_ref

        account_label = f"{metadata['account_name']} • {current_last4}"
        tx_date = _parse_date_fragment(
            trans_raw,
            metadata["statement_start_date"],
            metadata["statement_end_date"],
            metadata["fallback_year"],
        )
        post_date = _parse_date_fragment(
            post_raw,
            metadata["statement_start_date"],
            metadata["statement_end_date"],
            metadata["fallback_year"],
        )

        records.append(
            {
                "statement_id": metadata["statement_id"],
                "source_file": metadata["decoded_name"],
                "source_path": metadata["source_path"],
                "source_kind": "pdf_text",
                "statement_type": metadata["statement_type"],
                "account_name": metadata["account_name"],
                "account_last4": current_last4,
                "account_label": account_label,
                "owner": current_owner,
                "role": current_role,
                "household": metadata["household"],
                "transaction_date": tx_date,
                "posting_date": post_date,
                "transaction_date_raw": trans_raw,
                "posting_date_raw": post_raw,
                "description": normalized,
                "reference": reference,
                "amount": amount,
                "currency": "CAD",
                "statement_start": metadata["statement_start"],
                "statement_end": metadata["statement_end"],
                "notes": "",
            }
        )

    return records


def _parse_bank_pdf_text(text: str, source_path: Path) -> list[dict[str, object]]:
    metadata = _extract_metadata(text, source_path)
    # Activity table section
    idx = text.lower().find("details of your account activity")
    if idx < 0:
        return []
    section = text[idx:]
    lines = [ln.strip() for ln in section.splitlines() if ln.strip()]

    owner = str(metadata["owners"][0])
    last4 = str(metadata["account_last4"])
    account_label = f"{metadata['account_name']} • {last4}"
    fy = metadata["fallback_year"]
    sd = metadata["statement_start_date"]
    ed = metadata["statement_end_date"]

    records: list[dict[str, object]] = []
    pending_date: str | None = None
    pending_body: str | None = None

    def flush() -> None:
        nonlocal pending_date, pending_body
        if not pending_date or not pending_body:
            pending_date, pending_body = None, None
            return

        body = pending_body
        amounts = [float(x.replace(",", "")) for x in _MONEY.findall(body)]
        if not amounts:
            pending_date, pending_body = None, None
            return

        if len(amounts) >= 2:
            amt_mag = amounts[-2]
        else:
            amt_mag = amounts[-1]

        desc = body
        for _ in range(len(amounts)):
            desc = re.sub(r"[\d,]+\.\d{2}\s*$", "", desc).strip()

        sign = _bank_amount_sign(desc)
        cash_amount = sign * abs(amt_mag)

        tx_date = _parse_bank_day_month(pending_date, sd, ed, fy)
        reference, normalized = _extract_reference_and_description(desc)

        records.append(
            {
                "statement_id": metadata["statement_id"],
                "source_file": metadata["decoded_name"],
                "source_path": metadata["source_path"],
                "source_kind": "pdf_text",
                "statement_type": "bank_account",
                "account_name": metadata["account_name"],
                "account_last4": last4,
                "account_label": account_label,
                "owner": owner,
                "role": "Holder",
                "household": metadata["household"],
                "transaction_date": tx_date,
                "posting_date": None,
                "transaction_date_raw": pending_date,
                "posting_date_raw": "",
                "description": normalized or desc,
                "reference": reference,
                "amount": cash_amount,
                "currency": "CAD",
                "statement_start": metadata["statement_start"],
                "statement_end": metadata["statement_end"],
                "notes": "",
            }
        )
        pending_date, pending_body = None, None

    header_seen = False
    for line in lines:
        low = line.lower()
        if "date" in low and "description" in low and "withdrawals" in low:
            header_seen = True
            continue
        if not header_seen:
            continue
        if low.startswith("opening balance") or low.startswith("closing balance"):
            flush()
            continue

        m = _BANK_DATE_LINE.match(line)
        if m:
            flush()
            pending_date, pending_body = m.group(1), m.group(2)
        elif pending_body is not None:
            pending_body = f"{pending_body} {line}"

    flush()
    return records


def parse_rbc_pdf(source_path: Path) -> pd.DataFrame:
    try:
        text = extract_pdf_text(source_path)
    except Exception:
        return pd.DataFrame()

    if not text or len(text.strip()) < 50:
        return pd.DataFrame()

    t_upper = text.upper()
    records: list[dict[str, object]] = []

    if "POSTING" in t_upper and "ACTIVITY DESCRIPTION" in t_upper and "TRANSACTION" in t_upper:
        records = _parse_credit_card_pdf_text(text, source_path)
    elif "DETAILS OF YOUR ACCOUNT ACTIVITY" in t_upper and "WITHDRAWALS" in t_upper:
        records = _parse_bank_pdf_text(text, source_path)
    else:
        records = _parse_credit_card_pdf_text(text, source_path)
        if not records:
            records = _parse_bank_pdf_text(text, source_path)

    frame = pd.DataFrame.from_records(records)
    if frame.empty:
        return frame

    frame["transaction_date"] = pd.to_datetime(frame["transaction_date"], errors="coerce")
    frame["posting_date"] = pd.to_datetime(frame["posting_date"], errors="coerce")
    return frame
