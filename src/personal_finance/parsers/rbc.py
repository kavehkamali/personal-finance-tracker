from __future__ import annotations

import html
import re
from datetime import date, datetime
from pathlib import Path
from urllib.parse import unquote

import pandas as pd


NAME_BLACKLIST = {
    "SIGNATURE RBC REWARDS VISA",
    "CONTACT US",
    "IMPORTANT INFORMATION",
    "CALCULATING YOUR BALANCE",
    "RBC ROYAL BANK",
    "CREDIT CARD PAYMENT CENTRE",
    "PAYMENTS & INTEREST RATES",
    "AVION POINTS",
    "STAY SAFE FROM FRAUD & SCAMS",
}

DATE_FRAGMENT_RE = re.compile(r"^[A-Z]{3}\s+\d{1,2}$", re.IGNORECASE)
# e.g. STATEMENT FROM DEC 09, 2025 TO JAN 8, 2026 (optional year after first date)
PERIOD_RE = re.compile(
    r"STATEMENT\s+FROM\s+([A-Z]{3}\s+\d{1,2})(?:,\s*(\d{4}))?\s+TO\s+([A-Z]{3}\s+\d{1,2}),\s*(\d{4})",
    re.IGNORECASE,
)
TABLE_RE = re.compile(r"<table>.*?</table>", re.IGNORECASE | re.DOTALL)
TR_RE = re.compile(r"<tr>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
TD_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)


def _clean_cell(value: str) -> str:
    text = re.sub(r"<.*?>", "", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _is_probable_name(candidate: str) -> bool:
    candidate = candidate.strip()
    if candidate.upper() != candidate:
        return False
    if candidate in NAME_BLACKLIST:
        return False
    words = candidate.split()
    if len(words) < 2 or len(words) > 4:
        return False
    return all(word.isalpha() and len(word) > 1 for word in words)


def _extract_statement_period(text: str) -> tuple[date | None, date | None, int | None]:
    match = PERIOD_RE.search(text)
    if match:
        start_fragment, start_year_opt, end_fragment, end_year_text = match.groups()
        end_year = int(end_year_text)
        start_year = int(start_year_opt) if start_year_opt else end_year
        end_date = datetime.strptime(f"{end_fragment.upper()} {end_year}", "%b %d %Y").date()
        start_date = datetime.strptime(f"{start_fragment.upper()} {start_year}", "%b %d %Y").date()
        if start_date > end_date:
            start_date = start_date.replace(year=start_year - 1)
        return start_date, end_date, end_year

    # Chequing / banking PDFs: "From December 17, 2025 to January 16, 2026"
    long_match = re.search(
        r"From\s+([A-Za-z]+\s+\d{1,2}),\s*(\d{4})\s+to\s+([A-Za-z]+\s+\d{1,2}),\s*(\d{4})",
        text,
        re.IGNORECASE,
    )
    if long_match:
        s_frag, y1, e_frag, y2 = long_match.groups()
        y1_i, y2_i = int(y1), int(y2)
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                start_date = datetime.strptime(f"{s_frag} {y1_i}", fmt).date()
                end_date = datetime.strptime(f"{e_frag} {y2_i}", fmt).date()
                return start_date, end_date, end_date.year
            except ValueError:
                continue

    return None, None, None


def _parse_date_fragment(fragment: str, start_date: date | None, end_date: date | None, fallback_year: int | None) -> date | None:
    if not fragment or fallback_year is None:
        return None

    parsed = datetime.strptime(f"{fragment.upper()} {fallback_year}", "%b %d %Y").date()
    if start_date is None or end_date is None:
        return parsed

    if parsed > end_date:
        return parsed.replace(year=parsed.year - 1)
    if parsed < start_date and start_date.year != end_date.year:
        return parsed.replace(year=parsed.year + 1)
    return parsed


def _extract_metadata(markdown_text: str, source_path: Path) -> dict[str, object]:
    decoded_name = unquote(source_path.name)
    decoded_stem = unquote(source_path.stem)
    lowered = markdown_text.lower()

    start_date, end_date, fallback_year = _extract_statement_period(markdown_text)
    all_names = []
    for line in markdown_text.splitlines()[:120]:
        cleaned = _clean_cell(line.lstrip("# "))
        if _is_probable_name(cleaned):
            all_names.append(cleaned.title())

    unique_names = list(dict.fromkeys(all_names))
    account_last4 = None
    file_match = re.search(r"-(\d{4})(?:\D|$)", decoded_stem)
    if file_match:
        account_last4 = file_match.group(1)

    filename_lower = decoded_name.lower()
    if "credit line" in filename_lower or "line of credit" in filename_lower:
        account_name = "RBC Credit Line"
        statement_type = "credit_line"
    elif "mastercard" in filename_lower:
        account_name = "RBC MasterCard"
        statement_type = "credit_card"
    elif "visa" in filename_lower:
        account_name = "RBC Visa"
        statement_type = "credit_card"
    elif "mastercard" in lowered:
        account_name = "RBC MasterCard"
        statement_type = "credit_card"
    elif "visa" in lowered:
        account_name = "RBC Visa"
        statement_type = "credit_card"
    else:
        account_name = "RBC Bank Account"
        statement_type = "bank_account"

    return {
        "decoded_name": decoded_name,
        "statement_id": decoded_stem,
        "account_name": account_name,
        "account_last4": account_last4 or "unknown",
        "statement_type": statement_type,
        "owners": unique_names or ["Household"],
        "household": " & ".join(unique_names[:2]) if unique_names else "Household",
        "statement_start": start_date.isoformat() if start_date else None,
        "statement_end": end_date.isoformat() if end_date else None,
        "statement_start_date": start_date,
        "statement_end_date": end_date,
        "fallback_year": fallback_year,
        "source_path": str(source_path),
    }


def _iter_tables_with_context(markdown_text: str):
    position = 0
    for match in TABLE_RE.finditer(markdown_text):
        yield markdown_text[position:match.start()], match.group(0)
        position = match.end()


def _table_rows(table_html: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row_html in TR_RE.findall(table_html):
        cells = [_clean_cell(cell) for cell in TD_RE.findall(row_html)]
        if cells:
            rows.append(cells)
    return rows


def _infer_context(context: str, metadata: dict[str, object]) -> dict[str, str]:
    tail = "\n".join(context.splitlines()[-20:])
    names = [
        _clean_cell(line.lstrip("# "))
        for line in tail.splitlines()
        if _is_probable_name(_clean_cell(line.lstrip("# ")))
    ]
    owner = names[-1].title() if names else str(metadata["owners"][0])
    role_match = re.search(r"\b(PRIMARY|CO-APPLICANT|JOINT|AUTHORIZED USER)\b", tail, re.IGNORECASE)
    role = role_match.group(1).title() if role_match else "Holder"
    account_match = re.search(r"(\d{4})\s*-\s*(PRIMARY|CO-APPLICANT|JOINT|AUTHORIZED USER)", tail, re.IGNORECASE)
    account_last4 = account_match.group(1) if account_match else str(metadata["account_last4"])
    account_label = f"{metadata['account_name']} • {account_last4}"
    return {
        "owner": owner,
        "role": role,
        "account_last4": account_last4,
        "account_label": account_label,
    }


def _parse_amount(raw_amount: str) -> float:
    cleaned = raw_amount.replace("$", "").replace(",", "").strip()
    if not cleaned:
        return 0.0
    negative = cleaned.startswith("-")
    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    if not match:
        return 0.0
    amount = float(match.group(0))
    return -amount if negative else amount


def _extract_reference_and_description(description: str) -> tuple[str, str]:
    description = _clean_cell(description)
    ref_match = re.search(r"(\d{10,})$", description)
    if not ref_match:
        return "", description
    reference = ref_match.group(1)
    normalized = description[: -len(reference)].strip()
    return reference, normalized


def _parse_credit_card_table(rows: list[list[str]], metadata: dict[str, object], context: dict[str, str]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    current_transaction: dict[str, object] | None = None

    for cells in rows:
        joined = " ".join(cells).upper()
        if "TRANSACTION DATE" in joined or "ACTIVITY DESCRIPTION" in joined:
            continue
        if "SUBTOTAL" in joined or "CREDIT BALANCE" in joined:
            continue

        if cells[0] == "" and current_transaction:
            continuation = cells[2] if len(cells) > 2 else ""
            compact = re.sub(r"\s+", "", continuation)
            if compact.isdigit() and len(compact) >= 10:
                current_transaction["reference"] = compact
            elif continuation:
                current_transaction["notes"] = " ".join(
                    filter(None, [str(current_transaction.get("notes", "")), continuation])
                ).strip()
            continue

        transaction_date_raw = cells[0]
        if not DATE_FRAGMENT_RE.match(transaction_date_raw):
            continue

        posting_date_raw = cells[1] if len(cells) > 1 else ""
        description = cells[2] if len(cells) > 2 else ""
        amount = _parse_amount(cells[-1])
        embedded_reference, normalized_description = _extract_reference_and_description(description)

        current_transaction = {
            "statement_id": metadata["statement_id"],
            "source_file": metadata["decoded_name"],
            "source_path": metadata["source_path"],
            "source_kind": "markdown",
            "statement_type": metadata["statement_type"],
            "account_name": metadata["account_name"],
            "account_last4": context["account_last4"],
            "account_label": context["account_label"],
            "owner": context["owner"],
            "role": context["role"],
            "household": metadata["household"],
            "transaction_date": _parse_date_fragment(
                transaction_date_raw,
                metadata["statement_start_date"],
                metadata["statement_end_date"],
                metadata["fallback_year"],
            ),
            "posting_date": _parse_date_fragment(
                posting_date_raw,
                metadata["statement_start_date"],
                metadata["statement_end_date"],
                metadata["fallback_year"],
            )
            if posting_date_raw
            else None,
            "transaction_date_raw": transaction_date_raw,
            "posting_date_raw": posting_date_raw,
            "description": normalized_description,
            "reference": embedded_reference,
            "amount": amount,
            "balance": None,
            "currency": "CAD",
            "statement_start": metadata["statement_start"],
            "statement_end": metadata["statement_end"],
            "notes": "",
        }
        records.append(current_transaction)

    return records


def _parse_bank_table(rows: list[list[str]], metadata: dict[str, object], context: dict[str, str]) -> list[dict[str, object]]:
    if not rows:
        return []

    headers = [cell.upper() for cell in rows[0]]
    body = rows[1:]
    records: list[dict[str, object]] = []

    for cells in body:
        if not cells or not DATE_FRAGMENT_RE.match(cells[0]):
            continue

        transaction_date_raw = cells[0]
        description = cells[1] if len(cells) > 1 else ""
        debit = ""
        credit = ""
        balance = ""

        for header, value in zip(headers[2:], cells[2:]):
            if "DEBIT" in header or "WITHDRAW" in header:
                debit = value
            elif "CREDIT" in header or "DEPOSIT" in header:
                credit = value
            elif "AMOUNT" in header:
                debit = value
            elif "BALANCE" in header:
                balance = value

        if debit and credit:
            amount = _parse_amount(credit) - _parse_amount(debit)
        elif credit:
            amount = _parse_amount(credit)
        else:
            amount = -abs(_parse_amount(debit))

        bal_val: float | None = None
        if str(balance).strip():
            bal_val = _parse_amount(balance)

        reference, normalized_description = _extract_reference_and_description(description)
        records.append(
            {
                "statement_id": metadata["statement_id"],
                "source_file": metadata["decoded_name"],
                "source_path": metadata["source_path"],
                "source_kind": "markdown",
                "statement_type": "bank_account",
                "account_name": metadata["account_name"],
                "account_last4": context["account_last4"],
                "account_label": context["account_label"],
                "owner": context["owner"],
                "role": context["role"],
                "household": metadata["household"],
                "transaction_date": _parse_date_fragment(
                    transaction_date_raw,
                    metadata["statement_start_date"],
                    metadata["statement_end_date"],
                    metadata["fallback_year"],
                ),
                "posting_date": None,
                "transaction_date_raw": transaction_date_raw,
                "posting_date_raw": "",
                "description": normalized_description,
                "reference": reference,
                "amount": amount,
                "balance": bal_val,
                "currency": "CAD",
                "statement_start": metadata["statement_start"],
                "statement_end": metadata["statement_end"],
                "notes": "",
            }
        )

    return records


def parse_rbc_markdown(source_path: Path) -> pd.DataFrame:
    markdown_text = source_path.read_text(encoding="utf-8")
    metadata = _extract_metadata(markdown_text, source_path)
    records: list[dict[str, object]] = []

    for context_text, table_html in _iter_tables_with_context(markdown_text):
        rows = _table_rows(table_html)
        if not rows:
            continue

        headers = [cell.upper() for cell in rows[0]]
        context = _infer_context(context_text, metadata)

        if "TRANSACTION DATE" in " ".join(headers) and "ACTIVITY DESCRIPTION" in " ".join(headers):
            records.extend(_parse_credit_card_table(rows, metadata, context))
            continue
        if any("DEBIT" in header or "WITHDRAW" in header for header in headers):
            records.extend(_parse_bank_table(rows, metadata, context))
            continue
        if headers[:2] == ["DATE", "DESCRIPTION"]:
            records.extend(_parse_bank_table(rows, metadata, context))

    frame = pd.DataFrame.from_records(records)
    if frame.empty:
        return frame

    frame["transaction_date"] = pd.to_datetime(frame["transaction_date"], errors="coerce")
    frame["posting_date"] = pd.to_datetime(frame["posting_date"], errors="coerce")
    return frame
