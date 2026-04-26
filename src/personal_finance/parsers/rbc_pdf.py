"""
Parse RBC statement PDFs from embedded text (no MinerU/OCR).

RBC exports selectable text for most statements; this path runs first so the
dashboard works without optional OCR dependencies.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from itertools import product
from pathlib import Path
from urllib.parse import unquote

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

# Page boilerplate / footers — flush activity before these so rows are not merged with the next page.
_BANK_BOILERPLATE = re.compile(
    r"^(?:\d+\s+of\s+\d+|RBPDA\d|RBPDA\s|Your\s+RBC\s+personal|account\s+statement|From\s+[A-Za-z]+\s+\d|"
    r"Important\s+information|Never\s+share|Protect\s+your\s+PIN|Here\s+are\s+four|^\*[0-9A-Z]{6,}|"
    r"Royal\s+Bank\s+of\s+Canada|C\.P\.\s+\d+|Montreal\s+QC|https?://|Please\s+check|Please\s+retain|Stay\s+Informed|"
    r"Details\s+of\s+your\s+account\s+activity\s*-\s*continued)",
    re.I,
)

# Continuation line begins a new row for the *same* statement day (RBC omits repeated "DD Mon").
_BANK_NEW_ROW_CONT = re.compile(
    r"^(Online\s+Banking|Online\s+Transfer|Mortgage\s+payment|Interac|Contactless|Payroll|ATM|Loan\s+payment|Partial\s|Monthly|"
    r"e-Transfer\s+sent|e-Transfer\s+received|e-Transfer\s+Request|e-Transfer\s+-|Misc\s+Payment|Utility\s+Bill|Insurance\b|Term\s+Loan|"
    r"Bill\s+Payment|Item\s+returned|NSF\s+item|CRA-|WWW\.)",
    re.I,
)

# Small follow-on lines RBC prints on the next line (same day) — must be a separate row.
_BANK_FOLLOWON_ROW = re.compile(
    r"^(Loan\s+interest|Interest\s+payment|Principal\b)",
    re.I,
)

# RBC often omits the balance on ``Online Banking transfer …`` and prints it on the next
# undated ``Interac`` / ``Contactless`` line — merge so the closing balance applies to both.
_BANK_MERGE_WITH_PREV = re.compile(r"^(Interac|Contactless)", re.I)

# ``Online Banking loan payment … 3,500.00`` may omit the balance; the next ``Mortgage payment`` line carries it.
_MORTGAGE_PAYMENT_LINE = re.compile(r"^Mortgage\s+payment\b", re.I)
# Only merge when the *first* line of the buffer is a type that commonly omits the balance
# (do not merge a completed ``Interac purchase refund`` chunk with the next ``Interac`` row).
# Restrict to ``Online Banking`` — ``e-Transfer sent`` + ``Contactless`` are separate transactions.
_BANK_MERGE_PARENT = re.compile(r"^Online\s+Banking\b", re.I)

_POSITIVE_WORDS = (
    "deposit",
    "payroll",
    "ei canada",
    "employment insurance",
    "refund",
    "rebate",
    "reward",
    "e-transfer received",
    "transfer received",
)
_NEGATIVE_WORDS = (
    "payment",
    "purchase",
    "withdrawal",
    "loan payment",
    "loan interest",
    "etransfer sent",
    "e-transfer sent",
    "interac purchase",
    "contactless",
    "atm withdrawal",
    "monthly fee",
    "partial monthly fee",
    "bill payment",
    "utility bill",
)


def _bank_amount_sign(description: str) -> float:
    d = description.lower()
    # Avoid matching generic "interest" inside "loan interest" (charge) as a credit.
    if "loan interest" in d or "interest charge" in d:
        return -1.0
    # Direction comes from which column RBC used; infer from running balance when possible.
    if "online banking transfer" in d:
        return 0.0
    # Transfers out to a linked deposit / savings account (withdrawal column).
    if "online transfer to deposit" in d:
        return -1.0
    # CRA credits (often printed after ``Online Banking payment - …`` on the prior line).
    if "cra-rev" in d:
        return 1.0
    if "item returned" in d and "nsf" in d:
        return 1.0
    if any(w in d for w in _POSITIVE_WORDS):
        return 1.0
    if any(w in d for w in _NEGATIVE_WORDS):
        return -1.0
    return -1.0


def _bank_kw_sign_with_default(description: str) -> float:
    s = _bank_amount_sign(description)
    return -1.0 if s == 0.0 else s


def _txn_segments_before_amounts(body: str, amounts: list[float]) -> list[str] | None:
    """One description segment per transaction amount (excludes trailing balance)."""
    matches = list(_MONEY.finditer(body))
    if len(matches) != len(amounts):
        return None
    parsed = [float(m.group(1).replace(",", "")) for m in matches]
    for got, want in zip(parsed, amounts):
        if round(got, 2) != round(want, 2):
            return None
    if len(amounts) < 2:
        return None
    segments: list[str] = []
    pos = 0
    for i in range(len(amounts) - 1):
        m = matches[i]
        segments.append(body[pos : m.start()].strip())
        pos = m.end()
    return segments


def _resolve_txn_signs(
    tx_amounts: list[float], net: float, segments: list[str], tol: float = 0.02
) -> list[int] | None:
    """Pick ±1 per row so sum(sign_i * amt_i) == net, respecting non-zero keyword signs."""
    n = len(tx_amounts)
    if n == 0 or n != len(segments) or n > 8:
        return None
    constraints: list[int | None] = []
    for seg in segments:
        ks = _bank_amount_sign(seg)
        constraints.append(None if ks == 0.0 else int(ks))
    for bits in product((-1, 1), repeat=n):
        ok = True
        for i, b in enumerate(bits):
            c = constraints[i]
            if c is not None and b != c:
                ok = False
                break
        if not ok:
            continue
        total = round(sum(bits[i] * tx_amounts[i] for i in range(n)), 2)
        if abs(total - net) <= tol:
            return list(bits)
    return None


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
                "balance": None,
                "currency": "CAD",
                "statement_start": metadata["statement_start"],
                "statement_end": metadata["statement_end"],
                "notes": "",
            }
        )

    return records


def _bank_money_amounts(body: str) -> list[float]:
    return [float(x.replace(",", "")) for x in _MONEY.findall(body)]


def _pdf_loc_activity_layout(text: str, source_path: Path) -> bool:
    """Royal Credit Line activity table (column headers differ from chequing)."""
    if "credit line" in unquote(source_path.name).lower():
        return True
    if "line of credit" in unquote(source_path.name).lower():
        return True
    lo = text.lower()
    pos = lo.find("details of your account activity")
    if pos < 0:
        return False
    window = lo[pos : pos + 800]
    return "payments ($)" in window and ("balance owing" in window or "owing ($)" in window)


def _is_bank_or_loc_activity_header(line: str) -> bool:
    low = line.lower()
    if "date" not in low or "description" not in low:
        return False
    # Chequing / standard bank account
    if "withdrawals" in low and "deposits" in low:
        return True
    # Royal Credit Line — PDFs may break words (e.g. ``Withdrawal s``); uses Payments + Balance owing.
    if "payments ($)" in low and ("balance owing" in low or "owing ($)" in low):
        return True
    return False


def _inside_parentheses_depth(s: str, pos: int) -> bool:
    depth = 0
    for c in s[:pos]:
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
    return depth > 0


def _loc_ordered_amounts(body: str) -> list[float]:
    """Money tokens in reading order; parenthetical amounts are credits (+) toward the line."""
    hits: list[tuple[int, float]] = []
    for m in re.finditer(r"\(([\d,]+\.\d{2})\)", body):
        hits.append((m.start(), float(m.group(1).replace(",", ""))))
    dec = re.compile(r"(?<![\d,])(-?)\s*([\d,]+\.\d{2})")
    for m in dec.finditer(body):
        if _inside_parentheses_depth(body, m.start()):
            continue
        sign = -1.0 if m.group(1) == "-" else 1.0
        hits.append((m.start(), sign * float(m.group(2).replace(",", ""))))
    hits.sort(key=lambda x: x[0])
    return [v for _, v in hits]


def _loc_movement_values(vals: list[float], body_lower: str) -> list[float]:
    """Strip trailing ``balance owing``; drop echoed ``Principal`` amount matching payment."""
    if not vals:
        return []
    if len(vals) == 1:
        return vals[:]
    last = vals[-1]
    if last < -0.005:
        mv = vals[:-1]
        if len(mv) == 2 and abs(mv[0] - mv[1]) < 0.02 and "principal" in body_lower:
            return [mv[0]]
        return mv
    if len(vals) == 2 and abs(vals[0] - vals[1]) < 0.02 and "principal" in body_lower:
        return [vals[0]]
    if len(vals) == 2 and vals[0] >= 0 and vals[1] >= 0 and abs(vals[0] - vals[1]) < 0.02:
        return [vals[0]]
    return vals[:]


def _loc_sign_movement_net(body_lower: str, net: float) -> float | None:
    if abs(net) < 0.005:
        return None
    if "interest payment" in body_lower:
        return abs(net)
    if "www pmt" in body_lower or " pmt " in body_lower:
        return abs(net)
    if (
        "principal" in body_lower
        and "tfr" not in body_lower
        and "www pmt" not in body_lower
        and " pmt " not in body_lower
    ):
        return abs(net)
    if "www tfr" in body_lower or " tfr " in body_lower or "tfr min" in body_lower:
        return -abs(net)
    if "withdrawal" in body_lower or "advance" in body_lower:
        return -abs(net)
    if "interest" in body_lower and "payment" not in body_lower:
        return -abs(net)
    if net > 0:
        return -abs(net)
    return net


def _loc_row_signed_amount(body: str) -> float | None:
    b = body.strip()
    if not b:
        return None
    vals = _loc_ordered_amounts(b)
    mv = _loc_movement_values(vals, b.lower())
    return _loc_sign_movement_net(b.lower(), round(sum(mv), 2))


def _loc_strip_description(body: str) -> str:
    d = body.strip()
    changed = True
    while changed:
        changed = False
        nxt = re.sub(r"\([\d,]+\.\d{2}\)\s*$", "", d).strip()
        if nxt != d:
            d = nxt
            changed = True
            continue
        nxt = re.sub(r"-?\s*[\d,]+\.\d{2}\s*$", "", d).strip()
        if nxt != d:
            d = nxt
            changed = True
    return _clean_cell(d) or body.strip()


# Marketing / legal blocks after the activity table on Royal Credit Line PDFs.
_LOC_ACTIVITY_OFF = re.compile(
    r"^(You\s+could\s+save|Sign\s+in\s+to\s+RBC|It.s\s+fast|Your\s+LoanProtector|"
    r"Rate\s+History|Important\s+information\s+about\s+your\s+account|"
    r"Helpful\s+explanations|Your\s+Royal\s+Credit\s+Line\s+Statement)",
    re.I,
)


def _strip_trailing_amounts(desc: str, n_tail: int) -> str:
    d = desc.strip()
    for _ in range(max(1, n_tail)):
        d = re.sub(r"[\d,]+\.\d{2}\s*$", "", d).strip()
    return d


def _opening_balance_from_lines(lines: list[str]) -> float | None:
    for ln in lines:
        if ln.lower().startswith("opening balance"):
            am = _bank_money_amounts(ln)
            return am[-1] if am else None
    return None


def _infer_undated_fragment(
    chunks: list[tuple[str | None, str]],
    idx: int,
    sd: date | None,
    ed: date | None,
    fy: int | None,
) -> str:
    """Build ``DD Mon`` for a chunk with no leading date (activity continued on a new page)."""
    next_raw: str | None = None
    prev_raw: str | None = None
    for j in range(idx + 1, len(chunks)):
        if chunks[j][0]:
            next_raw = chunks[j][0]
            break
    for j in range(idx - 1, -1, -1):
        if chunks[j][0]:
            prev_raw = chunks[j][0]
            break
    if next_raw and fy:
        nd = _parse_bank_day_month(next_raw, sd, ed, fy)
        if nd:
            guess = nd - timedelta(days=1)
            if prev_raw:
                pd = _parse_bank_day_month(prev_raw, sd, ed, fy)
                if pd and guess <= pd:
                    return prev_raw
            return f"{guess.day} {guess.strftime('%b')}"
    if prev_raw:
        return prev_raw
    if ed and fy:
        return f"{ed.day} {ed.strftime('%b')}"
    return "1 Jan"


def _parse_bank_pdf_text(text: str, source_path: Path) -> list[dict[str, object]]:
    metadata = _extract_metadata(text, source_path)
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
    loc_mode = _pdf_loc_activity_layout(text, source_path)

    chunks: list[tuple[str | None, str]] = []
    cur_date_raw: str | None = None
    buf: list[str] = []
    header_seen = False

    def emit_chunk() -> None:
        nonlocal buf
        if not buf:
            return
        body = " ".join(x for x in buf if x).strip()
        buf = []
        if body:
            chunks.append((cur_date_raw, body))

    for line in lines:
        low = line.lower()
        if _is_bank_or_loc_activity_header(line):
            header_seen = True
            continue
        if not header_seen:
            continue
        if low.startswith("opening balance") or low.startswith("closing balance"):
            emit_chunk()
            cur_date_raw = None
            continue
        if "**account switch**" in low:
            emit_chunk()
            cur_date_raw = None
            continue
        if _BANK_BOILERPLATE.match(line):
            emit_chunk()
            continue
        if loc_mode and _LOC_ACTIVITY_OFF.match(line):
            emit_chunk()
            cur_date_raw = None
            buf = []
            continue

        m = _BANK_DATE_LINE.match(line)
        if m:
            emit_chunk()
            cur_date_raw = m.group(1).strip()
            buf = [m.group(2).strip()]
            continue

        if not buf:
            cur_date_raw = None
            if loc_mode:
                # Do not start a pseudo-row from marketing / rate-table prose (no leading date).
                if not _BANK_DATE_LINE.match(line) and not re.match(r"^Principal\b", line, re.I):
                    continue
            buf = [line]
            continue

        joined_so_far = " ".join(buf)
        if _bank_money_amounts(joined_so_far) and _BANK_FOLLOWON_ROW.match(line):
            if loc_mode:
                buf.append(line)
                continue
            emit_chunk()
            buf = [line]
            continue
        if _bank_money_amounts(joined_so_far) and _BANK_NEW_ROW_CONT.match(line):
            if loc_mode:
                buf.append(line)
                continue
            if _MORTGAGE_PAYMENT_LINE.match(line) and len(_bank_money_amounts(joined_so_far)) == 1:
                buf.append(line)
                continue
            if (
                len(_bank_money_amounts(joined_so_far)) == 1
                and not _BANK_DATE_LINE.match(line)
                and _BANK_MERGE_WITH_PREV.match(line)
                and _BANK_MERGE_PARENT.match(buf[0])
            ):
                buf.append(line)
                continue
            emit_chunk()
            buf = [line]
            continue

        buf.append(line)

    emit_chunk()

    run_bal = _opening_balance_from_lines(lines)
    if run_bal is None:
        run_bal = 0.0

    records: list[dict[str, object]] = []
    stmt_type = str(metadata["statement_type"])
    for i, (date_raw_in, body) in enumerate(chunks):
        date_raw = date_raw_in or _infer_undated_fragment(chunks, i, sd, ed, fy)
        low = body.lower()
        tx_date = _parse_bank_day_month(date_raw, sd, ed, fy)

        if loc_mode:
            vals = _loc_ordered_amounts(body)
            if not vals:
                continue
            signed = _loc_row_signed_amount(body)
            if signed is None:
                continue
            bal_after: float | None = None
            if vals[-1] < -0.005:
                bal_after = vals[-1]
            desc_loc = _loc_strip_description(body)
            ref, norm = _extract_reference_and_description(desc_loc)
            records.append(
                {
                    "statement_id": metadata["statement_id"],
                    "source_file": metadata["decoded_name"],
                    "source_path": metadata["source_path"],
                    "source_kind": "pdf_text",
                    "statement_type": stmt_type,
                    "account_name": metadata["account_name"],
                    "account_last4": last4,
                    "account_label": account_label,
                    "owner": owner,
                    "role": "Holder",
                    "household": metadata["household"],
                    "transaction_date": tx_date,
                    "posting_date": None,
                    "transaction_date_raw": date_raw,
                    "posting_date_raw": "",
                    "description": norm or desc_loc,
                    "reference": ref,
                    "amount": signed,
                    "balance": bal_after,
                    "currency": "CAD",
                    "statement_start": metadata["statement_start"],
                    "statement_end": metadata["statement_end"],
                    "notes": "",
                }
            )
            continue

        amounts = _bank_money_amounts(body)
        if not amounts:
            continue

        def append_bank_row(
            desc_local: str, signed_local: float, balance_after: float | None = None
        ) -> None:
            ref, norm = _extract_reference_and_description(desc_local)
            records.append(
                {
                    "statement_id": metadata["statement_id"],
                    "source_file": metadata["decoded_name"],
                    "source_path": metadata["source_path"],
                    "source_kind": "pdf_text",
                    "statement_type": stmt_type,
                    "account_name": metadata["account_name"],
                    "account_last4": last4,
                    "account_label": account_label,
                    "owner": owner,
                    "role": "Holder",
                    "household": metadata["household"],
                    "transaction_date": tx_date,
                    "posting_date": None,
                    "transaction_date_raw": date_raw,
                    "posting_date_raw": "",
                    "description": norm or desc_local,
                    "reference": ref,
                    "amount": signed_local,
                    "balance": balance_after,
                    "currency": "CAD",
                    "statement_start": metadata["statement_start"],
                    "statement_end": metadata["statement_end"],
                    "notes": "",
                }
            )

        if "partial monthly fee" in low:
            desc = body.strip()
            if (
                len(amounts) >= 2
                and amounts[-1] > 50
                and amounts[-1] > 15 * min(amounts[:-1])
            ):
                fee = min(amounts[:-1])
                new_bal = amounts[-1]
                for _ in range(len(amounts)):
                    desc = re.sub(r"[\d,]+\.\d{2}\s*$", "", desc).strip()
                signed = -abs(fee)
                run_bal = new_bal
                append_bank_row(desc, signed, new_bal)
                continue
            else:
                fee = max(amounts)
                for _ in range(len(amounts)):
                    desc = re.sub(r"[\d,]+\.\d{2}\s*$", "", desc).strip()
                signed = -abs(fee)
                run_bal += signed
                append_bank_row(desc, signed, None)
            continue

        # ``Loan interest … X.XX Y.YY`` — last is balance; do not use balance-delta (run_bal can be wrong).
        if ("loan interest" in low or "interest charge" in low) and len(amounts) >= 2:
            charge = amounts[-2]
            new_bal = amounts[-1]
            desc = _strip_trailing_amounts(body, 2)
            signed = -abs(charge)
            run_bal = new_bal
            append_bank_row(desc, signed, new_bal)
            continue

        # ``Bill Payment … W … B`` — first money is the withdrawal, second is running balance.
        if "bill payment" in low and len(amounts) == 2:
            wdraw = amounts[0]
            new_bal = amounts[1]
            desc = _strip_trailing_amounts(body, 2)
            signed = -abs(wdraw)
            run_bal = new_bal
            append_bank_row(desc, signed, new_bal)
            continue

        # ``Item returned NSF R … B`` — first money is the credit, second is running balance.
        if "item returned" in low and "nsf" in low and len(amounts) == 2:
            refund = amounts[0]
            new_bal = amounts[1]
            desc = _strip_trailing_amounts(body, 2)
            signed = abs(refund)
            run_bal = new_bal
            append_bank_row(desc, signed, new_bal)
            continue

        # ``Insurance … P … B`` — first money is the premium/charge, second is running balance.
        if "insurance" in low and len(amounts) == 2:
            prem = amounts[0]
            new_bal = amounts[1]
            desc = _strip_trailing_amounts(body, 2)
            signed = -abs(prem)
            run_bal = new_bal
            append_bank_row(desc, signed, new_bal)
            continue

        if len(amounts) >= 3 and abs(amounts[-1] - amounts[-2]) > 0.02:
            new_bal = amounts[-1]
            net = round(new_bal - run_bal, 2)
            tx_amounts = amounts[:-1]
            segs = _txn_segments_before_amounts(body, amounts)
            signs = _resolve_txn_signs(tx_amounts, net, segs) if segs else None
            if signs is not None:
                last_j = len(signs) - 1
                for j, (seg, amt, sg) in enumerate(zip(segs, tx_amounts, signs)):
                    append_bank_row(
                        seg.strip(),
                        round(sg * abs(amt), 2),
                        new_bal if j == last_j else None,
                    )
                run_bal = new_bal
                continue
            desc_fb = _strip_trailing_amounts(body, len(amounts))
            append_bank_row(desc_fb or body.strip(), net, new_bal)
            run_bal = new_bal
            continue

        signed: float
        desc: str
        balance_after: float | None = None
        if len(amounts) >= 2 and abs(amounts[-1] - amounts[-2]) > 0.02:
            new_bal = amounts[-1]
            desc = _strip_trailing_amounts(body, 2)
            mag = abs(amounts[-2])
            delta = round(new_bal - run_bal, 2)
            # ``Online Transfer to Deposit`` carries the first balance after several no-balance lines;
            # ``run_bal`` can drift — use the withdrawal column amount.
            if "online transfer to deposit" in desc.lower():
                signed = -mag
            else:
                signed = delta
            run_bal = new_bal
            balance_after = new_bal
        elif len(amounts) >= 2:
            amt_mag = amounts[-2]
            desc = _strip_trailing_amounts(body, 2)
            signed = _bank_kw_sign_with_default(desc) * abs(amt_mag)
            run_bal += signed
        else:
            amt_mag = amounts[-1]
            desc = _strip_trailing_amounts(body, 1)
            signed = _bank_kw_sign_with_default(desc) * abs(amt_mag)
            run_bal += signed

        append_bank_row(desc, signed, balance_after)

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
