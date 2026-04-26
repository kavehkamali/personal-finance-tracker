"""
Scan statement PDF filenames on disk and compare to expected monthly coverage.

Month key = YYYY-MM from the date embedded in the RBC export filename
(e.g. ``Chequing Statement-1117 2026-01-16.pdf`` → ``2026-01``).

Optional expectations: ``data/settings/statement_coverage.json`` — see
``DEFAULT_EXPECTATIONS`` for keys.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from personal_finance.config import INPUT_STATEMENTS_DIR, STATEMENT_COVERAGE_JSON, UPLOAD_DIR, SUPPORTED_UPLOAD_SUFFIXES

# Allow macOS / browser duplicate suffixes: ``… 2026-01-12.pdf``, ``… 2026-01-12-1.pdf``, ``… 2026-01-12-2-1.pdf``
STEM_RE = re.compile(
    r"^(?P<kind>Chequing|Visa|MasterCard|Credit\s+Line)\s+Statement-(?P<last4>\d{4})\s+"
    r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})(?:-\d+)*$",
    re.IGNORECASE,
)


def logical_statement_stem(stem: str) -> str:
    """
    Collapse duplicate-download suffixes so ``… 2026-01-12-2-1`` and ``… 2026-01-12`` map to the same key.
    """
    s = stem.strip()
    m = re.match(r"^(?P<head>.+Statement-\d{4})\s+(?P<ymd>\d{4}-\d{2}-\d{2})(?:-\d+)*$", s, re.IGNORECASE)
    if m:
        return f"{m.group('head')} {m.group('ymd')}"
    return s


def _statement_path_preference_key(path: Path) -> tuple[int, int, int, str]:
    """Prefer input_statements, then PDF over MD, then fewer ``-N`` duplicate segments, then name."""
    stem = unquote(path.stem)
    pri_dir = 0 if path.resolve().parent == INPUT_STATEMENTS_DIR.resolve() else 1
    pri_ext = 0 if path.suffix.lower() == ".pdf" else 1
    tail = 0
    m = re.search(r"(\d{4}-\d{2}-\d{2})((?:-\d+)*)$", stem)
    if m:
        suf = m.group(2) or ""
        tail = len(re.findall(r"-(\d+)", suf))
    return (pri_dir, pri_ext, tail, stem)


def _gather_statement_candidates() -> list[Path]:
    found: list[Path] = []
    for base in (INPUT_STATEMENTS_DIR, UPLOAD_DIR):
        if not base.is_dir():
            continue
        for p in base.iterdir():
            if p.is_file() and p.suffix.lower() in SUPPORTED_UPLOAD_SUFFIXES:
                found.append(p)
    return found

DEFAULT_EXPECTATIONS: dict[str, Any] = {
    "expected_chequing_count": 4,
    "expected_visa_count": 1,
    "expected_mastercard_count": 1,
    "expected_credit_line_count": None,
    "chequing_last4_exact": None,
    "visa_last4_exact": None,
    "mastercard_last4_exact": None,
    "credit_line_last4_exact": None,
}


def _load_expectations() -> tuple[dict[str, Any], str]:
    path = STATEMENT_COVERAGE_JSON
    merged = dict(DEFAULT_EXPECTATIONS)
    source = "defaults"
    if path.is_file():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = {}
        if isinstance(raw, dict):
            for k, v in raw.items():
                if k in merged or k in DEFAULT_EXPECTATIONS:
                    merged[k] = v
            source = str(path)
    return merged, source


def _bucket_kind(kind_raw: str) -> str:
    kr = kind_raw.lower()
    if "chequing" in kr:
        return "chequing"
    if "visa" in kr:
        return "visa"
    if "mastercard" in kr:
        return "mastercard"
    if "credit" in kr and "line" in kr:
        return "credit_line"
    return "other"


def _unique_statement_paths() -> list[Path]:
    """
    One path per *logical* statement (same RBC date + account, ignoring ``-1`` / ``-2-1`` duplicate filenames).

    Prefers ``input_statements/`` over ``.cache/uploads``, PDF over markdown, and the copy with the fewest
    numeric suffix segments.
    """
    candidates = _gather_statement_candidates()
    groups: dict[str, list[Path]] = defaultdict(list)
    for p in candidates:
        stem = unquote(p.stem)
        groups[logical_statement_stem(stem)].append(p)
    chosen: list[Path] = []
    for _key in sorted(groups.keys()):
        paths = groups[_key]
        best = min(paths, key=_statement_path_preference_key)
        chosen.append(best)
    return sorted(chosen, key=lambda p: unquote(p.stem))


def deduped_statement_paths() -> list[Path]:
    """Same list the pipeline should ingest (deduped logical statements, all supported suffixes)."""
    return _unique_statement_paths()


def count_statement_paths_before_dedupe() -> int:
    return len(_gather_statement_candidates())


def unique_statement_pdf_paths() -> list[Path]:
    """Unique PDF stems (``input_statements`` preferred over uploads)."""
    return [p for p in _unique_statement_paths() if p.suffix.lower() == ".pdf"]


def build_statement_coverage_report() -> dict[str, Any]:
    expectations, source = _load_expectations()
    all_pdf = [p for p in _gather_statement_candidates() if p.suffix.lower() == ".pdf"]
    paths = unique_statement_pdf_paths()
    duplicate_pdf_copies_ignored = max(0, len(all_pdf) - len(paths))

    by_month: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: {"chequing": [], "visa": [], "mastercard": [], "credit_line": [], "other": []}
    )
    unrecognized: list[str] = []

    for path in paths:
        stem = unquote(path.stem)
        m = STEM_RE.match(stem)
        if not m:
            unrecognized.append(unquote(path.name))
            continue
        kind_raw = m.group("kind")
        last4 = m.group("last4")
        ym = f"{m.group('y')}-{m.group('m')}"
        bucket = _bucket_kind(kind_raw)
        if bucket == "other":
            unrecognized.append(unquote(path.name))
            continue
        by_month[ym][bucket].append(
            {
                "last4": last4,
                "filename": unquote(path.name),
                "stem": stem,
            }
        )

    def sorted_last4s(entries: list[dict[str, Any]]) -> list[str]:
        return sorted({e["last4"] for e in entries})

    months_out: list[dict[str, Any]] = []
    for ym in sorted(by_month.keys()):
        g = by_month[ym]
        ch_entries = g["chequing"]
        visa_entries = g["visa"]
        mc_entries = g["mastercard"]
        loc_entries = g["credit_line"]

        ch_set = sorted_last4s(ch_entries)
        visa_set = sorted_last4s(visa_entries)
        mc_set = sorted_last4s(mc_entries)
        loc_set = sorted_last4s(loc_entries)

        multifile: list[str] = []
        for bucket_name, entries in (
            ("chequing", ch_entries),
            ("visa", visa_entries),
            ("mastercard", mc_entries),
            ("credit_line", loc_entries),
        ):
            by_stmt_date: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
            for e in entries:
                sm = STEM_RE.match(e["stem"])
                if not sm:
                    continue
                k = (e["last4"], sm.group("y"), sm.group("m"), sm.group("d"))
                by_stmt_date[k].append(e["filename"])
            for (last4, y, mo, d), names in by_stmt_date.items():
                uniq = sorted(set(names))
                if len(uniq) > 1:
                    multifile.append(
                        f"{bucket_name} ·{last4} on {y}-{mo}-{d}: {len(uniq)} different files ({'; '.join(uniq)})"
                    )

        issues: list[str] = []

        exact_ch = expectations.get("chequing_last4_exact")
        if isinstance(exact_ch, list) and all(isinstance(x, str) for x in exact_ch):
            want = sorted(str(x).strip() for x in exact_ch)
            got = sorted(ch_set)
            if want != got:
                issues.append(f"Chequing last4 set mismatch: need {want}, have {got}")
        else:
            n_exp = int(expectations.get("expected_chequing_count") or 0)
            if n_exp > 0 and len(ch_set) != n_exp:
                issues.append(f"Chequing count: expected {n_exp}, found {len(ch_set)} ({', '.join(ch_set) or '—'})")

        exact_v = expectations.get("visa_last4_exact")
        if isinstance(exact_v, list) and all(isinstance(x, str) for x in exact_v):
            want = sorted(str(x).strip() for x in exact_v)
            if sorted(visa_set) != want:
                issues.append(f"Visa last4 mismatch: need {want}, have {sorted(visa_set)}")
        else:
            n_exp = int(expectations.get("expected_visa_count") or 0)
            if n_exp > 0 and len(visa_set) != n_exp:
                issues.append(f"Visa count: expected {n_exp}, found {len(visa_set)} ({', '.join(visa_set) or '—'})")

        exact_mc = expectations.get("mastercard_last4_exact")
        if isinstance(exact_mc, list) and all(isinstance(x, str) for x in exact_mc):
            want = sorted(str(x).strip() for x in exact_mc)
            if sorted(mc_set) != want:
                issues.append(f"MasterCard last4 mismatch: need {want}, have {sorted(mc_set)}")
        else:
            n_exp = int(expectations.get("expected_mastercard_count") or 0)
            if n_exp > 0 and len(mc_set) != n_exp:
                issues.append(f"MasterCard count: expected {n_exp}, found {len(mc_set)} ({', '.join(mc_set) or '—'})")

        loc_n = expectations.get("expected_credit_line_count")
        exact_loc = expectations.get("credit_line_last4_exact")
        if isinstance(exact_loc, list) and all(isinstance(x, str) for x in exact_loc):
            want = sorted(str(x).strip() for x in exact_loc)
            if sorted(loc_set) != want:
                issues.append(f"Credit line last4 mismatch: need {want}, have {sorted(loc_set)}")
        elif loc_n is not None:
            try:
                n_exp = int(loc_n)
            except (TypeError, ValueError):
                n_exp = -1
            if n_exp >= 0 and len(loc_set) != n_exp:
                issues.append(f"Credit line count: expected {n_exp}, found {len(loc_set)} ({', '.join(loc_set) or '—'})")

        for line in multifile:
            issues.append(f"Multiple PDFs: {line}")

        ok = len(issues) == 0
        months_out.append(
            {
                "month_key": ym,
                "chequing_last4": ch_set,
                "visa_last4": visa_set,
                "mastercard_last4": mc_set,
                "credit_line_last4": loc_set,
                "pdf_count": sum(len(g[b]) for b in ("chequing", "visa", "mastercard", "credit_line")),
                "ok": ok,
                "issues": issues,
            }
        )

    caption = (
        "Each row is one calendar month from the date in the PDF filename (RBC’s statement period can differ by a few days). "
        f"Expectations: {expectations.get('expected_chequing_count')} chequing, "
        f"{expectations.get('expected_visa_count')} Visa, {expectations.get('expected_mastercard_count')} MasterCard"
        + (
            f", {expectations.get('expected_credit_line_count')} credit line(s)"
            if expectations.get("expected_credit_line_count") is not None
            else ""
        )
        + ". "
    )
    if source != "defaults":
        caption += f" Custom rules loaded from `data/settings/{STATEMENT_COVERAGE_JSON.name}`."
    else:
        caption += (
            " Optional: add `data/settings/statement_coverage.json` to set exact last4 lists "
            "(`chequing_last4_exact`, etc.) or LOC counts."
        )
    if duplicate_pdf_copies_ignored:
        caption += (
            f" Collapsed {duplicate_pdf_copies_ignored} duplicate PDF download(s) "
            "(same account + statement date; extra ``-1``, ``-2-1`` suffixes ignored)."
        )

    return {
        "expectations": {k: expectations[k] for k in sorted(expectations.keys())},
        "expectations_source": source,
        "caption": caption,
        "unique_pdf_count": len(paths),
        "duplicate_pdf_copies_ignored": duplicate_pdf_copies_ignored,
        "unrecognized_files": unrecognized,
        "months": months_out,
    }
