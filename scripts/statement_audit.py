#!/usr/bin/env python3
"""
Convert statement files to CSV, then audit account/month coverage and totals.

This is a standalone command-line report. It does not start the web app, does
not use OCR, and does not apply dashboard categorization.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote

import pandas as pd


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _safe_csv_name(stem: str) -> str:
    stem = unquote(stem)
    stem = re.sub(r'[<>:"/\\|?*]', "_", stem)
    return stem[:180] + ".csv" if len(stem) > 180 else stem + ".csv"


def _kind_label(kind: str) -> str:
    k = kind.lower()
    if "chequing" in k:
        return "chequing"
    if "visa" in k:
        return "visa"
    if "mastercard" in k:
        return "mastercard"
    if "credit" in k and "line" in k:
        return "credit_line"
    return "unknown"


def _month_span(months: list[str]) -> list[str]:
    if not months:
        return []
    start = pd.Period(min(months), freq="M")
    end = pd.Period(max(months), freq="M")
    return [str(p) for p in pd.period_range(start, end, freq="M")]


def _statement_meta(path: Path) -> dict[str, str | None]:
    from personal_finance.statement_coverage import STEM_RE, logical_statement_stem

    stem = unquote(path.stem)
    m = STEM_RE.match(logical_statement_stem(stem))
    if not m:
        return {
            "filename": unquote(path.name),
            "stem": stem,
            "account_kind": "unknown",
            "account_last4": None,
            "account_key": "unknown",
            "statement_month": None,
            "statement_date": None,
        }

    kind = _kind_label(m.group("kind"))
    last4 = m.group("last4")
    statement_date = f"{m.group('y')}-{m.group('m')}-{m.group('d')}"
    return {
        "filename": unquote(path.name),
        "stem": stem,
        "account_kind": kind,
        "account_last4": last4,
        "account_key": f"{kind}:{last4}",
        "statement_month": f"{m.group('y')}-{m.group('m')}",
        "statement_date": statement_date,
    }


def _dedupe_statement_files(files: list[Path]) -> list[Path]:
    from personal_finance.statement_coverage import logical_statement_stem

    def preference(path: Path) -> tuple[int, int, str]:
        stem = unquote(path.stem)
        suffix_segments = 0
        m = re.search(r"(\d{4}-\d{2}-\d{2})((?:-\d+)*)$", stem)
        if m:
            suffix_segments = len(re.findall(r"-(\d+)", m.group(2) or ""))
        ext_priority = 0 if path.suffix.lower() == ".pdf" else 1
        return (ext_priority, suffix_segments, stem)

    groups: dict[str, list[Path]] = defaultdict(list)
    for path in files:
        groups[logical_statement_stem(unquote(path.stem))].append(path)
    return sorted((min(paths, key=preference) for paths in groups.values()), key=lambda p: unquote(p.stem))


def _parse_statement(path: Path) -> pd.DataFrame:
    from personal_finance.parsers.rbc import parse_rbc_markdown
    from personal_finance.parsers.rbc_pdf import parse_rbc_pdf

    if path.suffix.lower() == ".pdf":
        return parse_rbc_pdf(path)
    if path.suffix.lower() == ".md":
        return parse_rbc_markdown(path)
    raise ValueError(f"Unsupported extension {path.suffix!r}")


def _file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _statement_file_inventory(raw_files: list[Path], selected_files: list[Path]) -> pd.DataFrame:
    from personal_finance.statement_coverage import logical_statement_stem

    selected = {p.resolve() for p in selected_files}
    digest_counts: dict[str, int] = defaultdict(int)
    logical_counts: dict[str, int] = defaultdict(int)
    rows: list[dict[str, object]] = []
    for path in raw_files:
        logical_key = logical_statement_stem(unquote(path.stem))
        digest = _file_digest(path)
        digest_counts[digest] += 1
        logical_counts[logical_key] += 1
        rows.append(
            {
                "filename": unquote(path.name),
                "source_path": str(path),
                "logical_statement_key": logical_key,
                "sha256": digest,
                "selected_for_audit": path.resolve() in selected,
            }
        )
    for row in rows:
        row["same_content_file_count"] = digest_counts[str(row["sha256"])]
        row["same_logical_statement_count"] = logical_counts[str(row["logical_statement_key"])]
        row["ignored_duplicate_copy"] = not bool(row["selected_for_audit"])
    return pd.DataFrame(rows).sort_values(["logical_statement_key", "filename"])


def _extract_all(files: list[Path], output_dir: Path) -> tuple[list[dict[str, object]], dict[str, str]]:
    rows: list[dict[str, object]] = []
    errors: dict[str, str] = {}

    for path in files:
        out_path = output_dir / _safe_csv_name(path.stem)
        meta = _statement_meta(path)
        try:
            df = _parse_statement(path)
            df.to_csv(out_path, index=False)
            row_count = int(len(df))
            status = "empty" if df.empty else "ok"
        except Exception as exc:  # pragma: no cover - operational CLI guard
            pd.DataFrame().to_csv(out_path, index=False)
            row_count = 0
            status = "error"
            errors[path.name] = str(exc)

        rows.append(
            {
                **meta,
                "source_path": str(path),
                "csv_path": str(out_path),
                "extract_status": status,
                "row_count": row_count,
                "extract_error": errors.get(path.name, ""),
            }
        )
        print(f"[{status} rows={row_count}] {path.name} -> {out_path.name}")

    return rows, errors


def _reconcile_all(files: list[Path], extract_rows: list[dict[str, object]], errors: dict[str, str]) -> list[dict[str, object]]:
    from personal_finance.statement_totals_reconcile import _reconcile_out_stub, reconcile_statement_path

    by_name = {str(row["filename"]): row for row in extract_rows}
    out: list[dict[str, object]] = []

    for path in files:
        try:
            rec = reconcile_statement_path(path)
        except Exception as exc:  # pragma: no cover - operational CLI guard
            rec = _reconcile_out_stub(path)
            rec["notes"] = f"Reconcile error: {exc}"

        meta = _statement_meta(path)
        extract = by_name.get(unquote(path.name), {})
        if path.name in errors:
            rec["notes"] = f"{rec.get('notes', '')}; Extract failed: {errors[path.name]}".strip("; ")

        out.append(
            {
                **meta,
                "source_path": str(path),
                "csv_path": extract.get("csv_path", ""),
                "extract_status": extract.get("extract_status", ""),
                "extract_row_count": extract.get("row_count", 0),
                "reconcile_kind": rec.get("kind"),
                "reconcile_ok": bool(rec.get("ok")),
                "statement_in": rec.get("statement_in"),
                "statement_out": rec.get("statement_out"),
                "parsed_in": rec.get("parsed_in"),
                "parsed_out": rec.get("parsed_out"),
                "diff_in": rec.get("diff_in"),
                "diff_out": rec.get("diff_out"),
                "notes": rec.get("notes", ""),
            }
        )

    return out


def _coverage_rows(reconcile_rows: list[dict[str, object]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    recognized = [
        r
        for r in reconcile_rows
        if r.get("account_key") and r.get("account_key") != "unknown" and r.get("statement_month")
    ]
    months = _month_span(sorted({str(r["statement_month"]) for r in recognized}))
    accounts = sorted({str(r["account_key"]) for r in recognized})

    by_account_month: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in recognized:
        by_account_month[(str(row["account_key"]), str(row["statement_month"]))].append(row)

    coverage: list[dict[str, object]] = []
    for account in accounts:
        account_rows = [r for r in recognized if r.get("account_key") == account]
        first = sorted(account_rows, key=lambda r: str(r.get("statement_month") or ""))[0]
        for month in months:
            rows = by_account_month.get((account, month), [])
            if not rows:
                status = "missing"
                filenames = ""
                row_count = 0
                ok = False
                notes = "No statement file for this account/month."
            else:
                row_count = int(sum(int(r.get("extract_row_count") or 0) for r in rows))
                ok = any(bool(r.get("reconcile_ok")) for r in rows)
                status = "full" if ok else "partial"
                filenames = "; ".join(str(r.get("filename") or "") for r in rows)
                notes = " | ".join(str(r.get("notes") or "") for r in rows)

            coverage.append(
                {
                    "account_key": account,
                    "account_kind": first.get("account_kind"),
                    "account_last4": first.get("account_last4"),
                    "month": month,
                    "status": status,
                    "reconcile_ok": ok,
                    "row_count": row_count,
                    "filenames": filenames,
                    "notes": notes,
                }
            )

    coverage_df = pd.DataFrame(coverage)
    if coverage_df.empty:
        account_df = pd.DataFrame(
            columns=[
                "account_key",
                "account_kind",
                "account_last4",
                "months_full",
                "months_partial",
                "months_missing",
                "available_months",
                "first_month",
                "last_month",
            ]
        )
    else:
        account_rows_out: list[dict[str, object]] = []
        for account, grp in coverage_df.groupby("account_key", sort=True):
            avail = grp[grp["status"].isin(["full", "partial"])]
            first = grp.iloc[0]
            middle_missing = []
            leading_missing = []
            trailing_missing = []
            if not avail.empty:
                first_avail = str(avail["month"].min())
                last_avail = str(avail["month"].max())
                for _, row in grp[grp["status"] == "missing"].iterrows():
                    month = str(row["month"])
                    if month < first_avail:
                        leading_missing.append(month)
                    elif month > last_avail:
                        trailing_missing.append(month)
                    else:
                        middle_missing.append(month)
            else:
                first_avail = ""
                last_avail = ""
            account_rows_out.append(
                {
                    "account_key": account,
                    "account_kind": first["account_kind"],
                    "account_last4": first["account_last4"],
                    "months_full": int((grp["status"] == "full").sum()),
                    "months_partial": int((grp["status"] == "partial").sum()),
                    "months_missing": int((grp["status"] == "missing").sum()),
                    "middle_missing_count": len(middle_missing),
                    "middle_missing_months": ", ".join(middle_missing),
                    "leading_missing_months": ", ".join(leading_missing),
                    "trailing_missing_months": ", ".join(trailing_missing),
                    "has_middle_gap": bool(middle_missing),
                    "available_months": ", ".join(avail["month"].astype(str).tolist()),
                    "first_month": first_avail,
                    "last_month": last_avail,
                }
            )
        account_df = pd.DataFrame(account_rows_out)

    return coverage_df, account_df


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_dir",
        type=Path,
        nargs="?",
        default=Path("input_statements"),
        help="Folder containing .pdf and/or .md statement files (default: input_statements)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for CSVs and audit reports (default: <input_dir>/statement_audit)",
    )
    parser.add_argument(
        "--include-duplicates",
        action="store_true",
        help="Do not collapse duplicate RBC downloads with the same account/date.",
    )
    args = parser.parse_args()

    root = _repo_root()
    for candidate in (root, root / "src"):
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))

    input_dir = args.input_dir.expanduser().resolve()
    if not input_dir.is_dir():
        print(f"Not a directory: {input_dir}", file=sys.stderr)
        return 1

    output_dir = (args.output_dir or (input_dir / "statement_audit")).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_files = sorted(
        p
        for p in input_dir.iterdir()
        if p.is_file() and p.suffix.lower() in {".pdf", ".md"}
    )
    files = raw_files
    if not args.include_duplicates:
        files = _dedupe_statement_files(files)

    if not files:
        print(f"No .pdf or .md files in {input_dir}")
        return 0

    extract_rows, errors = _extract_all(files, output_dir)
    reconcile_rows = _reconcile_all(files, extract_rows, errors)
    coverage_df, account_df = _coverage_rows(reconcile_rows)

    extraction_path = output_dir / "statement_extraction.csv"
    inventory_path = output_dir / "statement_file_inventory.csv"
    reconcile_path = output_dir / "statement_reconciliation.csv"
    coverage_path = output_dir / "account_month_coverage.csv"
    account_path = output_dir / "account_summary.csv"

    _statement_file_inventory(raw_files, files).to_csv(inventory_path, index=False)
    pd.DataFrame(extract_rows).to_csv(extraction_path, index=False)
    pd.DataFrame(reconcile_rows).to_csv(reconcile_path, index=False)
    coverage_df.to_csv(coverage_path, index=False)
    account_df.to_csv(account_path, index=False)

    full_n = int((coverage_df["status"] == "full").sum()) if not coverage_df.empty else 0
    partial_n = int((coverage_df["status"] == "partial").sum()) if not coverage_df.empty else 0
    missing_n = int((coverage_df["status"] == "missing").sum()) if not coverage_df.empty else 0
    middle_gap_n = int(account_df["has_middle_gap"].sum()) if not account_df.empty else 0
    reconcile_ok_n = sum(1 for row in reconcile_rows if row.get("reconcile_ok"))

    print()
    print(f"Statements processed: {len(files)}")
    print(f"Accounts found: {len(account_df)}")
    print(f"Account-months: full={full_n}, partial={partial_n}, missing={missing_n}")
    print(f"Accounts with middle-month gaps: {middle_gap_n}")
    print(f"Statement total checks: ok={reconcile_ok_n}, not_ok={len(reconcile_rows) - reconcile_ok_n}")
    print(f"CSV statements and reports: {output_dir}")
    print(f"- {inventory_path.name}")
    print(f"- {extraction_path.name}")
    print(f"- {reconcile_path.name}")
    print(f"- {coverage_path.name}")
    print(f"- {account_path.name}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
