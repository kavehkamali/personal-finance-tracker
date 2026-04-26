#!/usr/bin/env python3
"""
Step 1 — folder → one CSV per statement file plus a reconciliation summary.

Reads each ``.pdf`` (embedded text via ``parse_rbc_pdf``) and each ``.md``
(``parse_rbc_markdown``). No OCR, no merge, no categories — raw parse only.

Each row CSV includes a ``balance`` column when the source line carries a
balance (chequing PDF / markdown bank tables); otherwise the cell is empty.

Also writes ``extraction_summary.csv`` in the output directory: per file,
statement summary totals (from the document text) vs sums of parsed rows
and the differences.

Run from the repository root (see below).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote

import pandas as pd


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _safe_csv_name(stem: str) -> str:
    stem = unquote(stem)
    stem = re.sub(r'[<>:"/\\|?*]', "_", stem)
    return stem[:180] + ".csv" if len(stem) > 180 else stem + ".csv"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Folder containing .pdf and/or .md statement files",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for CSV files (default: <input_dir>/extracted_csv)",
    )
    args = parser.parse_args()

    root = _repo_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    input_dir = args.input_dir.expanduser().resolve()
    if not input_dir.is_dir():
        print(f"Not a directory: {input_dir}", file=sys.stderr)
        return 1

    output_dir = (args.output_dir or (input_dir / "extracted_csv")).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    from personal_finance.parsers.rbc import parse_rbc_markdown
    from personal_finance.parsers.rbc_pdf import parse_rbc_pdf
    from personal_finance.statement_totals_reconcile import (
        _reconcile_out_stub,
        reconcile_statement_path,
    )

    files = sorted(
        p
        for p in input_dir.iterdir()
        if p.is_file() and p.suffix.lower() in {".pdf", ".md"}
    )
    if not files:
        print(f"No .pdf or .md files in {input_dir}")
        return 0

    ok, empty, errors = 0, 0, 0
    extract_errors: dict[str, str] = {}
    for path in files:
        out_path = output_dir / _safe_csv_name(path.stem)
        try:
            if path.suffix.lower() == ".pdf":
                df = parse_rbc_pdf(path)
            else:
                df = parse_rbc_markdown(path)
            if df is None or df.empty:
                empty += 1
                print(f"[empty] {path.name} -> {out_path.name}")
            else:
                ok += 1
                print(f"[rows={len(df)}] {path.name} -> {out_path.name}")
            df.to_csv(out_path, index=False)
        except Exception as exc:  # pragma: no cover
            errors += 1
            print(f"[error] {path.name}: {exc}", file=sys.stderr)
            extract_errors[path.name] = str(exc)

    summary_rows: list[dict[str, object]] = []
    for path in files:
        try:
            row = reconcile_statement_path(path)
        except Exception as exc:  # pragma: no cover
            row = _reconcile_out_stub(path)
            row["notes"] = f"Reconcile error: {exc}"
        if path.name in extract_errors:
            extra = f"Extract failed: {extract_errors[path.name]}"
            row["notes"] = f"{row.get('notes', '')}; {extra}".strip("; ")
        summary_rows.append(row)

    summary_path = output_dir / "extraction_summary.csv"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    print(f"Summary: {summary_path}")

    print(f"Done. CSV dir: {output_dir}  (ok={ok}, empty={empty}, errors={errors})")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
