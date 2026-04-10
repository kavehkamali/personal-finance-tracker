#!/usr/bin/env python3
"""Smoke-test MinerU backends used on macOS (pipeline, vlm-mlx-engine, vlm-transformers)."""
from __future__ import annotations

import shutil
import sys
import tempfile
import traceback
from pathlib import Path

# Repo root
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def make_minimal_pdf(path: Path) -> None:
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    c.drawString(100, 750, "Test statement line: $12.34 on 2024-01-15")
    c.showPage()
    c.save()


def run_backend(name: str, pdf: Path, out: Path) -> tuple[bool, str]:
    from personal_finance.ocr import MINERU_AVAILABLE, parse_doc

    if not MINERU_AVAILABLE:
        return False, "MinerU not importable"
    try:
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        parse_doc([pdf], out, backend=name)
        mds = list(out.rglob("*.md"))
        if not mds:
            return False, "no .md produced"
        return True, f"ok ({mds[0].name}, {mds[0].stat().st_size} bytes)"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"


def main() -> int:
    backends = ["pipeline", "vlm-mlx-engine", "vlm-transformers"]
    with tempfile.TemporaryDirectory(prefix="pf_ocr_test_") as td:
        tdir = Path(td)
        pdf = tdir / "tiny.pdf"
        make_minimal_pdf(pdf)

        print("Platform:", sys.platform)
        print("PDF:", pdf, pdf.stat().st_size, "bytes")
        print()

        all_ok = True
        for b in backends:
            out = tdir / f"out_{b.replace('-', '_')}"
            ok, msg = run_backend(b, pdf, out)
            status = "PASS" if ok else "FAIL"
            print(f"[{status}] {b}")
            print(msg)
            print()
            all_ok = all_ok and ok

        return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
