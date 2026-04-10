from __future__ import annotations

import re
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path

from personal_finance.config import DEFAULT_OCR_BACKEND, OCR_BATCH_SIZE

try:
    from loguru import logger
    from mineru.cli.common import do_parse, read_fn
    from mineru.utils.enum_class import MakeMode

    MINERU_AVAILABLE = True
except ImportError:
    logger = None
    MINERU_AVAILABLE = False


def parse_doc(path_list: list[Path], output_dir: Path, backend: str = DEFAULT_OCR_BACKEND, lang: str = "en") -> None:
    if not MINERU_AVAILABLE:
        raise RuntimeError("MinerU is not installed. Run `uv sync --extra ocr` to enable PDF OCR.")

    file_name_list = [p.stem for p in path_list]
    pdf_bytes_list = [read_fn(p) for p in path_list]
    lang_list = [lang] * len(path_list)

    # MinerU 3.x: pipeline uses doc_analyze_streaming inside do_parse; VLM paths unchanged at CLI level.
    do_parse(
        str(output_dir),
        file_name_list,
        pdf_bytes_list,
        lang_list,
        backend=backend,
        parse_method="auto",
        formula_enable=True,
        table_enable=True,
        f_draw_layout_bbox=False,
        f_draw_span_bbox=False,
        f_dump_md=True,
        f_dump_middle_json=True,
        f_dump_model_output=True,
        f_dump_orig_pdf=False,
        f_dump_content_list=False,
        f_make_md_mode=MakeMode.MM_MD,
    )
    if logger is not None:
        logger.info("OCR output written under {}", output_dir)


def _backend_cache_slug(backend: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", backend.lower()).strip("_")
    return s or "default"


def ocr_cache_dir_for_backend(output_root: Path, pdf_stem: str, backend: str) -> Path:
    """Isolated cache folder per PDF stem + OCR backend so ensembles do not clobber each other."""
    return output_root / f"{pdf_stem}__{_backend_cache_slug(backend)}"


def _first_markdown_under(cache_root: Path) -> Path | None:
    found = sorted(cache_root.rglob("*.md"))
    return found[0] if found else None


def _relocate_batch_output(batch_dir: Path, pdf_path: Path, output_root: Path, backend: str) -> None:
    """Move ``batch_dir/{stem}/…`` into ``{stem}__slug/{stem}/…`` to match single-file cache layout."""
    stem = pdf_path.stem
    src = batch_dir / stem
    if not src.is_dir():
        raise RuntimeError(f"OCR batch: expected output directory missing for {pdf_path.name} (looked for {src})")
    dest_parent = ocr_cache_dir_for_backend(output_root, stem, backend)
    dest_parent.mkdir(parents=True, exist_ok=True)
    dest_sub = dest_parent / stem
    if dest_sub.exists():
        shutil.rmtree(dest_sub)
    shutil.move(str(src), str(dest_sub))


def batch_ensure_pdf_markdown(
    pdf_paths: list[Path],
    output_root: Path,
    backend: str = DEFAULT_OCR_BACKEND,
    *,
    lang: str = "en",
    batch_size: int | None = None,
) -> None:
    """
    For each PDF whose cache has no markdown yet, run MinerU in multi-file chunks.

    Safe for fast (single backend) and slow (multiple backends): call once per backend with the
    list of PDFs that still need that backend. Duplicate stems are never batched together.
    """
    if not pdf_paths or not MINERU_AVAILABLE:
        return

    output_root.mkdir(parents=True, exist_ok=True)
    bs = OCR_BATCH_SIZE if batch_size is None else max(1, int(batch_size))

    misses: list[Path] = []
    for p in pdf_paths:
        cache_root = ocr_cache_dir_for_backend(output_root, p.stem, backend)
        if _first_markdown_under(cache_root) is None:
            misses.append(p)
    if not misses:
        return

    by_stem: defaultdict[str, list[Path]] = defaultdict(list)
    for p in misses:
        by_stem[p.stem].append(p)

    work_chunks: list[list[Path]] = []
    for _stem, group in by_stem.items():
        if len(group) == 1:
            continue
        for p in group:
            work_chunks.append([p])
    singles: list[Path] = []
    for _stem, group in by_stem.items():
        if len(group) == 1:
            singles.append(group[0])
    for i in range(0, len(singles), bs):
        work_chunks.append(singles[i : i + bs])

    for chunk in work_chunks:
        batch_dir = Path(tempfile.mkdtemp(prefix="pf_ocr_", dir=str(output_root)))
        try:
            try:
                parse_doc(chunk, output_dir=batch_dir, backend=backend, lang=lang)
                for p in chunk:
                    _relocate_batch_output(batch_dir, p, output_root, backend)
            except Exception:
                if logger is not None:
                    logger.exception("OCR batch chunk failed ({} file(s)); will retry per file if needed)", len(chunk))
        finally:
            shutil.rmtree(batch_dir, ignore_errors=True)


def ocr_pdf_to_markdown(pdf_path: Path, output_root: Path, backend: str = DEFAULT_OCR_BACKEND) -> Path:
    if not MINERU_AVAILABLE:
        raise RuntimeError("MinerU is not installed. Run `uv sync --extra ocr` to enable PDF ingestion.")

    cache_root = ocr_cache_dir_for_backend(output_root, pdf_path.stem, backend)
    hit = _first_markdown_under(cache_root)
    if hit:
        return hit

    batch_ensure_pdf_markdown([pdf_path], output_root, backend=backend, batch_size=1)
    generated = _first_markdown_under(cache_root)
    if not generated:
        raise RuntimeError(f"OCR completed but no markdown was generated for {pdf_path.name}")
    return generated
