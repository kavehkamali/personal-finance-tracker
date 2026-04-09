from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from personal_finance.config import DEFAULT_OCR_BACKEND

try:
    from loguru import logger
    from mineru.backend.pipeline.model_json_to_middle_json import result_to_middle_json as pipeline_result_to_middle_json
    from mineru.backend.pipeline.pipeline_analyze import doc_analyze as pipeline_doc_analyze
    from mineru.backend.pipeline.pipeline_middle_json_mkcontent import union_make as pipeline_union_make
    from mineru.backend.vlm.vlm_analyze import doc_analyze as vlm_doc_analyze
    from mineru.backend.vlm.vlm_middle_json_mkcontent import union_make as vlm_union_make
    from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2, prepare_env, read_fn
    from mineru.data.data_reader_writer import FileBasedDataWriter
    from mineru.utils.draw_bbox import draw_layout_bbox, draw_span_bbox
    from mineru.utils.enum_class import MakeMode

    MINERU_AVAILABLE = True
except ImportError:
    logger = None
    MINERU_AVAILABLE = False


def _process_output(
    pdf_info,
    pdf_bytes,
    pdf_file_name: str,
    local_md_dir: str,
    local_image_dir: str,
    md_writer,
    middle_json,
    model_output=None,
    is_pipeline: bool = True,
) -> None:
    image_dir = str(os.path.basename(local_image_dir))
    make_func = pipeline_union_make if is_pipeline else vlm_union_make
    md_content_str = make_func(pdf_info, MakeMode.MM_MD, image_dir)
    md_writer.write_string(f"{pdf_file_name}.md", md_content_str)
    md_writer.write_string(f"{pdf_file_name}_middle.json", json.dumps(middle_json, ensure_ascii=False, indent=2))
    if model_output is not None:
        md_writer.write_string(f"{pdf_file_name}_model.json", json.dumps(model_output, ensure_ascii=False, indent=2))
    if logger is not None:
        logger.info("OCR output written to {}", local_md_dir)


def parse_doc(path_list: list[Path], output_dir: Path, backend: str = DEFAULT_OCR_BACKEND, lang: str = "en") -> None:
    if not MINERU_AVAILABLE:
        raise RuntimeError("MinerU is not installed. Run `uv sync --extra ocr` to enable PDF OCR.")

    file_name_list: list[str] = []
    pdf_bytes_list: list[bytes] = []
    lang_list: list[str] = []

    for path in path_list:
        file_name_list.append(path.stem)
        pdf_bytes_list.append(read_fn(path))
        lang_list.append(lang)

    if backend == "pipeline":
        for idx, pdf_bytes in enumerate(pdf_bytes_list):
            pdf_bytes_list[idx] = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, 0, None)

        infer_results, all_image_lists, all_pdf_docs, out_lang_list, ocr_enabled_list = pipeline_doc_analyze(
            pdf_bytes_list,
            lang_list,
            parse_method="auto",
            formula_enable=True,
            table_enable=True,
        )

        for idx, model_list in enumerate(infer_results):
            model_json = copy.deepcopy(model_list)
            pdf_file_name = file_name_list[idx]
            local_image_dir, local_md_dir = prepare_env(output_dir, pdf_file_name, "auto")
            image_writer = FileBasedDataWriter(local_image_dir)
            md_writer = FileBasedDataWriter(local_md_dir)
            middle_json = pipeline_result_to_middle_json(
                model_list,
                all_image_lists[idx],
                all_pdf_docs[idx],
                image_writer,
                out_lang_list[idx],
                ocr_enabled_list[idx],
                True,
            )
            _process_output(
                middle_json["pdf_info"],
                pdf_bytes_list[idx],
                pdf_file_name,
                local_md_dir,
                local_image_dir,
                md_writer,
                middle_json,
                model_json,
                is_pipeline=True,
            )
        return

    resolved_backend = backend[4:] if backend.startswith("vlm-") else backend
    for idx, pdf_bytes in enumerate(pdf_bytes_list):
        pdf_file_name = file_name_list[idx]
        converted = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, 0, None)
        local_image_dir, local_md_dir = prepare_env(output_dir, pdf_file_name, "vlm")
        image_writer = FileBasedDataWriter(local_image_dir)
        md_writer = FileBasedDataWriter(local_md_dir)
        middle_json, infer_result = vlm_doc_analyze(converted, image_writer=image_writer, backend=resolved_backend, server_url=None)
        _process_output(
            middle_json["pdf_info"],
            converted,
            pdf_file_name,
            local_md_dir,
            local_image_dir,
            md_writer,
            middle_json,
            infer_result,
            is_pipeline=False,
        )


def ocr_pdf_to_markdown(pdf_path: Path, output_root: Path, backend: str = DEFAULT_OCR_BACKEND) -> Path:
    if not MINERU_AVAILABLE:
        raise RuntimeError("MinerU is not installed. Run `uv sync --extra ocr` to enable PDF ingestion.")

    existing = sorted((output_root / pdf_path.stem).rglob("*.md"))
    if existing:
        return existing[0]

    parse_doc([pdf_path], output_dir=output_root, backend=backend)
    generated = sorted((output_root / pdf_path.stem).rglob("*.md"))
    if not generated:
        raise RuntimeError(f"OCR completed but no markdown was generated for {pdf_path.name}")
    return generated[0]
