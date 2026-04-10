from __future__ import annotations

import threading
from pathlib import Path
from typing import Annotated

import pandas as pd
from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from personal_finance.analytics import apply_filters, build_dashboard_payload
from personal_finance.categories import (
    BENEFICIARY_OPTIONS,
    CATEGORY_OPTIONS,
    NECESSITY_OPTIONS,
    _normalize_rule,
    load_category_rules,
    normalize_text,
    save_category_rules,
)
from personal_finance.config import APP_NAME, ocr_ensemble_backends_list, resolve_extraction_runtime
from personal_finance.jobs import create_job, get_job, make_progress_callback, update_job
from personal_finance.pipeline import (
    clear_regenerable_cache,
    ensure_directories,
    load_pipeline_meta,
    load_transactions,
    rebuild_dataset,
    save_uploaded_files,
)
from personal_finance.transaction_overrides import merge_transaction_overrides


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def _sanitize_list_search(q: str | None, max_len: int = 200) -> str | None:
    if q is None:
        return None
    t = str(q).strip()
    return t[:max_len] if t else None


@app.on_event("startup")
def startup_init() -> None:
    # Do not run MinerU/OCR here — that was firing "Predict" on every server start. Use
    # "Extract transactions" or "Refresh data" after uploads, or process on first API request.
    ensure_directories()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html", {"app_name": APP_NAME})


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/summary")
async def summary(
    owner: str | None = Query(default=None),
    account: str | None = Query(default=None),
    category: str | None = Query(default=None),
    necessity: str | None = Query(default=None),
    beneficiary: str | None = Query(default=None),
    month: str | None = Query(default=None, description="Statement month key e.g. 2025-12"),
    year: int | None = Query(default=None, ge=1990, le=2100, description="Calendar year (all months in year when month is not set)"),
    include_internal: bool = Query(default=False),
    exclude_category: Annotated[list[str], Query(description="Repeat param to hide categories from totals")] = [],
    exclude_necessity: Annotated[list[str], Query()] = [],
    exclude_beneficiary: Annotated[list[str], Query()] = [],
    q: str | None = Query(default=None, description="Case-insensitive substring on category, merchant, or description"),
) -> dict:
    transactions = load_transactions()
    meta = load_pipeline_meta()
    return build_dashboard_payload(
        transactions,
        meta,
        owner=owner,
        account=account,
        category=category,
        necessity=necessity,
        beneficiary=beneficiary,
        month=month,
        year=year,
        include_internal=include_internal,
        exclude_categories=exclude_category or None,
        exclude_necessities=exclude_necessity or None,
        exclude_beneficiaries=exclude_beneficiary or None,
        text_search=_sanitize_list_search(q),
    )


@app.get("/api/transactions")
async def transactions(
    owner: str | None = Query(default=None),
    account: str | None = Query(default=None),
    category: str | None = Query(default=None),
    necessity: str | None = Query(default=None),
    beneficiary: str | None = Query(default=None),
    month: str | None = Query(default=None),
    year: int | None = Query(default=None, ge=1990, le=2100),
    include_internal: bool = Query(default=False),
    exclude_category: Annotated[list[str], Query()] = [],
    exclude_necessity: Annotated[list[str], Query()] = [],
    exclude_beneficiary: Annotated[list[str], Query()] = [],
    q: str | None = Query(default=None, description="Substring on category, merchant, or description"),
) -> dict:
    df = load_transactions()
    filtered = apply_filters(
        df,
        owner=owner,
        account=account,
        category=category,
        necessity=necessity,
        beneficiary=beneficiary,
        month=month,
        year=year,
        include_internal=include_internal,
        exclude_categories=exclude_category or None,
        exclude_necessities=exclude_necessity or None,
        exclude_beneficiaries=exclude_beneficiary or None,
        text_search=_sanitize_list_search(q),
    )
    if filtered.empty:
        return {"transactions": []}

    out = filtered.sort_values("transaction_date", ascending=False).head(200).copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")
    out["posting_date"] = out["posting_date"].dt.strftime("%Y-%m-%d")
    cols = [
        "transaction_date",
        "posting_date",
        "owner",
        "account_label",
        "merchant",
        "description",
        "category",
        "necessity",
        "beneficiary",
        "flow_type",
        "amount",
        "expense_amount",
        "internal_match_status",
        "tx_key",
        "category_source",
        "extraction_confidence",
        "extraction_sources",
        "extraction_disagreement",
    ]
    cols = [c for c in cols if c in out.columns]
    if "internal_detection" in out.columns and "internal_detection" not in cols:
        cols.append("internal_detection")
    sub = out[cols].copy()
    for key in ("amount", "expense_amount"):
        if key in sub.columns:
            sub[key] = pd.to_numeric(sub[key], errors="coerce").round(2)
    if "extraction_confidence" in sub.columns:
        sub["extraction_confidence"] = pd.to_numeric(sub["extraction_confidence"], errors="coerce").round(3)
    return {"transactions": sub.to_dict(orient="records")}


def _extraction_from_request_body(payload: object) -> tuple[list[str], str]:
    data = payload if isinstance(payload, dict) else {}
    raw_preset = data.get("preset")
    preset = raw_preset.strip().lower() if isinstance(raw_preset, str) else None
    raw_backends = data.get("backends")
    backends: list[str] | None = None
    if isinstance(raw_backends, list):
        backends = [str(x) for x in raw_backends if isinstance(x, (str, int, float))]
    try:
        return resolve_extraction_runtime(preset, backends)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _start_process_job(
    ocr_backends: list[str],
    ocr_ensemble_mode: str,
    *,
    request_preset: str | None = None,
) -> str:
    """Parse all discovered statements (including OCR ensemble); reports progress to the job."""

    job_id = create_job("process")

    def worker() -> None:
        try:
            update_job(
                job_id,
                status="running",
                stage="starting",
                message="Starting extraction…",
                progress=0.04,
                extraction_preset=request_preset,
                extraction_backends=list(ocr_backends),
                extraction_ensemble_mode=ocr_ensemble_mode,
            )
            pipeline_meta = rebuild_dataset(
                progress_callback=make_progress_callback(job_id),
                ocr_backends=ocr_backends,
                ocr_ensemble_mode=ocr_ensemble_mode,
            )
            summary_payload = build_dashboard_payload(load_transactions(), load_pipeline_meta())
            summary_payload["meta"] = {**summary_payload["meta"], **pipeline_meta}
            update_job(
                job_id,
                status="complete",
                stage="complete",
                message="Finished processing",
                progress=1.0,
                ocr_backend=None,
                ocr_backend_index=None,
                ocr_backends_total=None,
                result=jsonable_encoder(summary_payload),
            )
        except Exception as exc:  # pragma: no cover - UI job path
            update_job(job_id, status="error", stage="error", message="Processing failed", error=str(exc))

    threading.Thread(target=worker, daemon=True).start()
    return job_id


@app.post("/api/clear-cache")
async def clear_cache() -> dict[str, object]:
    """Delete cached uploads, OCR output, and processed CSV/meta under ``.cache/``."""
    summary = clear_regenerable_cache()
    return {"ok": True, **summary}


@app.post("/api/upload")
async def upload(files: list[UploadFile] = File(...)) -> dict:
    payloads: list[tuple[str, bytes]] = []
    for file in files:
        payloads.append((file.filename or "statement", await file.read()))
    job_id = create_job("upload")
    update_job(job_id, status="running", stage="saving", message="Saving uploaded files", progress=0.15)

    def worker() -> None:
        try:
            saved = save_uploaded_files(payloads)
            update_job(
                job_id,
                status="complete",
                stage="saved",
                message="Files saved — run extraction when ready",
                progress=1.0,
                total_files=len(saved),
                result=jsonable_encoder(
                    {
                        "upload_only": True,
                        "saved_files": [path.name for path in saved],
                    }
                ),
            )
        except Exception as exc:  # pragma: no cover - UI job path
            update_job(job_id, status="error", stage="error", message="Upload failed", error=str(exc))

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id}


@app.get("/api/extraction-options")
async def extraction_options() -> dict:
    """Preset → backends + ensemble mode (for UI labels). Env controls the slow preset list."""
    fast_b, fast_m = resolve_extraction_runtime("fast")
    slow_b, slow_m = resolve_extraction_runtime("slow")
    return {
        "env_default_backends": ocr_ensemble_backends_list(),
        "presets": {
            "fast": {
                "backends": fast_b,
                "ensemble_mode": fast_m,
                "label": "Fast",
                "description": "Single MinerU classic pipeline (one backend). Ignores PF_OCR_BACKENDS multi-model list.",
            },
            "slow": {
                "backends": slow_b,
                "ensemble_mode": slow_m,
                "label": "Slow (full)",
                "description": "All models in your PF_OCR_BACKENDS list, every time OCR runs (best consensus).",
            },
        },
    }


@app.post("/api/process-statements")
async def process_statements(payload: dict = Body(default_factory=dict)) -> dict:
    """Rebuild transactions from all PDFs/markdown under input_statements/ and uploads/."""
    backs, mode = _extraction_from_request_body(payload)
    raw = payload.get("preset")
    req_preset = raw.strip().lower() if isinstance(raw, str) else None
    job_id = _start_process_job(backs, mode, request_preset=req_preset)
    return {
        "job_id": job_id,
        "extraction": {
            "request_preset": req_preset,
            "backends": backs,
            "ensemble_mode": mode,
        },
    }


@app.post("/api/reload")
async def reload_data(payload: dict = Body(default_factory=dict)) -> dict:
    """Same as POST /api/process-statements (async job; poll /api/jobs/{id})."""
    backs, mode = _extraction_from_request_body(payload)
    raw = payload.get("preset")
    req_preset = raw.strip().lower() if isinstance(raw, str) else None
    job_id = _start_process_job(backs, mode, request_preset=req_preset)
    return {
        "job_id": job_id,
        "extraction": {
            "request_preset": req_preset,
            "backends": backs,
            "ensemble_mode": mode,
        },
    }


@app.get("/api/jobs/{job_id}")
async def job_status(job_id: str) -> dict:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/category-rules")
async def category_rules() -> dict:
    return {
        "rules": load_category_rules(),
        "category_options": CATEGORY_OPTIONS,
        "necessity_options": NECESSITY_OPTIONS,
        "beneficiary_options": BENEFICIARY_OPTIONS,
    }


@app.post("/api/transaction-corrections")
async def apply_transaction_corrections(payload: dict = Body(...)) -> dict:
    """Persist per-transaction category overrides and optionally prepend keyword rules; rebuilds dataset."""
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Provide a non-empty items array")

    rules = load_category_rules()
    rules_inserted = 0
    seen_keywords: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if not item.get("add_rule"):
            continue
        kw = normalize_text(str(item.get("add_rule_keyword", ""))).lower()
        if len(kw) < 2 or kw in seen_keywords:
            continue
        seen_keywords.add(kw)
        candidate = {
            "keyword": kw,
            "category": str(item.get("category", "Other")),
            "necessity": str(item.get("necessity", "Auto")),
            "beneficiary": str(item.get("beneficiary", "Auto")),
        }
        normalized = _normalize_rule(candidate)
        if normalized:
            rules.insert(0, normalized)
            rules_inserted += 1
    if rules_inserted:
        save_category_rules(rules)

    updates: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        tx_key = str(item.get("tx_key", "")).strip()
        if not tx_key:
            continue
        updates.append(
            {
                "tx_key": tx_key,
                "category": item.get("category"),
                "necessity": item.get("necessity", "Auto"),
                "beneficiary": item.get("beneficiary", "Auto"),
            }
        )
    if updates:
        merge_transaction_overrides(updates)

    rebuild_dataset()
    summary_payload = build_dashboard_payload(load_transactions(), load_pipeline_meta())
    return {
        "status": "ok",
        "rules_inserted": rules_inserted,
        "summary": jsonable_encoder(summary_payload),
    }


@app.post("/api/category-rules")
async def update_category_rules(payload: dict = Body(...)) -> dict:
    rules = payload.get("rules", [])
    saved_rules = save_category_rules(rules)
    rebuild_dataset()
    summary_payload = build_dashboard_payload(load_transactions(), load_pipeline_meta())
    return {
        "rules": saved_rules,
        "category_options": CATEGORY_OPTIONS,
        "necessity_options": NECESSITY_OPTIONS,
        "beneficiary_options": BENEFICIARY_OPTIONS,
        "summary": summary_payload,
    }
