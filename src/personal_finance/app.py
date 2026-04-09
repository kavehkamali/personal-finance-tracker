from __future__ import annotations

import threading
from pathlib import Path
from typing import Annotated

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
from personal_finance.config import APP_NAME
from personal_finance.jobs import create_job, get_job, make_progress_callback, update_job
from personal_finance.pipeline import load_pipeline_meta, load_transactions, rebuild_dataset, save_uploaded_files
from personal_finance.transaction_overrides import merge_transaction_overrides


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.on_event("startup")
def startup_rebuild() -> None:
    rebuild_dataset()


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
    include_internal: bool = Query(default=False),
    exclude_category: Annotated[list[str], Query(description="Repeat param to hide categories from totals")] = [],
    exclude_necessity: Annotated[list[str], Query()] = [],
    exclude_beneficiary: Annotated[list[str], Query()] = [],
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
        include_internal=include_internal,
        exclude_categories=exclude_category or None,
        exclude_necessities=exclude_necessity or None,
        exclude_beneficiaries=exclude_beneficiary or None,
    )


@app.get("/api/transactions")
async def transactions(
    owner: str | None = Query(default=None),
    account: str | None = Query(default=None),
    category: str | None = Query(default=None),
    necessity: str | None = Query(default=None),
    beneficiary: str | None = Query(default=None),
    month: str | None = Query(default=None),
    include_internal: bool = Query(default=False),
    exclude_category: Annotated[list[str], Query()] = [],
    exclude_necessity: Annotated[list[str], Query()] = [],
    exclude_beneficiary: Annotated[list[str], Query()] = [],
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
        include_internal=include_internal,
        exclude_categories=exclude_category or None,
        exclude_necessities=exclude_necessity or None,
        exclude_beneficiaries=exclude_beneficiary or None,
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
    ]
    cols = [c for c in cols if c in out.columns]
    if "internal_detection" in out.columns and "internal_detection" not in cols:
        cols.append("internal_detection")
    return {"transactions": out[cols].round(2).to_dict(orient="records")}


@app.post("/api/upload")
async def upload(files: list[UploadFile] = File(...)) -> dict:
    payloads: list[tuple[str, bytes]] = []
    for file in files:
        payloads.append((file.filename or "statement", await file.read()))
    job_id = create_job("upload")
    update_job(job_id, status="running", stage="saving", message="Saving uploaded files", progress=0.03)

    def worker() -> None:
        try:
            saved = save_uploaded_files(payloads)
            update_job(
                job_id,
                status="running",
                stage="saved",
                message="Files saved",
                progress=0.08,
                total_files=len(saved),
            )
            pipeline_meta = rebuild_dataset(progress_callback=make_progress_callback(job_id))
            summary_payload = build_dashboard_payload(load_transactions(), load_pipeline_meta())
            summary_payload["saved_files"] = [path.name for path in saved]
            summary_payload["meta"] = {**summary_payload["meta"], **pipeline_meta}
            update_job(
                job_id,
                status="complete",
                stage="complete",
                message="Finished processing",
                progress=1.0,
                result=jsonable_encoder(summary_payload),
            )
        except Exception as exc:  # pragma: no cover - UI job path
            update_job(job_id, status="error", stage="error", message="Processing failed", error=str(exc))

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id}


@app.post("/api/reload")
async def reload_data() -> dict:
    """Rebuild transactions from statements; client should refetch /api/summary with current filters."""
    rebuild_dataset()
    return {"status": "ok"}


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
