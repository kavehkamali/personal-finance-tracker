from __future__ import annotations

import threading
from pathlib import Path

from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from personal_finance.analytics import apply_filters, build_dashboard_payload
from personal_finance.categories import BENEFICIARY_OPTIONS, CATEGORY_OPTIONS, NECESSITY_OPTIONS, load_category_rules, save_category_rules
from personal_finance.config import APP_NAME
from personal_finance.jobs import create_job, get_job, make_progress_callback, update_job
from personal_finance.pipeline import load_pipeline_meta, load_transactions, rebuild_dataset, save_uploaded_files


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
    )
    if filtered.empty:
        return {"transactions": []}

    out = filtered.sort_values("transaction_date", ascending=False).head(200).copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")
    out["posting_date"] = out["posting_date"].dt.strftime("%Y-%m-%d")
    return {
        "transactions": out[
            [
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
            ]
        ].round(2).to_dict(orient="records")
    }


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
