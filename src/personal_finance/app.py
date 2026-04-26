from __future__ import annotations

import threading
import subprocess
import sys
import re
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
from personal_finance.config import APP_NAME, INPUT_STATEMENTS_DIR, ROOT_DIR, ocr_ensemble_backends_list, resolve_extraction_runtime
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


AUDIT_DIR = INPUT_STATEMENTS_DIR / "statement_audit"
ANALYTICS_DIR = AUDIT_DIR / "analytics"


def _records_from_csv(path: Path) -> list[dict[str, object]]:
    if not path.is_file():
        return []
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return []
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")


def _first_record(path: Path) -> dict[str, object]:
    rows = _records_from_csv(path)
    return rows[0] if rows else {}


def _sum_rows(rows: list[dict[str, object]], key: str) -> float:
    total = 0.0
    for row in rows:
        try:
            total += float(row.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return round(total, 2)


def _output_check_by_month(spend_transactions: list[dict[str, object]]) -> list[dict[str, object]]:
    monthly: dict[str, dict[str, object]] = {}
    for row in spend_transactions:
        month = str(row.get("statement_month") or "")
        if not month:
            continue
        category = str(row.get("category") or "")
        description = str(row.get("description") or "").lower()
        if "principal" in category.lower() or "principal" in description or "credit line" in category.lower():
            continue
        current = monthly.setdefault(
            month,
            {
                "statement_month": month,
                "net_output": 0.0,
                "credit_card_output": 0.0,
                "bank_output": 0.0,
                "refunds": 0.0,
                "mortgage_home_line": 0.0,
                "loan_interest": 0.0,
                "transaction_count": 0,
            },
        )
        try:
            amount = float(row.get("amount") or 0)
        except (TypeError, ValueError):
            continue
        source = str(row.get("spend_source") or "")
        current["net_output"] = float(current["net_output"]) + amount
        current["transaction_count"] = int(current["transaction_count"]) + 1
        if amount < 0:
            current["refunds"] = float(current["refunds"]) + abs(amount)
        if source.startswith("credit_card"):
            current["credit_card_output"] = float(current["credit_card_output"]) + amount
        elif source.startswith("debit_bank"):
            current["bank_output"] = float(current["bank_output"]) + amount
        if category == "Mortgage Payments":
            current["mortgage_home_line"] = float(current["mortgage_home_line"]) + amount
        elif category == "Loan Interest":
            current["loan_interest"] = float(current["loan_interest"]) + amount

    rows = []
    for row in sorted(monthly.values(), key=lambda item: str(item["statement_month"])):
        row = row.copy()
        for key in (
            "net_output",
            "credit_card_output",
            "bank_output",
            "refunds",
            "mortgage_home_line",
            "loan_interest",
        ):
            row[key] = round(float(row[key]), 2)
        rows.append(row)
    return rows


def _recurring_label(description: object, category: object) -> str | None:
    text = " ".join(str(description or "").lower().split())
    cat = str(category or "")
    rules = (
        ("mortgage payment", "Mortgage payment"),
        ("term loan toyota finance", "Toyota finance"),
        ("loan interest", "Loan interest"),
        ("monthly fee", "Monthly fee"),
        ("bell mobility", "Bell Mobility"),
        ("reliance home comfort", "Reliance Home Comfort"),
        ("insurance cie belair", "Belair insurance"),
        ("insurance belair", "Belair insurance"),
        ("belair ins", "Belair insurance"),
        ("rbcins-life", "RBC life insurance"),
        ("rbcins", "RBC life insurance"),
        ("all life", "All Life insurance"),
        ("alectra", "Alectra"),
        ("enbridge", "Enbridge Gas"),
        ("hydro-quebec", "Hydro Quebec"),
        ("videotron", "Videotron"),
        ("netflix", "Netflix"),
        ("spotify", "Spotify"),
        ("apple.com/bill", "Apple billing"),
        ("google one", "Google One"),
        ("amazon.ca prime", "Amazon Prime"),
        ("brain power", "Brain Power"),
        ("townrichmondhill", "Town Richmond Hill"),
        ("act*townrichmondhill", "Town Richmond Hill"),
    )
    for needle, label in rules:
        if needle in text:
            return label
    if cat not in {
        "Mortgage Payments",
        "Auto Loan - Toyota",
        "Loan Interest",
        "Bank Fees & Interest",
        "Utilities & Insurance",
        "Kids Education & Activities",
        "Subscriptions & Digital",
    }:
        return None
    cleaned = re.sub(r"\b\d+([\.-]\d+)?\b", "", text)
    cleaned = re.sub(r"[^a-z& ]+", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned.title() if cleaned else None


def _recurring_payments(spend_transactions: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    if not spend_transactions:
        return {"summary": [], "transactions": []}

    df = pd.DataFrame(spend_transactions)
    if df.empty:
        return {"summary": [], "transactions": []}
    df["amount_num"] = pd.to_numeric(df.get("amount"), errors="coerce").fillna(0.0)
    df = df[df["amount_num"] > 0].copy()
    if df.empty:
        return {"summary": [], "transactions": []}

    df["recurring_label"] = [
        _recurring_label(desc, cat)
        for desc, cat in zip(df.get("description", ""), df.get("category", ""))
    ]
    df = df[df["recurring_label"].notna()].copy()
    if df.empty:
        return {"summary": [], "transactions": []}

    df["recurring_key"] = (
        df["category"].astype(str).str.lower().str.replace(r"[^a-z0-9]+", "-", regex=True).str.strip("-")
        + "::"
        + df["recurring_label"].astype(str).str.lower().str.replace(r"[^a-z0-9]+", "-", regex=True).str.strip("-")
    )
    months = sorted(df["statement_month"].astype(str).dropna().unique().tolist())
    summary_rows: list[dict[str, object]] = []
    kept_keys: set[str] = set()
    for key, grp in df.groupby("recurring_key", dropna=False):
        month_totals = grp.groupby("statement_month")["amount_num"].sum()
        months_seen = sorted(month_totals.index.astype(str).tolist())
        if len(months_seen) < 2:
            continue
        label = str(grp["recurring_label"].iloc[0])
        category = str(grp["category"].iloc[0])
        total_paid = float(grp["amount_num"].sum())
        avg_seen = total_paid / max(1, len(months_seen))
        missing = [month for month in months if month not in months_seen]
        possible_late = []
        for idx, month in enumerate(months[:-1]):
            next_month = months[idx + 1]
            if month in missing and float(month_totals.get(next_month, 0.0)) > avg_seen * 1.45:
                possible_late.append(f"{month}->{next_month}")
        transaction_dates = pd.to_datetime(grp.get("transaction_date"), errors="coerce").dropna().sort_values()
        if len(transaction_dates) >= 3:
            median_gap = float(transaction_dates.diff().dt.days.dropna().median())
            cadence = "biweekly/weekly" if median_gap <= 18 else "monthly"
        else:
            cadence = "monthly"
        expected_total = avg_seen * len(months) if cadence == "monthly" else total_paid
        variance = total_paid - expected_total
        values = [float(month_totals.get(month, 0.0)) for month in months_seen]
        high_variance = bool(values and min(values) > 0 and max(values) / min(values) > 1.6)
        notes = []
        if missing:
            notes.append("missing month")
        if possible_late:
            notes.append("possible late/catch-up")
        if high_variance:
            notes.append("variable amount")
        kept_keys.add(str(key))
        row = {
            "recurring_key": str(key),
            "label": label,
            "category": category,
            "cadence": cadence,
            "months_seen": ", ".join(months_seen),
            "missing_months": ", ".join(missing),
            "possible_late": ", ".join(possible_late),
            "transaction_count": int(len(grp)),
            "total_paid": round(total_paid, 2),
            "avg_paid_when_seen": round(avg_seen, 2),
            "expected_total_if_monthly": round(expected_total, 2),
            "variance_vs_expected": round(variance, 2),
            "notes": ", ".join(notes) if notes else "ok",
        }
        for month in months:
            row[month] = round(float(month_totals.get(month, 0.0)), 2)
        summary_rows.append(row)

    summary_rows = sorted(
        summary_rows,
        key=lambda row: (row["notes"] == "ok", -float(row["total_paid"])),
    )
    detail_cols = [
        "recurring_key",
        "recurring_label",
        "statement_month",
        "source_statement_month",
        "account_key",
        "spend_source",
        "transaction_date",
        "description",
        "amount",
        "category",
        "source_statement",
    ]
    details = df[df["recurring_key"].astype(str).isin(kept_keys)].copy()
    details["amount"] = details["amount_num"].map(lambda value: round(float(value), 2))
    details = details[[col for col in detail_cols if col in details.columns]]
    details = details.where(pd.notna(details), None)
    return {"summary": summary_rows, "transactions": details.to_dict(orient="records")}


def _safe_input_filename(name: str) -> str:
    raw = Path(name or "statement").name
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", raw).strip()
    return safe or "statement"


def _audit_dashboard_payload() -> dict[str, object]:
    account_summary = _records_from_csv(AUDIT_DIR / "account_summary.csv")
    account_month_coverage = _records_from_csv(AUDIT_DIR / "account_month_coverage.csv")
    file_inventory = _records_from_csv(AUDIT_DIR / "statement_file_inventory.csv")
    reconciliation = _records_from_csv(AUDIT_DIR / "statement_reconciliation.csv")

    all_spend_summary = _records_from_csv(ANALYTICS_DIR / "all_spend_category_summary.csv")
    all_spend_by_month = _records_from_csv(ANALYTICS_DIR / "all_spend_category_by_month.csv")
    all_spend_transactions = _records_from_csv(ANALYTICS_DIR / "all_spend_categorized_transactions.csv")
    all_spend_totals = _first_record(ANALYTICS_DIR / "all_spend_summary.csv")
    refund_by_month = _records_from_csv(ANALYTICS_DIR / "refunds_by_month.csv")
    refund_transactions = _records_from_csv(ANALYTICS_DIR / "refund_transactions.csv")

    credit_summary = _first_record(ANALYTICS_DIR / "credit_card_expense_summary.csv")
    credit_by_month = _records_from_csv(ANALYTICS_DIR / "credit_card_expenses_by_month.csv")
    income_check = _records_from_csv(ANALYTICS_DIR / "income_cash_in_check_by_month.csv")
    income_by_source = _records_from_csv(ANALYTICS_DIR / "income_cash_in_by_source.csv")
    output_check = _output_check_by_month(all_spend_transactions)
    recurring = _recurring_payments(all_spend_transactions)

    failed_reconciliation = [row for row in reconciliation if not bool(row.get("reconcile_ok"))]
    ignored_duplicate_files = [row for row in file_inventory if bool(row.get("ignored_duplicate_copy"))]
    exact_duplicate_files = [row for row in file_inventory if int(row.get("same_content_file_count") or 0) > 1]
    middle_gaps = [row for row in account_summary if bool(row.get("has_middle_gap"))]
    partial_or_missing = [row for row in account_month_coverage if row.get("status") != "full"]

    total_spend = float(all_spend_totals.get("total_expense") or _sum_rows(all_spend_summary, "total_expense") or 0)
    income_total = next((row for row in income_check if row.get("statement_month") == "TOTAL"), {})

    return {
        "paths": {
            "audit_dir": str(AUDIT_DIR),
            "analytics_dir": str(ANALYTICS_DIR),
        },
        "overview": {
            "account_count": len(account_summary),
            "middle_gap_count": len(middle_gaps),
            "failed_reconciliation_count": len(failed_reconciliation),
            "partial_or_missing_count": len(partial_or_missing),
            "ignored_duplicate_file_count": len(ignored_duplicate_files),
            "exact_duplicate_file_count": len(exact_duplicate_files),
            "total_spend": round(total_spend, 2),
            "external_spend": float(all_spend_totals.get("external_expense") or total_spend or 0),
            "credit_card_expense": float(all_spend_totals.get("credit_card_expense") or 0),
            "debit_bank_expense": float(all_spend_totals.get("debit_bank_expense") or 0),
            "months_included": str(all_spend_totals.get("months_included") or credit_summary.get("months_included") or ""),
            "lg_payroll": float(income_total.get("lg_payroll") or 0),
            "unity_final_payroll": float(income_total.get("unity_final_payroll") or 0),
            "ei_canada": float(income_total.get("ei_canada") or 0),
            "tracked_income": float(income_total.get("tracked_income") or 0),
            "external_cash_in": float(income_total.get("external_cash_in") or 0),
            "income_unmatched_external": float(income_total.get("tracked_income_vs_external_cash_in_diff") or 0),
        },
        "audit": {
            "account_summary": account_summary,
            "file_inventory": file_inventory,
            "ignored_duplicate_files": ignored_duplicate_files,
            "exact_duplicate_files": exact_duplicate_files,
            "account_month_coverage": account_month_coverage,
            "partial_or_missing": partial_or_missing,
            "middle_gaps": middle_gaps,
            "failed_reconciliation": failed_reconciliation,
        },
        "spend": {
            "summary": all_spend_summary,
            "by_month": all_spend_by_month,
            "transactions": all_spend_transactions,
            "totals": all_spend_totals,
        },
        "refunds": {
            "by_month": refund_by_month,
            "transactions": refund_transactions,
        },
        "credit_cards": {
            "summary": credit_summary,
            "by_month": credit_by_month,
        },
        "income": {
            "check_by_month": income_check,
            "by_source": income_by_source,
        },
        "output": {
            "check_by_month": output_check,
        },
        "recurring": recurring,
    }


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


@app.get("/api/audit-dashboard")
async def audit_dashboard() -> dict[str, object]:
    if not (AUDIT_DIR / "account_summary.csv").is_file() or not (ANALYTICS_DIR / "all_spend_category_summary.csv").is_file():
        raise HTTPException(status_code=404, detail="Audit reports not found. Run Refresh audit first.")
    return _audit_dashboard_payload()


@app.post("/api/audit-refresh")
async def audit_refresh() -> dict[str, object]:
    commands = [
        [sys.executable, str(ROOT_DIR / "scripts" / "statement_audit.py"), str(INPUT_STATEMENTS_DIR)],
        [sys.executable, str(ROOT_DIR / "scripts" / "statement_analytics.py")],
    ]
    output: list[str] = []
    for cmd in commands:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        output.append(completed.stdout)
        if completed.returncode != 0:
            raise HTTPException(status_code=500, detail="\n".join(output))
    payload = _audit_dashboard_payload()
    payload["refresh_output"] = "\n".join(output)
    return payload


@app.post("/api/audit-upload")
async def audit_upload(files: list[UploadFile] = File(...)) -> dict[str, object]:
    INPUT_STATEMENTS_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    for file in files:
        name = _safe_input_filename(file.filename or "statement")
        target = INPUT_STATEMENTS_DIR / name
        if target.exists():
            stem = target.stem
            suffix = target.suffix
            n = 1
            while target.exists():
                target = INPUT_STATEMENTS_DIR / f"{stem}-{n}{suffix}"
                n += 1
        target.write_bytes(await file.read())
        saved.append(target.name)
    return {"ok": True, "saved_files": saved}


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
