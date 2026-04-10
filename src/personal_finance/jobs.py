from __future__ import annotations

import threading
import uuid
from typing import Any, Callable


_JOBS: dict[str, dict[str, Any]] = {}
_LOCK = threading.Lock()


def create_job(kind: str) -> str:
    job_id = str(uuid.uuid4())
    with _LOCK:
        _JOBS[job_id] = {
            "id": job_id,
            "kind": kind,
            "status": "queued",
            "stage": "queued",
            "message": "Queued",
            "progress": 0.0,
            "current_file": None,
            "processed_files": 0,
            "total_files": 0,
            "ocr_backend": None,
            "ocr_backend_index": None,
            "ocr_backends_total": None,
            "extraction_preset": None,
            "extraction_backends": None,
            "extraction_ensemble_mode": None,
            "result": None,
            "error": None,
        }
    return job_id


def update_job(job_id: str, **updates: Any) -> None:
    with _LOCK:
        if job_id not in _JOBS:
            return
        _JOBS[job_id].update(updates)


def get_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        job = _JOBS.get(job_id)
        return dict(job) if job else None


def make_progress_callback(job_id: str) -> Callable[[dict[str, Any]], None]:
    def callback(payload: dict[str, Any]) -> None:
        update_job(
            job_id,
            status="running",
            stage=payload.get("stage", "running"),
            message=payload.get("message", "Processing"),
            progress=payload.get("progress", 0.0),
            current_file=payload.get("current_file"),
            processed_files=payload.get("processed_files", 0),
            total_files=payload.get("total_files", 0),
            ocr_backend=payload.get("ocr_backend"),
            ocr_backend_index=payload.get("ocr_backend_index"),
            ocr_backends_total=payload.get("ocr_backends_total"),
        )

    return callback
