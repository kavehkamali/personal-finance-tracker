import os
import re
from pathlib import Path

# Repository root (parent of `src/`)
ROOT_DIR = Path(__file__).resolve().parents[2]

# Local statement drop folder (gitignored; only .gitkeep tracked)
INPUT_STATEMENTS_DIR = ROOT_DIR / "input_statements"

# Ephemeral / regenerable outputs (gitignored)
CACHE_DIR = ROOT_DIR / ".cache"
UPLOAD_DIR = CACHE_DIR / "uploads"
OCR_OUTPUT_DIR = CACHE_DIR / "ocr_output"
PROCESSED_DIR = CACHE_DIR / "processed"

# User-editable JSON (gitignored contents; defaults created by app)
DATA_DIR = ROOT_DIR / "data"
SETTINGS_DIR = DATA_DIR / "settings"

TRANSACTIONS_CSV = PROCESSED_DIR / "transactions.csv"
PIPELINE_META_JSON = PROCESSED_DIR / "pipeline_meta.json"
CATEGORY_RULES_JSON = SETTINGS_DIR / "category_rules.json"
MERCHANT_ALIASES_JSON = SETTINGS_DIR / "merchant_aliases.json"
TRANSACTION_OVERRIDES_JSON = SETTINGS_DIR / "transaction_overrides.json"

APP_NAME = "PersonalFinance"
# MinerU VLM on Apple Silicon: vlm-mlx-engine (MLX, local GPU). See README "VLMs on Mac".
DEFAULT_OCR_BACKEND = "vlm-mlx-engine"
SUPPORTED_UPLOAD_SUFFIXES = {".pdf", ".md"}

# OCR ensemble: never | when_empty | always (when MinerU is installed)
# when_empty = multi-backend only if embedded PDF text yields no rows
OCR_ENSEMBLE_MODE = os.getenv("PF_OCR_ENSEMBLE", "when_empty").strip().lower()
# Mac-friendly default: local VLM (MLX) + classic pipeline for consensus. Override PF_OCR_BACKENDS
# to add e.g. vlm-transformers (HF/MPS, slower) on Apple Silicon or Intel without MLX.
OCR_ENSEMBLE_BACKENDS_RAW = os.getenv("PF_OCR_BACKENDS", "vlm-mlx-engine,pipeline")

# How many PDFs to pass to MinerU per ``do_parse`` call when filling OCR cache (same for fast/slow).
# Duplicate stems in one batch are split automatically. Set to 1 to disable cross-file batching.
def _ocr_batch_size() -> int:
    raw = os.getenv("PF_OCR_BATCH_SIZE", "16").strip()
    try:
        n = int(raw, 10)
    except ValueError:
        n = 16
    return max(1, n)


OCR_BATCH_SIZE = _ocr_batch_size()


def ocr_ensemble_backends_list() -> list[str]:
    return [x.strip() for x in OCR_ENSEMBLE_BACKENDS_RAW.split(",") if x.strip()]


_BACKEND_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


def sanitize_ocr_backend_id(value: str) -> str | None:
    t = value.strip()
    if not t or not _BACKEND_ID_RE.fullmatch(t):
        return None
    return t


def sanitize_backend_list(items: list[object] | None, *, max_backends: int = 8) -> list[str]:
    if not items:
        return []
    out: list[str] = []
    for x in items:
        if not isinstance(x, str):
            continue
        sid = sanitize_ocr_backend_id(x)
        if sid and sid not in out:
            out.append(sid)
        if len(out) >= max_backends:
            break
    return out


def resolve_extraction_runtime(
    preset: str | None,
    custom_backends: list[str] | None = None,
) -> tuple[list[str], str]:
    """
    Map UI preset to (ocr_backend_list, ensemble_mode).

    - fast: exactly one backend — classic MinerU ``pipeline`` (layout + OCR stack), mode
      ``when_empty`` so embedded PDF text is used when it yields rows (typical for bank PDFs);
      pipeline runs only when native extraction finds nothing. Never uses the multi-VLM list from
      PF_OCR_BACKENDS. (Terminal may still show several internal pipeline stages when OCR runs.)
    - slow: all backends in PF_OCR_BACKENDS, ensemble mode ``always`` (consensus / slow path).
    - custom: user-supplied backend IDs; ensemble mode follows PF_OCR_ENSEMBLE.
    """
    p = (preset or "slow").strip().lower()
    if p == "fast":
        return (["pipeline"], "when_empty")
    if p == "slow":
        return (ocr_ensemble_backends_list(), "always")
    if p == "custom":
        cleaned = sanitize_backend_list(custom_backends)
        if not cleaned:
            raise ValueError("Custom extraction requires at least one valid backend id.")
        return (cleaned, OCR_ENSEMBLE_MODE)
    return (ocr_ensemble_backends_list(), "always")


# Back-compat alias for code that still expects a "statements" name
STATEMENTS_DIR = INPUT_STATEMENTS_DIR
