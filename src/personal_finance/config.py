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
DEFAULT_OCR_BACKEND = "vlm-mlx-engine"
SUPPORTED_UPLOAD_SUFFIXES = {".pdf", ".md"}

# Back-compat alias for code that still expects a "statements" name
STATEMENTS_DIR = INPUT_STATEMENTS_DIR
