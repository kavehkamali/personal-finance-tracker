#!/usr/bin/env bash
# PersonalFinance — run server or clean generated data
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

CACHE_ROOT="${ROOT_DIR}/.cache"
UPLOAD_DIR="${CACHE_ROOT}/uploads"
OCR_DIR="${CACHE_ROOT}/ocr_output"
PROCESSED_DIR="${CACHE_ROOT}/processed"
SETTINGS_DIR="${ROOT_DIR}/data/settings"
INPUT_DIR="${ROOT_DIR}/input_statements"

ensure_tree() {
  mkdir -p "${INPUT_DIR}" "${UPLOAD_DIR}" "${OCR_DIR}" "${PROCESSED_DIR}" "${SETTINGS_DIR}"
  touch "${INPUT_DIR}/.gitkeep" "${SETTINGS_DIR}/.gitkeep"
}

clean_dir_contents() {
  local dir="$1"
  [[ -d "${dir}" ]] || return 0
  find "${dir}" -mindepth 1 ! -name '.gitkeep' -exec rm -rf {} + 2>/dev/null || true
}

cmd_clean() {
  local mode="${1:-cache}"
  case "${mode}" in
    cache)
      echo "Clearing .cache (uploads, OCR output, processed CSV)…"
      clean_dir_contents "${UPLOAD_DIR}"
      clean_dir_contents "${OCR_DIR}"
      clean_dir_contents "${PROCESSED_DIR}"
      echo "Done. input_statements/ was not touched."
      ;;
    all)
      cmd_clean cache
      echo "Clearing saved rules & merchant aliases…"
      clean_dir_contents "${SETTINGS_DIR}"
      echo "Done."
      ;;
    ocr)
      clean_dir_contents "${OCR_DIR}"
      echo "OCR cache cleared."
      ;;
    processed)
      clean_dir_contents "${PROCESSED_DIR}"
      echo "Processed outputs cleared."
      ;;
    rules)
      clean_dir_contents "${SETTINGS_DIR}"
      echo "Settings cleared."
      ;;
    *)
      cat <<'EOF'
Usage: scripts/dev.sh clean [cache|all|ocr|processed|rules]

  cache      Remove .cache/uploads, .cache/ocr_output, .cache/processed
  all        cache + data/settings (category rules, merchant aliases)
  ocr        OCR markdown cache only
  processed  transactions.csv + pipeline_meta only
  rules      settings JSON only

input_statements/ is never deleted.
EOF
      exit 1
      ;;
  esac
}

cmd_run() {
  ensure_tree
  if ! command -v uv >/dev/null 2>&1; then
    echo "uv is required but was not found in PATH."
    exit 1
  fi

  export UV_PROJECT_ENVIRONMENT="${ROOT_DIR}/.venv"

  echo "Syncing dependencies with uv…"
  if ! uv sync --extra ocr; then
    echo "OCR extra failed; installing core dependencies only."
    uv sync
  fi

  HOST="${HOST:-127.0.0.1}"
  PORT="${PORT:-8000}"

  echo "Starting PersonalFinance at http://${HOST}:${PORT}"
  exec uv run uvicorn personal_finance.app:app --app-dir src --host "${HOST}" --port "${PORT}" --reload
}

case "${1:-run}" in
  run)
    cmd_run
    ;;
  clean)
    shift || true
    cmd_clean "${1:-cache}"
    ;;
  *)
    cat <<'EOF'
Usage:
  scripts/dev.sh run              # sync deps and start uvicorn
  scripts/dev.sh clean [mode]    # see: scripts/dev.sh clean

Environment:
  HOST, PORT   (default 127.0.0.1:8000)
EOF
    exit 1
    ;;
esac
