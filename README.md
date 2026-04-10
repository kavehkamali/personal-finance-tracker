# PersonalFinance

Local-first household finance dashboard for RBC statements: ingest PDFs or markdown, normalize transactions, reconcile internal transfers, auto-categorize, and explore an interactive dashboard.

## Quick start

```bash
./run.sh
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

Upload saves PDFs/markdown locally first; pick **Fast** (MinerU `pipeline` only when embedded text yields no rows — ignores `PF_OCR_BACKENDS`) or **Slow (full)** (every backend in `PF_OCR_BACKENDS`, mode `always`). **Refresh data** uses the same preset (stored in the browser).

Equivalent: `scripts/dev.sh run`

## Where things live

| Path | Purpose |
|------|---------|
| `input_statements/` | **Your** PDFs / markdown (gitignored). Drop files here or use the web upload. |
| `.cache/uploads/` | Copies of files uploaded through the UI (gitignored). |
| `.cache/ocr_output/` | MinerU / OCR markdown + assets (gitignored). |
| `.cache/processed/` | `transactions.csv`, `pipeline_meta.json` — rebuilt by the pipeline (gitignored). |
| `data/settings/` | `category_rules.json`, `merchant_aliases.json` — created on first run (gitignored). |

On first run after this layout, the app **migrates** any old `data/uploads`, `data/ocr_output`, or `data/processed` into `.cache/` if `.cache` is still empty.

## Cleaning generated data

```bash
./clean.sh cache    # wipe .cache (uploads, OCR, processed) — keeps input_statements/
./clean.sh all      # also removes data/settings (rules + merchant aliases)
./clean.sh ocr      # OCR cache only
./clean.sh processed
./clean.sh rules
```

Same as `scripts/dev.sh clean …`.

## Stack

- **Backend:** FastAPI  
- **Frontend:** Jinja + vanilla JS + Plotly  
- **Env:** `uv` (optional OCR extra: MinerU 3 with `pipeline` + `vlm`; on macOS also `mlx` for `vlm-mlx-engine`)

## VLMs on Mac (MinerU)

Statement PDFs can use **local** MinerU backends—no cloud LLM API for OCR.

| Your Mac | Recommended backends | Notes |
|----------|----------------------|--------|
| **Apple Silicon (M1/M2/M3/…)** | `vlm-mlx-engine` (default) | MLX VLM on the Neural Engine / GPU; typically fastest local path. Needs recent macOS (MinerU docs often cite 13.5+). |
| Same + stronger consensus | `vlm-mlx-engine,pipeline` | **Default** `PF_OCR_BACKENDS`: VLM + classic pipeline merged for agreement stats. |
| Optional second VLM | add `vlm-transformers` | Hugging Face Transformers; can use MPS on Apple Silicon or CPU on Intel—slower than MLX. |
| **Intel Mac** | `pipeline` and/or `vlm-transformers` | MLX VLM is not for Intel; use classic pipeline plus optional `vlm-transformers` (CPU/MPS if available). |

Examples:

```bash
# Default Mac setup (already the app default when MinerU is installed)
export PF_OCR_BACKENDS="vlm-mlx-engine,pipeline"
export PF_OCR_ENSEMBLE=when_empty   # or always for every PDF

uv sync --extra ocr
./run.sh
```

Install the OCR extra (`uv sync --extra ocr` or `./scripts/dev.sh run`) so MinerU, PyTorch/transformers (pipeline + `vlm-transformers`), and on **macOS** MLX (`vlm-mlx-engine`) are present. First runs download model weights to MinerU’s cache (several GB for pipeline).

**Smoke-test local backends** (after `uv sync --extra ocr`):

```bash
.venv/bin/python scripts/test_ocr_backends_mac.py
```

On Apple Silicon this verifies `pipeline`, `vlm-mlx-engine`, and `vlm-transformers` end-to-end on a tiny PDF. Linux installs omit the `mlx` extra; use `pipeline` and `vlm-transformers` there.

## Project layout

```text
.
├── input_statements/     # local inputs (gitignored)
├── .cache/               # regenerable outputs (gitignored)
├── data/settings/        # user JSON config (gitignored)
├── scripts/
│   ├── dev.sh            # run | clean
│   └── test_ocr_backends_mac.py  # optional MinerU backend smoke test
├── src/personal_finance/
│   ├── app.py
│   ├── analytics.py
│   ├── categories.py
│   ├── config.py
│   ├── merchant_aliases.py
│   ├── ocr.py
│   ├── pipeline.py
│   ├── parsers/
│   ├── static/
│   └── templates/
├── run.sh                # → scripts/dev.sh run
├── clean.sh              # → scripts/dev.sh clean
├── pyproject.toml
└── README.md
```

## Internal transfers

Rows that look like payments, transfers, bill pay, or e-transfers are matched by amount, direction, date proximity, and reference. Matched pairs are excluded from spend totals by default; unmatched candidates stay visible.

## Notes

- `input_statements/` is ignored by git except `.gitkeep` so the folder exists in fresh clones.
- Do not commit personal PDFs or derived CSVs; keep them local or use your own backup.
