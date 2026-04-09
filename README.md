# PersonalFinance

Local-first household finance dashboard for RBC statements: ingest PDFs or markdown, normalize transactions, reconcile internal transfers, auto-categorize, and explore an interactive dashboard.

## Quick start

```bash
./run.sh
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

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
- **Env:** `uv` (optional OCR extra: MinerU)

## Project layout

```text
.
├── input_statements/     # local inputs (gitignored)
├── .cache/               # regenerable outputs (gitignored)
├── data/settings/        # user JSON config (gitignored)
├── scripts/
│   └── dev.sh            # run | clean
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
