#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-data/jp_search_quality.db}"

echo "== JP Search Quality Pipeline =="
echo "DB: $DB_PATH"
echo

echo "== Step 1: Generate synthetic CSV data =="
./scripts/generate_data.sh "$DB_PATH"
echo

echo "== Step 2: Create SQLite DB + import CSVs =="
./scripts/create_db.sh "$DB_PATH"
echo

echo "== Step 3: Build metrics + export CSV outputs =="
./scripts/export_metrics.sh "$DB_PATH"
echo

echo "== Step 4: Generate figures =="
python3 scripts/make_figures.py
echo

echo "✅ Done."
echo "Outputs:"
echo "- DB: $DB_PATH"
echo "- Metrics CSVs: reports/metrics_outputs/"
echo "- Figures: reports/figures/"
