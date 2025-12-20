#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-data/jp_search_quality.db}"
OUT_DIR="reports/metrics_outputs"

mkdir -p "$OUT_DIR"

echo "Exporting metrics from: $DB_PATH"
echo "Output directory: $OUT_DIR"

# Export helper
export_table () {
  local table="$1"
  local out="$OUT_DIR/${table}.csv"
  sqlite3 -header -csv "$DB_PATH" "SELECT * FROM ${table};" > "$out"
  echo "Wrote $out"
}

# Build metrics tables (idempotent)
sqlite3 "$DB_PATH" < sql/metrics_build.sql
echo "Built metrics tables."

# Export tables
export_table "m_overall"
export_table "m_by_vertical"
export_table "m_by_device"
export_table "m_by_script_flags"
export_table "m_by_len_bucket"
export_table "m_top_bad_queries"
export_table "m_reformulation_drivers"
export_table "m_daily"

# Optional: export a small sample of the joined view for debugging
sqlite3 -header -csv "$DB_PATH" "SELECT * FROM v_search_scored LIMIT 200;" > "$OUT_DIR/sample_v_search_scored.csv"
echo "Wrote $OUT_DIR/sample_v_search_scored.csv"

echo "Done."
