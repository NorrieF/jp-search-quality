#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-data/jp_search_quality.db}"
rm -f "$DB_PATH"
sqlite3 "$DB_PATH" < sql/schema.sql

echo "Created $DB_PATH"

sqlite3 "$DB_PATH" <<'SQL'
.mode csv
.import data/content_catalog.csv content_catalog
.import data/search_events.csv search_events
.import data/click_events.csv click_events

-- Delete header rows if they were imported as data
DELETE FROM content_catalog WHERE content_id='content_id';
DELETE FROM search_events WHERE event_id='event_id';
DELETE FROM click_events WHERE click_id='click_id';
SQL

echo "Imported data/content_catalog.csv, data/search_events.csv, data/click_events.csv"

