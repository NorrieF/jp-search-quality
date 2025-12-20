-- sql/metrics_build.sql
-- Build search quality metric tables for JP Search Quality (Project 1)

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- Base views
-- ------------------------------------------------------------
DROP VIEW IF EXISTS v_search_with_click;
CREATE VIEW v_search_with_click AS
SELECT
  se.event_id,
  se.ts,
  se.user_id,
  se.session_id,
  se.locale,
  se.device,
  se.query_raw,
  se.query_norm,
  se.vertical,
  se.results_count,
  se.has_kanji,
  se.has_kana,
  se.has_romaji,
  se.has_halfwidth_kana,
  se.query_len,

  CASE WHEN COUNT(ce.click_id) > 0 THEN 1 ELSE 0 END AS has_click,
  MIN(ce.rank) AS best_rank,
  MAX(ce.dwell_sec) AS max_dwell_sec
FROM search_events se
LEFT JOIN click_events ce
  ON ce.event_id = se.event_id
GROUP BY se.event_id;

DROP VIEW IF EXISTS v_search_scored;
CREATE VIEW v_search_scored AS
SELECT
  *,
  CASE WHEN has_click = 1 AND COALESCE(max_dwell_sec, 0) >= 30 THEN 1 ELSE 0 END AS sat_click,
  CASE WHEN results_count = 0 THEN 1 ELSE 0 END AS is_zero_results,
  CASE WHEN results_count > 0 AND has_click = 0 THEN 1 ELSE 0 END AS is_no_click_with_results
FROM v_search_with_click;

DROP VIEW IF EXISTS v_session_seq;
CREATE VIEW v_session_seq AS
SELECT
  *,
  LAG(query_norm) OVER (PARTITION BY session_id ORDER BY ts) AS prev_query_norm,
  LAG(results_count) OVER (PARTITION BY session_id ORDER BY ts) AS prev_results_count,
  LAG(has_click) OVER (PARTITION BY session_id ORDER BY ts) AS prev_has_click
FROM v_search_scored;

DROP VIEW IF EXISTS v_session_seq_flags;
CREATE VIEW v_session_seq_flags AS
SELECT
  *,
  CASE
    WHEN prev_query_norm IS NULL THEN 0
    WHEN prev_query_norm <> query_norm THEN 1
    ELSE 0
  END AS is_reformulation
FROM v_session_seq;

-- ------------------------------------------------------------
-- Metric tables
-- ------------------------------------------------------------

-- Overall
DROP TABLE IF EXISTS m_overall;
CREATE TABLE m_overall AS
SELECT
  'overall' AS segment,
  COUNT(*) AS searches,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS ctr_pct,
  ROUND(AVG(is_no_click_with_results) * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(AVG(sat_click) * 100.0, 2) AS sat_click_rate_pct,
  ROUND(AVG(is_reformulation) * 100.0, 2) AS reformulation_rate_pct
FROM v_session_seq_flags;

-- By vertical
DROP TABLE IF EXISTS m_by_vertical;
CREATE TABLE m_by_vertical AS
SELECT
  vertical,
  COUNT(*) AS searches,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS ctr_pct,
  ROUND(AVG(is_no_click_with_results) * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(AVG(sat_click) * 100.0, 2) AS sat_click_rate_pct,
  ROUND(AVG(is_reformulation) * 100.0, 2) AS reformulation_rate_pct
FROM v_session_seq_flags
GROUP BY vertical;

-- By device
DROP TABLE IF EXISTS m_by_device;
CREATE TABLE m_by_device AS
SELECT
  device,
  COUNT(*) AS searches,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS ctr_pct,
  ROUND(AVG(is_no_click_with_results) * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(AVG(sat_click) * 100.0, 2) AS sat_click_rate_pct,
  ROUND(AVG(is_reformulation) * 100.0, 2) AS reformulation_rate_pct
FROM v_session_seq_flags
GROUP BY device;

-- By script flags (JP-specific)
DROP TABLE IF EXISTS m_by_script_flags;
CREATE TABLE m_by_script_flags AS
SELECT
  has_kanji,
  has_kana,
  has_romaji,
  has_halfwidth_kana,
  COUNT(*) AS searches,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS ctr_pct,
  ROUND(AVG(is_no_click_with_results) * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(AVG(sat_click) * 100.0, 2) AS sat_click_rate_pct
FROM v_session_seq_flags
GROUP BY has_kanji, has_kana, has_romaji, has_halfwidth_kana;

-- Query length buckets
DROP TABLE IF EXISTS m_by_len_bucket;
CREATE TABLE m_by_len_bucket AS
WITH bucketed AS (
  SELECT
    *,
    CASE
      WHEN query_len <= 3 THEN 'len<=3'
      WHEN query_len BETWEEN 4 AND 6 THEN 'len4-6'
      WHEN query_len BETWEEN 7 AND 10 THEN 'len7-10'
      ELSE 'len>10'
    END AS len_bucket
  FROM v_session_seq_flags
)
SELECT
  len_bucket,
  COUNT(*) AS searches,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS ctr_pct,
  ROUND(AVG(is_no_click_with_results) * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(AVG(sat_click) * 100.0, 2) AS sat_click_rate_pct,
  ROUND(AVG(is_reformulation) * 100.0, 2) AS reformulation_rate_pct
FROM bucketed
GROUP BY len_bucket;

-- Top problematic normalized queries (enough volume + high failure)
DROP TABLE IF EXISTS m_top_bad_queries;
CREATE TABLE m_top_bad_queries AS
WITH q AS (
  SELECT
    query_norm,
    COUNT(*) AS searches,
    AVG(is_zero_results) AS zr,
    AVG(is_no_click_with_results) AS ncr,
    AVG(sat_click) AS scr
  FROM v_session_seq_flags
  GROUP BY query_norm
)
SELECT
  query_norm,
  searches,
  ROUND(zr * 100.0, 2) AS zero_results_rate_pct,
  ROUND(ncr * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(scr * 100.0, 2) AS sat_click_rate_pct
FROM q
WHERE searches >= 50
ORDER BY (zr * 0.7 + ncr * 0.3) DESC, searches DESC
LIMIT 50;

-- Reformulation drivers: what tends to happen BEFORE a reformulation?
DROP TABLE IF EXISTS m_reformulation_drivers;
CREATE TABLE m_reformulation_drivers AS
SELECT
  CASE
    WHEN prev_query_norm IS NULL THEN 'first_query'
    WHEN prev_results_count = 0 THEN 'prev_zero_results'
    WHEN prev_results_count > 0 AND prev_has_click = 0 THEN 'prev_no_click_with_results'
    WHEN prev_has_click = 1 THEN 'prev_had_click'
    ELSE 'other'
  END AS prior_state,
  COUNT(*) AS next_searches,
  ROUND(AVG(is_reformulation) * 100.0, 2) AS reformulation_rate_pct,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS next_zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS next_ctr_pct
FROM v_session_seq_flags
GROUP BY prior_state;

-- Time trend (daily), so your report can show movement
DROP TABLE IF EXISTS m_daily;
CREATE TABLE m_daily AS
WITH d AS (
  SELECT
    substr(ts, 1, 10) AS day,
    *
  FROM v_session_seq_flags
)
SELECT
  day,
  COUNT(*) AS searches,
  ROUND(AVG(is_zero_results) * 100.0, 2) AS zero_results_rate_pct,
  ROUND(AVG(has_click) * 100.0, 2) AS ctr_pct,
  ROUND(AVG(is_no_click_with_results) * 100.0, 2) AS no_click_with_results_rate_pct,
  ROUND(AVG(sat_click) * 100.0, 2) AS sat_click_rate_pct,
  ROUND(AVG(is_reformulation) * 100.0, 2) AS reformulation_rate_pct
FROM d
GROUP BY day
ORDER BY day;
