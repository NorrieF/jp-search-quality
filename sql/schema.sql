DROP TABLE IF EXISTS click_events;
DROP TABLE IF EXISTS search_events;
DROP TABLE IF EXISTS content_catalog;

CREATE TABLE content_catalog (
  content_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,              -- track/artist/album/episode/show/movie
  title TEXT NOT NULL,
  artist_or_show TEXT,
  language TEXT,
  explicit_flag INTEGER,
  release_date TEXT,
  popularity REAL
);

CREATE TABLE search_events (
  event_id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  locale TEXT NOT NULL,
  device TEXT NOT NULL,
  query_raw TEXT NOT NULL,
  query_norm TEXT NOT NULL,
  vertical TEXT NOT NULL,          -- music/podcast/tv
  results_count INTEGER NOT NULL,

  has_kanji INTEGER NOT NULL,
  has_kana INTEGER NOT NULL,
  has_romaji INTEGER NOT NULL,
  has_halfwidth_kana INTEGER NOT NULL,
  query_len INTEGER NOT NULL
);

CREATE TABLE click_events (
  click_id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  session_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  content_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  dwell_sec INTEGER NOT NULL,
  FOREIGN KEY(event_id) REFERENCES search_events(event_id)
);

CREATE INDEX idx_search_session ON search_events(session_id);
CREATE INDEX idx_search_ts ON search_events(ts);
CREATE INDEX idx_click_event ON click_events(event_id);
CREATE INDEX idx_click_ts ON click_events(ts);
