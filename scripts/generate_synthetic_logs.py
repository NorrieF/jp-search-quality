#!/usr/bin/env python3
"""
Generate synthetic-but-realistic JP-market search logs for Project 1.

Outputs (CSV):
- data/content_catalog.csv
- data/search_events.csv
- data/click_events.csv

Designed to match sql/schema.sql exactly.
"""

from __future__ import annotations

import argparse
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


JST = timezone(timedelta(hours=9))


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def now_jst_iso() -> str:
    # ISO string without timezone suffix to keep SQLite TEXT simple
    return datetime.now(JST).replace(microsecond=0).isoformat()


KANJI_RE = re.compile(r"[\u4E00-\u9FFF]")
KANA_RE = re.compile(r"[\u3040-\u30FF]")  # hiragana + katakana
ROMAJI_RE = re.compile(r"[A-Za-z]")
HALFWIDTH_KANA_RE = re.compile(r"[\uFF65-\uFF9F]")


def has_kanji(s: str) -> int:
    return 1 if KANJI_RE.search(s) else 0


def has_kana(s: str) -> int:
    return 1 if KANA_RE.search(s) else 0


def has_romaji(s: str) -> int:
    return 1 if ROMAJI_RE.search(s) else 0


def has_halfwidth_kana(s: str) -> int:
    return 1 if HALFWIDTH_KANA_RE.search(s) else 0


def normalize_query(q: str) -> str:
    # Keep it simple but realistic:
    # - strip
    # - collapse whitespace
    # - lowercase romaji
    q2 = q.strip()
    q2 = re.sub(r"\s+", " ", q2)
    q2 = q2.lower()
    return q2


@dataclass(frozen=True)
class Entity:
    title: str
    artist_or_show: str | None
    language: str
    explicit_flag: int
    type: str  # track/artist/album/episode/show/movie
    vertical: str  # music/podcast/tv
    aliases: list[str]


def build_seed_entities() -> list[Entity]:
    """
    Small hand-crafted seed set with JP-specific alias / script variation.
    We'll expand it stochastically to create a "catalog".
    """
    return [
        Entity(
            title="First Love",
            artist_or_show="宇多田ヒカル",
            language="ja",
            explicit_flag=0,
            type="track",
            vertical="music",
            aliases=["宇多田ヒカル first love", "utada hikaru first love", "うただひかる first love"],
        ),
        Entity(
            title="怪物",
            artist_or_show="YOASOBI",
            language="ja",
            explicit_flag=0,
            type="track",
            vertical="music",
            aliases=["yoasobi 怪物", "ようあそび かいぶつ", "怪物 yoasobi"],
        ),
        Entity(
            title="Pretender",
            artist_or_show="Official髭男dism",
            language="ja",
            explicit_flag=0,
            type="track",
            vertical="music",
            aliases=["ヒゲダン pretender", "official hige dandism pretender", "髭男 pretender"],
        ),
        Entity(
            title="終わりなき旅",
            artist_or_show="Mr.Children",
            language="ja",
            explicit_flag=0,
            type="track",
            vertical="music",
            aliases=["ミスチル 終わりなき旅", "mr.children 終わりなき旅", "終わりなき旅 ミスチル"],
        ),
        Entity(
            title="鬼滅の刃",
            artist_or_show=None,
            language="ja",
            explicit_flag=0,
            type="show",
            vertical="tv",
            aliases=["きめつのやいば", "demon slayer", "鬼滅"],
        ),
        Entity(
            title="千と千尋の神隠し",
            artist_or_show=None,
            language="ja",
            explicit_flag=0,
            type="movie",
            vertical="tv",
            aliases=["spirited away", "千と千尋", "せんとちひろ"],
        ),
        Entity(
            title="最新回：AIニュースまとめ",
            artist_or_show="テックラジオ",
            language="ja",
            explicit_flag=0,
            type="episode",
            vertical="podcast",
            aliases=["テックラジオ 最新回", "ai ニュース まとめ", "tech radio latest"],
        ),
        Entity(
            title="第10話：最終回の裏側",
            artist_or_show="ドラマ考察ポッドキャスト",
            language="ja",
            explicit_flag=0,
            type="episode",
            vertical="podcast",
            aliases=["最終回 裏側", "ドラマ 考察 最終回", "drama podcast finale"],
        ),
        # A couple ambiguous / English-ish entries to create confusion
        Entity(
            title="HERO",
            artist_or_show=None,
            language="ja",
            explicit_flag=0,
            type="movie",
            vertical="tv",
            aliases=["hero 映画", "hero", "ヒーロー"],
        ),
        Entity(
            title="Green",
            artist_or_show="GReeeeN",
            language="ja",
            explicit_flag=0,
            type="track",
            vertical="music",
            aliases=["グリーン", "greeeen green", "green グリーン"],
        ),
    ]


def expand_catalog(seed: list[Entity], n_items: int, rng: random.Random) -> pd.DataFrame:
    """
    Create a synthetic catalog by cloning seed entities and adding some noise.
    """
    rows = []
    base_date = datetime(2015, 1, 1, tzinfo=JST)

    for _ in range(n_items):
        e = rng.choice(seed)
        content_id = new_id("c")

        # popularity skew: long-tail heavy
        popularity = float(np.clip(np.random.lognormal(mean=0.0, sigma=1.0), 0.0, 100.0))
        # release dates spread
        rel = base_date + timedelta(days=int(rng.random() * 3650))
        release_date = rel.date().isoformat()

        rows.append(
            dict(
                content_id=content_id,
                type=e.type,
                title=e.title,
                artist_or_show=e.artist_or_show,
                language=e.language,
                explicit_flag=int(e.explicit_flag),
                release_date=release_date,
                popularity=popularity,
                vertical=e.vertical,  # not in schema, but useful internally
                aliases=e.aliases,
            )
        )

    df = pd.DataFrame(rows)

    # Ensure schema columns only for output content_catalog.csv
    out = df[
        ["content_id", "type", "title", "artist_or_show", "language", "explicit_flag", "release_date", "popularity"]
    ].copy()

    return out, df  # out for CSV, df internal w/ vertical+aliases


def sample_query_from_entity(e_row: pd.Series, rng: random.Random) -> str:
    # Use seed-like alias behavior: pick an alias sometimes; otherwise title/artist combos.
    aliases = e_row.get("aliases", [])
    if isinstance(aliases, list) and aliases and rng.random() < 0.55:
        return rng.choice(aliases)

    title = str(e_row["title"])
    a = e_row.get("artist_or_show", None)
    if pd.isna(a) or a is None:
        return title

    artist = str(a)
    patterns = [
        f"{artist} {title}",
        f"{title} {artist}",
        f"{title}",
    ]
    return rng.choice(patterns)


def make_bad_variant(q: str, rng: random.Random) -> str:
    """
    Introduce realistic JP input issues: spacing, casing, punctuation, half-width noise.
    """
    out = q

    # random extra spaces
    if rng.random() < 0.25:
        out = re.sub(r"\s+", " ", out)
        out = out.replace(" ", "  ")

    # random punctuation / brackets noise
    if rng.random() < 0.18:
        out = out + rng.choice(["（歌詞）", " 公式", " ライブ", " 字幕", " 吹き替え"])

    # random romaji case variation
    if rng.random() < 0.15:
        out = "".join(ch.upper() if rng.random() < 0.5 else ch.lower() for ch in out)

    # half-width katakana sprinkle (rare but a known ingestion/input smell)
    if rng.random() < 0.05:
        out = out + rng.choice(["ｶﾀｶﾅ", "ﾃｽﾄ"])

    return out.strip()


def choose_device(rng: random.Random) -> str:
    return rng.choices(["mobile", "desktop"], weights=[0.72, 0.28], k=1)[0]


def choose_vertical(e_vertical: str, rng: random.Random) -> str:
    # Usually consistent with entity vertical, sometimes wrong due to user navigation or product confusion
    if rng.random() < 0.92:
        return e_vertical
    return rng.choice(["music", "podcast", "tv"])


def simulate_sessions(
    catalog_internal: pd.DataFrame,
    n_searches: int,
    rng: random.Random,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Produce search_events and click_events.
    """
    search_rows = []
    click_rows = []

    # We'll make sessions by batching searches
    searches_left = n_searches
    base_ts = datetime.now(JST).replace(microsecond=0) - timedelta(days=14)

    user_pool = [new_id("u") for _ in range(750)]

    while searches_left > 0:
        session_id = new_id("s")
        user_id = rng.choice(user_pool)
        device = choose_device(rng)

        # number of searches in this session
        k = min(searches_left, rng.choices([1, 2, 3, 4], weights=[0.62, 0.24, 0.10, 0.04], k=1)[0])
        searches_left -= k

        # pick a "theme entity" for session, to allow reformulation
        entity = catalog_internal.sample(n=1).iloc[0]
        e_vertical = str(entity["vertical"])

        # time within session
        t = base_ts + timedelta(minutes=int(rng.random() * 60 * 24 * 14))
        last_query_norm = None

        for i in range(k):
            event_id = new_id("se")

            q = sample_query_from_entity(entity, rng)

            # sometimes create a "bad variant"
            if rng.random() < 0.30:
                q = make_bad_variant(q, rng)

            qn = normalize_query(q)

            # create reformulation: if repeated, modify slightly
            if last_query_norm is not None and rng.random() < 0.55:
                # add a clarifier (very JP-reasonable)
                q = q + rng.choice([" 歌詞", " 主題歌", " 最新", " 最終回", " 口コミ"])
                qn = normalize_query(q)

            last_query_norm = qn

            vertical = choose_vertical(e_vertical, rng)

            # simulate results_count with some structured failure modes
            # romaji queries fail more often; halfwidth fails more often
            romaji_flag = has_romaji(q)
            half_flag = has_halfwidth_kana(q)

            p_zero = 0.05
            if romaji_flag:
                p_zero += 0.07
            if half_flag:
                p_zero += 0.10

            # ambiguous keywords increase "badness"
            if re.search(r"\bhero\b|グリーン|green", q, re.IGNORECASE):
                p_zero += 0.03

            zero = rng.random() < p_zero

            if zero:
                results_count = 0
            else:
                # some range of results
                results_count = int(rng.choices([1, 3, 5, 10, 25], weights=[0.10, 0.20, 0.30, 0.25, 0.15], k=1)[0])

            row = dict(
                event_id=event_id,
                ts=t.isoformat(),
                user_id=user_id,
                session_id=session_id,
                locale="JP",
                device=device,
                query_raw=q,
                query_norm=qn,
                vertical=vertical,
                results_count=results_count,
                has_kanji=has_kanji(q),
                has_kana=has_kana(q),
                has_romaji=romaji_flag,
                has_halfwidth_kana=half_flag,
                query_len=len(q),
            )
            search_rows.append(row)

            # simulate click behavior
            # If results=0, no clicks.
            if results_count == 0:
                t += timedelta(seconds=rng.randint(5, 25))
                continue

            # no-click probability
            p_no_click = 0.25
            if romaji_flag:
                p_no_click += 0.08
            if results_count <= 1:
                p_no_click -= 0.05
            if "歌詞" in q or "主題歌" in q:
                p_no_click += 0.05

            if rng.random() < p_no_click:
                t += timedelta(seconds=rng.randint(8, 35))
                continue

            # click rank: better when query matches well; worse when noisy
            base_rank = rng.choices([1, 2, 3, 4, 5, 7, 10], weights=[0.35, 0.20, 0.14, 0.10, 0.08, 0.08, 0.05], k=1)[0]
            if romaji_flag or half_flag:
                base_rank = min(10, base_rank + rng.randint(1, 3))

            rank = int(max(1, min(base_rank, results_count)))

            dwell = int(rng.choices([5, 12, 30, 60, 120, 240], weights=[0.10, 0.18, 0.26, 0.22, 0.16, 0.08], k=1)[0])

            # pick some content_id (not necessarily "correct", but realistic)
            content_id = str(catalog_internal.sample(n=1).iloc[0]["content_id"])

            click_rows.append(
                dict(
                    click_id=new_id("ce"),
                    ts=(t + timedelta(seconds=rng.randint(1, 10))).isoformat(),
                    session_id=session_id,
                    event_id=event_id,
                    content_id=content_id,
                    rank=rank,
                    dwell_sec=dwell,
                )
            )

            t += timedelta(seconds=rng.randint(10, 45))

    se = pd.DataFrame(search_rows)
    ce = pd.DataFrame(click_rows)

    # Ensure integer columns are int
    for c in ["results_count", "has_kanji", "has_kana", "has_romaji", "has_halfwidth_kana", "query_len"]:
        se[c] = se[c].astype(int)
    for c in ["rank", "dwell_sec"]:
        ce[c] = ce[c].astype(int)

    return se, ce


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_dir", default="data", help="Output directory for CSV files")
    parser.add_argument("--n_catalog", type=int, default=2000, help="Number of catalog items")
    parser.add_argument("--n_searches", type=int, default=12000, help="Number of search events")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed for reproducibility")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    np.random.seed(args.seed)

    seed_entities = build_seed_entities()
    catalog_out, catalog_internal = expand_catalog(seed_entities, args.n_catalog, rng)

    search_events, click_events = simulate_sessions(catalog_internal.assign(aliases=catalog_internal["aliases"]), args.n_searches, rng)

    # Write outputs that match schema exactly
    catalog_path = out_dir / "content_catalog.csv"
    search_path = out_dir / "search_events.csv"
    click_path = out_dir / "click_events.csv"

    catalog_out.to_csv(catalog_path, index=False, encoding="utf-8")
    search_events.to_csv(search_path, index=False, encoding="utf-8")
    click_events.to_csv(click_path, index=False, encoding="utf-8")

    print(f"Wrote: {catalog_path} ({len(catalog_out):,} rows)")
    print(f"Wrote: {search_path} ({len(search_events):,} rows)")
    print(f"Wrote: {click_path} ({len(click_events):,} rows)")
    print("Done.")


if __name__ == "__main__":
    main()
