from __future__ import annotations

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


FIG_DIR = Path("reports/figures")
IN_DIR = Path("reports/metrics_outputs")


def script_label(row: pd.Series) -> str:
    """Human-readable label for script flags."""
    parts = []
    if int(row["has_kanji"]) == 1:
        parts.append("kanji")
    if int(row["has_kana"]) == 1:
        parts.append("kana")
    if int(row["has_romaji"]) == 1:
        parts.append("romaji")
    if int(row["has_halfwidth_kana"]) == 1:
        parts.append("halfwidth")
    return "+".join(parts) if parts else "none"


def plot_zero_results_by_script_flags() -> Path:
    df = pd.read_csv(IN_DIR / "m_by_script_flags.csv")

    # Add readable group label and filter very small groups to reduce noise.
    df["group"] = df.apply(script_label, axis=1)
    df = df.sort_values(["searches"], ascending=False)

    # Optional: show only groups with at least N searches
    MIN_SEARCHES = 50
    df_plot = df[df["searches"] >= MIN_SEARCHES].copy()

    # Sort by zero-results rate for more interpretable chart
    df_plot = df_plot.sort_values("zero_results_rate_pct", ascending=False)

    plt.figure()
    plt.bar(df_plot["group"], df_plot["zero_results_rate_pct"])
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Zero-results rate (%)")
    plt.title(f"Zero-results rate by script group (n≥{MIN_SEARCHES})")
    plt.tight_layout()

    out = FIG_DIR / "zero_results_by_script_group.png"
    plt.savefig(out, dpi=200)
    plt.close()
    return out


def plot_top_bad_queries_no_click() -> Path:
    df = pd.read_csv(IN_DIR / "m_top_bad_queries.csv")

    # Take top 10 by "no_click_with_results_rate_pct"
    df = df.sort_values(
        ["no_click_with_results_rate_pct", "searches"],
        ascending=[False, False],
    ).head(10)

    # Reverse for nicer top-to-bottom ordering in barh
    df = df.iloc[::-1].copy()

    plt.figure()
    plt.barh(df["query_norm"], df["no_click_with_results_rate_pct"])
    plt.xlabel("No-click with results rate (%)")
    plt.title("Top 10 problematic queries by no-click-with-results rate")
    plt.tight_layout()

    out = FIG_DIR / "top10_bad_queries_no_click_with_results.png"
    plt.savefig(out, dpi=200)
    plt.close()
    return out


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Basic existence checks (helpful when paths are wrong)
    required = [
        IN_DIR / "m_by_script_flags.csv",
        IN_DIR / "m_top_bad_queries.csv",
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required metrics files: {missing}")

    a = plot_zero_results_by_script_flags()
    b = plot_top_bad_queries_no_click()

    print("Wrote figures:")
    print(f"- {a}")
    print(f"- {b}")


if __name__ == "__main__":
    main()
