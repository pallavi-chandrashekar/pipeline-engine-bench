"""Charts for the delta-rs investigation (docs/deltars-investigation.md):

  1. transform_write time per engine across the four tiers (the cliff), log scale.
  2. delta-rs query time by number of joins at L and XL (the second-join explosion).

Reuses the benchmark's palette from bench.report so the charts match the rest.

  python -m bench.diagnose_charts
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from bench import config as C  # noqa: E402
from bench.report import COLORS, GRID, INK, INK_2, SURFACE, _engine_key  # noqa: E402

OUT = C.ROOT / "docs" / "img"
TIERS = ["S", "M", "L", "XL"]
ROWS_M = {"S": 7.7, "M": 22.6, "L": 84.6, "XL": 218.1}
ALL_RUN = C.RESULTS_DIR / "bench-20261012-all"


def _fmt_s(v: float) -> str:
    return f"{v:.1f}s" if v < 10 else f"{v:.0f}s"


def _style(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)


def transform_write_by_tier(out):
    tw = pd.read_csv(ALL_RUN / "summary.csv")
    tw = tw[tw.stage == "transform_write"]
    engines = sorted(tw.engine.unique(), key=_engine_key)
    x = list(range(len(TIERS)))

    fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    for e in engines:
        y = tw[tw.engine == e].set_index("tier").reindex(TIERS).median_s.tolist()
        ax.plot(x, y, marker="o", ms=5, lw=2.2, color=COLORS.get(e, "#888"), label=e, zorder=3)
    ax.set_yscale("log")
    ax.set_xticks(x, [f"{t}\n{ROWS_M[t]:.0f}M rows" for t in TIERS])
    ax.set_ylabel("transform_write seconds  (log scale)", color=INK_2, fontsize=9)
    ax.set_xlim(-0.25, 3.25)
    _style(ax)

    # call out the two delta-rs points that make the story
    for tier, xi in (("L", 2), ("XL", 3)):
        v = tw[(tw.engine == "delta-rs") & (tw.tier == tier)].median_s.iloc[0]
        ax.annotate(_fmt_s(v), (xi, v), xytext=(0, 9), textcoords="offset points",
                    ha="center", color=COLORS["delta-rs"], fontsize=11, fontweight="bold")
    ax.legend(loc="upper left", frameon=False, fontsize=9, labelcolor=INK)
    ax.set_title("transform_write: delta-rs falls off a cliff at XL while the rest stay flat",
                 loc="left", color=INK, fontsize=12.5, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def join_ablation(out):
    def series(tier):
        d = pd.read_csv(C.RESULTS_DIR / f"diag-20261104-{tier}" / "diagnose.csv").set_index("experiment").seconds
        return [d["ablate_filter"], d["ablate_inner"], d["ablate_bothjoins"], d["query_only"]]

    cats = ["filter\n(0 joins)", "1 join", "2 joins", "full\nquery"]
    L, XL = series("L"), series("XL")
    x = np.arange(len(cats))
    w = 0.38

    fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    bars = [(ax.bar(x - w / 2, L, w, color="#9aa0a6", label="L · 84.6M rows", zorder=3), L),
            (ax.bar(x + w / 2, XL, w, color=COLORS["delta-rs"], label="XL · 218.1M rows", zorder=3), XL)]
    ax.set_yscale("log")
    for rects, vals in bars:
        for r, v in zip(rects, vals):
            ax.annotate(_fmt_s(v), (r.get_x() + r.get_width() / 2, v), xytext=(0, 3),
                        textcoords="offset points", ha="center", fontsize=8.5, color=INK)
    ax.set_xticks(x, cats)
    ax.set_ylabel("delta-rs query seconds  (log scale)", color=INK_2, fontsize=9)
    ax.set_ylim(0.3, max(XL) * 2.2)
    _style(ax)
    ax.legend(loc="upper left", frameon=False, fontsize=9, labelcolor=INK)
    ax.set_title("It's the second join: one join is ~1s, both are 20–560s",
                 loc="left", color=INK, fontsize=12.5, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    transform_write_by_tier(OUT / "transform_write_by_tier.png")
    join_ablation(OUT / "join_ablation.png")
    print(f"wrote {OUT / 'transform_write_by_tier.png'}")
    print(f"wrote {OUT / 'join_ablation.png'}")


if __name__ == "__main__":
    main()
