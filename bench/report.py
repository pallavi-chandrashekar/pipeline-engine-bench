"""Summarize runs.csv into summary.csv, summary.md and charts.

  python -m bench.report results/<run_id>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from bench.engines import ENGINES  # noqa: E402

PIPELINE_STAGES = ["startup", "transform_write", "aggregate", "top_zones"]
TIER_ORDER = ["SYN", "S", "M", "L", "XL"]

# Fixed categorical order (colour follows the engine, never its rank).
COLORS = {"spark": "#2a78d6", "duckdb": "#eb6834", "polars": "#1baf7a", "delta-rs": "#eda100"}
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _tier_key(t):
    return TIER_ORDER.index(t) if t in TIER_ORDER else len(TIER_ORDER)


def summarize(runs: pd.DataFrame) -> pd.DataFrame:
    ok = runs[(runs.status == "ok") & (runs.stage != "process")].copy()
    ok["seconds"] = ok.seconds.astype(float)
    ok["peak_rss_mb"] = pd.to_numeric(ok.peak_rss_mb, errors="coerce")
    g = ok.groupby(["tier", "rows", "engine", "stage"])
    s = g.agg(median_s=("seconds", "median"), min_s=("seconds", "min"), runs=("seconds", "size"),
              peak_rss_mb=("peak_rss_mb", "max")).reset_index()
    def combine(v):
        vals = [x for x in v.fillna("") if x]
        if not vals:
            return "-"  # stage has no check (startup) or never succeeded
        bad = sorted({x for x in vals if x != "pass"})
        return "; ".join(bad) if bad else "pass"

    checks = runs[runs.stage != "process"].groupby(["tier", "engine", "stage"]).verified.apply(combine).reset_index()
    return s.merge(checks, on=["tier", "engine", "stage"], how="left")


def _status_of(runs, tier, engine, stage):
    st = runs[(runs.tier == tier) & (runs.engine == engine) & (runs.stage == stage)].status
    if st.empty:
        return "-"
    return "ok" if (st == "ok").any() else st.iloc[0]


def pipeline_totals(s: pd.DataFrame, runs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (tier, engine), rgrp in runs.groupby(["tier", "engine"]):
        grp = s[(s.tier == tier) & (s.engine == engine)]
        med = grp.set_index("stage").median_s
        have = all(st in med for st in PIPELINE_STAGES)
        missing = [_status_of(runs, tier, engine, st) for st in PIPELINE_STAGES if st not in med]
        proc = rgrp[rgrp.stage == "process"]
        rows.append(dict(
            tier=tier, rows=int(rgrp.rows.iloc[0]), engine=engine,
            pipeline_s=float(med[PIPELINE_STAGES].sum()) if have else None,
            pipeline_status="ok" if have else (missing[0] if missing else "-"),
            merge_s=float(med["merge"]) if "merge" in med else None,
            merge_status=_status_of(runs, tier, engine, "merge"),
            peak_rss_mb=pd.to_numeric(proc.peak_rss_mb, errors="coerce").max(),
            all_checks_pass=bool(grp.verified.fillna("-").isin(["pass", "-"]).all()),
        ))
    out = pd.DataFrame(rows)
    out["_t"] = out.tier.map(_tier_key)
    out["_e"] = out.engine.map(lambda e: ENGINES.index(e) if e in ENGINES else 99)
    return out.sort_values(["_t", "_e"]).drop(columns=["_t", "_e"])


def _fmt_s(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x:.2f}s" if x < 10 else f"{x:.0f}s"


def _fmt_mb(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x / 1024:.1f} GB" if x >= 1024 else f"{x:.0f} MB"


def small_multiples(totals: pd.DataFrame, value: str, status: str | None, title: str, xlabel: str, fmt, path: Path):
    """One panel per tier, each on its own linear axis starting at zero (bars on a log
    axis exaggerate small values). Engines keep a fixed row order and colour; an engine
    that didn't produce a value shows its status (n/a, crashed, timeout) instead."""
    if totals[value].dropna().empty:
        return
    tiers = sorted(totals.tier.unique(), key=_tier_key)
    engines = [e for e in ENGINES if e in set(totals.engine)]
    fig, axes = plt.subplots(1, len(tiers), figsize=(max(5.5, 3.6 * len(tiers)), 0.55 * len(engines) + 1.9),
                             dpi=150, squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    for ax, t in zip(axes[0], tiers):
        ax.set_facecolor(SURFACE)
        d = totals[totals.tier == t].set_index("engine")
        vmax = d[value].max()
        ys = list(range(len(engines)))[::-1]
        for y, e in zip(ys, engines):
            v = d[value].get(e)
            if v is not None and not pd.isna(v):
                ax.barh(y, v, height=0.62, color=COLORS.get(e, "#888"), edgecolor=SURFACE, linewidth=2, zorder=3)
                ax.annotate(fmt(v), (v, y), xytext=(4, 0), textcoords="offset points",
                            va="center", ha="left", fontsize=9, color=INK)
            else:
                label = d[status].get(e, "-") if status else "-"
                ax.annotate(str(label), (0, y), xytext=(4, 0), textcoords="offset points",
                            va="center", ha="left", fontsize=9, color=INK_2, style="italic")
        ax.set_yticks(ys, engines, fontsize=10, color=INK)
        ax.set_ylim(-0.6, len(engines) - 0.4)
        ax.set_xlim(0, (vmax if vmax and not pd.isna(vmax) else 1) * 1.28)
        r = int(d.rows.iloc[0])
        ax.set_title(f"{t} · {r / 1e6:.1f}M rows" if r >= 1e6 else f"{t} · {r:,} rows",
                     loc="left", fontsize=10, color=INK_2)
        ax.grid(axis="x", color=GRID, linewidth=0.8, zorder=0)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(GRID)
        ax.tick_params(axis="x", colors=INK_2, labelsize=8, length=0)
        ax.tick_params(axis="y", length=0)
    fig.suptitle(title, x=0.01, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.supxlabel(xlabel, fontsize=9, color=INK_2)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def build(run_dir: Path) -> None:
    run_dir = Path(run_dir)
    runs = pd.read_csv(run_dir / "runs.csv", dtype={"verified": str, "detail": str, "notes": str, "status": str},
                       keep_default_na=False, na_values={"seconds": [""], "peak_rss_mb": [""]})
    s = summarize(runs)
    s.to_csv(run_dir / "summary.csv", index=False)
    totals = pipeline_totals(s, runs)
    totals.to_csv(run_dir / "totals.csv", index=False)
    env = json.loads((run_dir / "env.json").read_text()) if (run_dir / "env.json").exists() else {}

    lines = [f"# Results: run `{run_dir.name}`", ""]
    if env:
        v = env.get("versions", {})
        lines += [
            f"Machine: {env.get('platform')} | {env.get('cpu_physical')} cores / {env.get('cpu_logical')} threads"
            f" | {env.get('ram_gb')} GB RAM | Python {env.get('python')} | {env.get('java') or 'no java'}",
            "",
            "Versions: " + ", ".join(f"{k} {val}" for k, val in v.items() if val),
            "",
        ]
    lines += [
        "**Pipeline time** = startup + transform_write + aggregate + top_zones (median of runs).",
        "MERGE is reported separately because not every engine can MERGE into Delta.",
        "",
        "| Tier | Rows | Engine | Pipeline | MERGE | Peak memory | Checks |",
        "|---|---:|---|---:|---:|---:|---|",
    ]
    for r in totals.itertuples():
        merge = _fmt_s(r.merge_s) if r.merge_status == "ok" else r.merge_status
        pipe = _fmt_s(r.pipeline_s) if r.pipeline_status == "ok" else r.pipeline_status
        lines.append(f"| {r.tier} | {r.rows:,} | {r.engine} | {pipe} | {merge} | "
                     f"{_fmt_mb(r.peak_rss_mb)} | {'pass' if r.all_checks_pass else 'FAIL'} |")

    lines += ["", "## Per-stage medians", "", "| Tier | Engine | Stage | Median | Min | Runs | Peak memory | Check |",
              "|---|---|---|---:|---:|---:|---:|---|"]
    order = ["startup", "transform_write", "aggregate", "top_zones", "merge"]
    s["_t"], s["_st"] = s.tier.map(_tier_key), s.stage.map(lambda x: order.index(x) if x in order else 9)
    s["_e"] = s.engine.map(lambda e: ENGINES.index(e) if e in ENGINES else 99)
    for r in s.sort_values(["_t", "_e", "_st"]).itertuples():
        lines.append(f"| {r.tier} | {r.engine} | {r.stage} | {_fmt_s(r.median_s)} | {_fmt_s(r.min_s)} | {r.runs} | "
                     f"{_fmt_mb(r.peak_rss_mb)} | {r.verified} |")

    problems = runs[~runs.status.isin(["ok", "0"]) & (runs.stage != "process")]
    if not problems.empty:
        lines += ["", "## Not run / failed", ""]
        for r in problems.drop_duplicates(["tier", "engine", "stage", "status"]).itertuples():
            first = (str(r.detail).strip().splitlines() or [""])[0][:200]
            lines.append(f"- {r.tier} / {r.engine} / {r.stage}: **{r.status}**" + (f": {first}" if first else ""))
    notes = runs.drop_duplicates(["engine"])[["engine", "notes"]]
    lines += ["", "## Engine notes", ""] + [f"- {r.engine}: {r.notes}" for r in notes.itertuples() if isinstance(r.notes, str) and r.notes]
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n")

    small_multiples(totals, "pipeline_s", "pipeline_status",
                    "Same Delta pipeline, four engines, one machine",
                    "pipeline time in seconds (startup + transform & write + 2 queries; lower is better)",
                    _fmt_s, run_dir / "pipeline_time.png")
    small_multiples(totals, "peak_rss_mb", None, "Peak memory (whole process, incl. Spark's JVM)",
                    "peak resident memory (lower is better)", _fmt_mb, run_dir / "peak_memory.png")
    small_multiples(totals, "merge_s", "merge_status", "MERGE (upsert a corrections batch) into Delta",
                    "seconds (lower is better)", _fmt_s, run_dir / "merge_time.png")
    print((run_dir / "summary.md").read_text())


if __name__ == "__main__":
    build(Path(sys.argv[1]))
