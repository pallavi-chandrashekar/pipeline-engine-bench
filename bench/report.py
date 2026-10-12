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


def _valid(runs: pd.DataFrame) -> pd.Series:
    """A stage run counts only if it succeeded AND (where checked) produced the right answer."""
    return (runs.status == "ok") & runs.verified.isin(["pass", ""])


def summarize(runs: pd.DataFrame) -> pd.DataFrame:
    st = runs[runs.stage != "process"].copy()
    st["valid"] = _valid(st)
    out = []
    for (tier, engine, stage), g in st.groupby(["tier", "engine", "stage"]):
        v = g[g.valid]
        bad_checks = sorted({x for x in g.verified if x and x != "pass"})
        out.append(dict(
            tier=tier, rows=int(g.rows.iloc[0]), engine=engine, stage=stage,
            median_s=v.seconds.median() if len(v) else None,
            min_s=v.seconds.min() if len(v) else None,
            valid_runs=len(v), total_runs=len(g),
            peak_rss_mb=v.peak_rss_mb.median() if len(v) else None,
            verified="; ".join(bad_checks) if bad_checks else ("pass" if (g.verified == "pass").any() else "-"),
            status="ok" if len(v) else ("wrong answer" if bad_checks else g.status.iloc[0]),
        ))
    return pd.DataFrame(out)


def pipeline_totals(s: pd.DataFrame, runs: pd.DataFrame) -> pd.DataFrame:
    st = runs[runs.stage != "process"].copy()
    st["valid"] = _valid(st)
    rows = []
    for (tier, engine), g in s.groupby(["tier", "engine"]):
        stg = g.set_index("stage")
        pipe = stg.reindex(PIPELINE_STAGES)
        have = pipe.median_s.notna().all()
        bad = pipe[pipe.median_s.isna()]
        r = st[(st.tier == tier) & (st.engine == engine)]
        # peak memory over the pipeline stages only (MERGE is reported separately), per
        # repeat in which the whole pipeline was valid, then the median across repeats
        reps_ok = [rep for rep, rg in r[r.stage.isin(PIPELINE_STAGES)].groupby("rep") if rg.valid.all()]
        pipe_peaks = [r[(r.rep == rep) & r.stage.isin(PIPELINE_STAGES)].peak_rss_mb.max() for rep in reps_ok]
        merge = stg.loc["merge"] if "merge" in stg.index else None
        n_valid = int(pipe.valid_runs.min()) if have else 0
        n_total = int(pipe.total_runs.max()) if pipe.total_runs.notna().any() else 0
        checks_ran = (r.verified != "").any()
        rows.append(dict(
            tier=tier, rows=int(g.rows.iloc[0]), engine=engine,
            pipeline_s=float(pipe.median_s.sum()) if have else None,
            pipeline_status="ok" if have else str(bad.status.iloc[0]),
            valid_runs=f"{n_valid}/{n_total}",
            pipeline_peak_mb=pd.Series(pipe_peaks).median() if pipe_peaks else None,
            merge_s=None if merge is None or pd.isna(merge.median_s) else float(merge.median_s),
            merge_status="-" if merge is None else str(merge.status),
            merge_peak_mb=None if merge is None else merge.peak_rss_mb,
            checks=("pass" if g.verified.isin(["pass", "-"]).all() else "FAIL") if checks_ran else "-",
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
        if fmt is _fmt_mb:
            from matplotlib.ticker import FuncFormatter, MultipleLocator

            top = ax.get_xlim()[1]
            step = 1024 * max(1, round(top / 1024 / 5)) if top > 2048 else 256
            ax.xaxis.set_major_locator(MultipleLocator(step))
            ax.xaxis.set_major_formatter(FuncFormatter(
                lambda x, _: "0" if not x else (f"{x / 1024:g} GB" if x >= 1024 else f"{x:.0f} MB")))
        ax.tick_params(axis="y", length=0)
    fig.suptitle(title, x=0.01, ha="left", fontsize=13, fontweight="bold", color=INK)
    import textwrap

    width_chars = int(fig.get_figwidth() * 13)
    fig.supxlabel("\n".join(textwrap.wrap(xlabel, width_chars)), fontsize=9, color=INK_2)
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
        "**Pipeline time** = startup + transform_write + aggregate + top_zones, median over valid runs.",
        "A run counts as valid only if it finished AND passed its correctness check.",
        "MERGE is reported separately because not every engine can MERGE into Delta.",
        "Memory = peak RSS of the whole process tree (incl. Spark's JVM) during those stages.",
        "",
        "| Tier | Rows | Engine | Pipeline | Valid runs | Peak memory | MERGE | MERGE peak | Checks |",
        "|---|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for r in totals.itertuples():
        merge = _fmt_s(r.merge_s) if r.merge_status == "ok" else r.merge_status
        pipe = _fmt_s(r.pipeline_s) if r.pipeline_status == "ok" else r.pipeline_status
        lines.append(f"| {r.tier} | {r.rows:,} | {r.engine} | {pipe} | {r.valid_runs} | {_fmt_mb(r.pipeline_peak_mb)} | "
                     f"{merge} | {_fmt_mb(r.merge_peak_mb) if r.merge_status == 'ok' else '-'} | {r.checks} |")

    lines += ["", "## Per-stage medians (valid runs)", "",
              "| Tier | Engine | Stage | Median | Min | Valid/total | Peak memory | Check |",
              "|---|---|---|---:|---:|---:|---:|---|"]
    order = ["startup", "transform_write", "aggregate", "top_zones", "merge"]
    s["_t"], s["_st"] = s.tier.map(_tier_key), s.stage.map(lambda x: order.index(x) if x in order else 9)
    s["_e"] = s.engine.map(lambda e: ENGINES.index(e) if e in ENGINES else 99)
    for r in s.sort_values(["_t", "_e", "_st"]).itertuples():
        med = _fmt_s(r.median_s) if r.status == "ok" else r.status
        lines.append(f"| {r.tier} | {r.engine} | {r.stage} | {med} | {_fmt_s(r.min_s)} | {r.valid_runs}/{r.total_runs} | "
                     f"{_fmt_mb(r.peak_rss_mb)} | {r.verified} |")

    problems = runs[(~runs.status.isin(["ok", "0"]) | ~runs.verified.isin(["pass", ""])) & (runs.stage != "process")]
    if not problems.empty:
        lines += ["", "## Not run / failed", ""]
        for r in problems.drop_duplicates(["tier", "engine", "stage", "status"]).itertuples():
            first = (str(r.detail or r.verified).strip().splitlines() or [""])[0][:200]
            label = r.status if r.status != "ok" else "wrong answer"
            lines.append(f"- {r.tier} / {r.engine} / {r.stage} (rep {r.rep}): **{label}**" + (f": {first}" if first else ""))
    notes = runs[runs.notes != ""].drop_duplicates(["engine", "notes"])[["engine", "notes"]]
    lines += ["", "## Engine notes", ""] + [f"- {r.engine}: {r.notes}" for r in notes.itertuples()]
    if env.get("settings"):
        lines += ["", "Settings: " + ", ".join(f"`{k}={v}`" for k, v in env["settings"].items())]
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n")

    small_multiples(totals, "pipeline_s", "pipeline_status",
                    "Same Delta pipeline, four engines, one machine",
                    "pipeline time in seconds (startup + transform & write + 2 queries; lower is better)",
                    _fmt_s, run_dir / "pipeline_time.png")
    small_multiples(totals, "pipeline_peak_mb", "pipeline_status", "Peak memory during the pipeline",
                    "peak RSS of the whole process incl. Spark's JVM (lower is better). Spark's heap is sized to 60% "
                    "of RAM, so its peak mostly reflects that setting", _fmt_mb, run_dir / "peak_memory.png")
    small_multiples(totals, "merge_s", "merge_status", "MERGE (upsert a corrections batch) into Delta",
                    "seconds (lower is better). Polars delegates MERGE to delta-rs; DuckDB cannot MERGE into Delta", _fmt_s, run_dir / "merge_time.png")
    print((run_dir / "summary.md").read_text())


if __name__ == "__main__":
    build(Path(sys.argv[1]))
