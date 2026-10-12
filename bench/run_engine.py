"""Worker: run the full pipeline for ONE engine on ONE tier in a fresh process.

Launched by bench.harness. Writes metrics.json into --workdir after EVERY stage (so a
crash mid-run still leaves the finished stages on record) plus small result tables.
Stage start/end wall-clock times let the harness attribute memory samples to stages.

--warmup only starts the engine and stops it (untimed by the harness): it installs the
DuckDB delta extension, resolves Spark's Delta jars, and warms the OS file cache for
engine binaries, so the timed runs measure steady-state startup.
"""

from __future__ import annotations

import argparse
import json
import time
import traceback
from pathlib import Path

import pyarrow.parquet as pq

from bench import config as C
from bench import workload as W


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True)
    ap.add_argument("--tier", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--stages", default=",".join(W.STAGES))
    ap.add_argument("--warmup", action="store_true")
    a = ap.parse_args(argv)

    from bench.engines import NotSupported, load

    wd = Path(a.workdir)
    paths = C.table_paths(a.tier)
    stages: dict[str, dict] = {}
    notes: list[str] = []

    def flush():
        (wd / "metrics.json").write_text(
            json.dumps({"engine": a.engine, "tier": a.tier, "stages": stages, "notes": notes}, indent=2)
        )

    def timed(name, fn):
        rec = {"start": time.time()}
        t0 = time.perf_counter()
        out = None
        try:
            out = fn()
            rec["status"] = "ok"
        except NotSupported as e:
            rec["status"], rec["reason"] = "n/a", str(e)
        except Exception as e:  # noqa: BLE001 - every failure is a data point
            rec["status"], rec["reason"] = "error", f"{type(e).__name__}: {e}"[:2000]
            traceback.print_exc()
        rec["seconds"] = time.perf_counter() - t0
        rec["end"] = time.time()
        stages[name] = rec
        flush()
        return out

    # startup = import + session/connection + registering the bronze tables
    eng = None

    def startup():
        nonlocal eng
        eng = load(a.engine)
        note = eng.start()
        eng.register(str(paths["trips"]), str(paths["zones"]))
        if note:
            notes.append(note)

    timed("startup", startup)
    if stages["startup"]["status"] != "ok":
        return 1
    if a.warmup:
        eng.stop()
        return 0

    wanted = a.stages.split(",")
    silver_ok = False
    if "transform_write" in wanted:
        timed("transform_write", lambda: eng.transform_write(str(wd / "silver")))
        if stages["transform_write"]["status"] == "ok":
            try:
                eng.register_silver(str(wd / "silver"))  # metadata only; untimed
                silver_ok = True
            except Exception as e:  # noqa: BLE001
                stages["transform_write"].update(status="error", reason=f"silver not readable: {e}"[:2000])
                flush()

    for q in ("aggregate", "top_zones"):
        if q not in wanted:
            continue
        if not silver_ok:
            stages[q] = {"status": "skipped", "reason": "no silver table from transform_write",
                         "seconds": 0.0, "start": time.time(), "end": time.time()}
            flush()
            continue
        tbl = timed(q, getattr(eng, q))
        if tbl is not None:
            pq.write_table(tbl, wd / f"{q}.parquet")

    if "merge" in wanted:
        timed("merge", lambda: eng.merge(str(wd / "trips_copy"), str(paths["corrections"])))

    try:
        eng.stop()
    except Exception:  # noqa: BLE001 - e.g. Spark's JVM already gone
        traceback.print_exc()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
