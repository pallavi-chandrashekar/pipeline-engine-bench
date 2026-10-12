"""Worker: run the full pipeline for ONE engine on ONE tier in a fresh process.

Launched by bench.harness; writes metrics.json and small result tables into --workdir.
Stage start/end wall-clock times are recorded so the harness can attribute its
memory samples to stages.
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
    a = ap.parse_args(argv)

    wd = Path(a.workdir)
    paths = C.table_paths(a.tier)
    stages: dict[str, dict] = {}
    notes: list[str] = []

    def timed(name, fn):
        rec = {"start": time.time()}
        t0 = time.perf_counter()
        try:
            out = fn()
            rec["status"] = "ok"
        except Exception as e:  # noqa: BLE001 - every failure is a data point
            from bench.engines import NotSupported

            out = None
            if isinstance(e, NotSupported):
                rec["status"] = "n/a"
                rec["reason"] = str(e)
            else:
                rec["status"] = "error"
                rec["reason"] = f"{type(e).__name__}: {e}"[:2000]
                traceback.print_exc()
        rec["seconds"] = time.perf_counter() - t0
        rec["end"] = time.time()
        stages[name] = rec
        if isinstance(out, str):
            notes.append(out)
        return out

    # startup = import + session/connection + registering the bronze tables
    def startup():
        from bench.engines import load

        eng = load(a.engine)
        note = eng.start()
        eng.register(str(paths["trips"]), str(paths["zones"]))
        if isinstance(note, str):
            notes.append(note)
        return eng

    rec = {"start": time.time()}
    t0 = time.perf_counter()
    eng = startup()
    rec.update(seconds=time.perf_counter() - t0, end=time.time(), status="ok")
    stages["startup"] = rec

    wanted = a.stages.split(",")
    if "transform_write" in wanted:
        timed("transform_write", lambda: eng.transform_write(str(wd / "silver")))
    for q in ("aggregate", "top_zones"):
        if q in wanted:
            tbl = timed(q, getattr(eng, q))
            if tbl is not None:
                pq.write_table(tbl, wd / f"{q}.parquet")
    if "merge" in wanted:
        timed("merge", lambda: eng.merge(str(wd / "trips_copy"), str(paths["corrections"])))

    eng.stop()
    (wd / "metrics.json").write_text(
        json.dumps({"engine": a.engine, "tier": a.tier, "stages": stages, "notes": notes}, indent=2)
    )


if __name__ == "__main__":
    main()
