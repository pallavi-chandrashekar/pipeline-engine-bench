"""Isolate delta-rs's slow `transform_write` stage into separate experiments.

The benchmark times delta-rs's query and write together in one line, so "the slowdown
is in the DataFusion join execution" is a deduction, not a measurement. This splits them
apart and records, per experiment, wall time, time to the first batch, rows, output file
count, peak RSS of the process tree, and **swap growth** (so memory pressure can be told
apart from slow joins).

Each experiment runs in its own subprocess so its peak RSS and swap are its own.

  python -m bench.diagnose_deltars --tier L                       # all experiments
  python -m bench.diagnose_deltars --tier XL --experiments query_only,probe
  for t in S M L XL; do python -m bench.diagnose_deltars --tier $t --experiments query_only; done

Experiments
  probe         what qb.execute returns + seconds to the FIRST batch (is it streaming?)
  query_only    delta-rs query, batches drained, no write      (DataFusion join cost)
  write_only    write_deltalake on a pre-computed silver table  (the writer alone)
  deltars_e2e   the benchmark's exact query+write path          (baseline)
  duckdb_e2e    DuckDB query + the same writer                  (reference: writer is fine)
  ablate_filter / ablate_inner / ablate_left   the query with no joins / one join (which join?)
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import psutil
import pyarrow as pa
import pyarrow.dataset as pads
from deltalake import DeltaTable, QueryBuilder, write_deltalake

from bench import config as C
from bench import workload as W

DEFAULT_EXPERIMENTS = ["probe", "query_only", "write_only", "deltars_e2e", "duckdb_e2e",
                       "ablate_filter", "ablate_inner", "ablate_left", "ablate_bothjoins"]

# The silver filter, verbatim from workload.SILVER_SQL, reused by the ablations.
_FILTERS = "t.trip_distance > 0 AND t.total_amount > 0 AND t.dropoff_ts > t.pickup_ts"
ABLATE_SQL = {
    # no joins at all
    "ablate_filter": f"SELECT t.* FROM trips t WHERE {_FILTERS}",
    # only the inner join to the pickup zone
    "ablate_inner": (f"SELECT t.*, pz.borough AS pu_borough FROM trips t "
                     f"JOIN zones pz ON t.pu_location_id = pz.location_id WHERE {_FILTERS}"),
    # only the left join to the dropoff zone
    "ablate_left": (f"SELECT t.*, dz.borough AS do_borough FROM trips t "
                    f"LEFT JOIN zones dz ON t.do_location_id = dz.location_id WHERE {_FILTERS}"),
    # both joins, but without the year/month EXTRACT columns (isolates two-join plan vs the scalars)
    "ablate_bothjoins": (f"SELECT t.*, pz.borough AS pu_borough, dz.borough AS do_borough FROM trips t "
                         f"JOIN zones pz ON t.pu_location_id = pz.location_id "
                         f"LEFT JOIN zones dz ON t.do_location_id = dz.location_id WHERE {_FILTERS}"),
}


class _RSSSampler(threading.Thread):
    """Peak RSS of a process and all its children, sampled every `interval` seconds."""

    def __init__(self, pid: int, interval: float = 0.1):
        super().__init__(daemon=True)
        self.pid, self.interval, self.peak = pid, interval, 0
        self._halt = threading.Event()

    def run(self):
        try:
            root = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return
        while not self._halt.is_set():
            try:
                procs = [root] + root.children(recursive=True)
            except psutil.NoSuchProcess:
                break
            rss = 0
            for p in procs:
                try:
                    rss += p.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            self.peak = max(self.peak, rss)
            self._halt.wait(self.interval)

    def stop(self):
        self._halt.set()
        self.join()


def _swap_used_mb() -> float | None:
    """macOS swap in use (MB), from sysctl vm.swapusage. None off macOS / on error."""
    try:
        out = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True).stdout
    except OSError:
        return None
    m = re.search(r"used\s*=\s*([\d.]+)M", out)
    return float(m.group(1)) if m else None


# --------------------------------------------------------------------------- worker


def _qb(tier: str) -> QueryBuilder:
    paths = C.table_paths(tier)
    qb = QueryBuilder()
    qb.register("trips", DeltaTable(str(paths["trips"])))
    qb.register("zones", DeltaTable(str(paths["zones"])))
    return qb


def _drain(reader, first_only: bool = False):
    """Iterate an Arrow stream, timing the first batch; return (first_batch_s, rows)."""
    rbr = pa.RecordBatchReader.from_stream(reader)
    first, n = None, 0
    for b in rbr:
        if first is None:
            first = time.perf_counter()
        n += b.num_rows
        if first_only:
            break
    return first, n


def worker(exp: str, tier: str, workdir: Path, ref_silver: str | None) -> None:
    paths = C.table_paths(tier)
    out = workdir / "silver"
    rec: dict = {"experiment": exp, "tier": tier}

    if exp in ("probe", "query_only", "ablate_filter", "ablate_inner", "ablate_left", "ablate_bothjoins"):
        sql = W.SILVER_SQL if exp in ("probe", "query_only") else ABLATE_SQL[exp]
        t0 = time.perf_counter()
        reader = _qb(tier).execute(sql)
        rec["exec_s"] = round(time.perf_counter() - t0, 3)  # time for execute() to return
        first, n = _drain(reader, first_only=(exp == "probe"))
        rec["first_batch_s"] = round(first - t0, 3) if first else None
        rec["seconds"] = round(time.perf_counter() - t0, 3)
        rec["rows"] = None if exp == "probe" else n

    elif exp == "write_only":
        reader = pads.dataset(ref_silver).scanner(batch_size=1_000_000).to_reader()
        t0 = time.perf_counter()
        write_deltalake(str(out), reader, mode="overwrite", partition_by=W.SILVER_PARTITIONS)
        rec["seconds"] = round(time.perf_counter() - t0, 3)
        rec["file_count"] = sum(1 for _ in out.rglob("*.parquet"))

    elif exp == "deltars_e2e":
        t0 = time.perf_counter()
        write_deltalake(str(out), _qb(tier).execute(W.SILVER_SQL), mode="overwrite", partition_by=W.SILVER_PARTITIONS)
        rec["seconds"] = round(time.perf_counter() - t0, 3)
        rec["file_count"] = sum(1 for _ in out.rglob("*.parquet"))

    elif exp == "duckdb_e2e":
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("INSTALL delta; LOAD delta")
        con.execute(f"CREATE VIEW trips AS SELECT * FROM delta_scan('{paths['trips']}')")
        con.execute(f"CREATE VIEW zones AS SELECT * FROM delta_scan('{paths['zones']}')")
        reader = con.execute(W.SILVER_SQL).fetch_record_batch(1_000_000)
        t0 = time.perf_counter()
        write_deltalake(str(out), reader, mode="overwrite", partition_by=W.SILVER_PARTITIONS)
        rec["seconds"] = round(time.perf_counter() - t0, 3)
        rec["file_count"] = sum(1 for _ in out.rglob("*.parquet"))

    else:
        rec["error"] = f"unknown experiment {exp}"

    (workdir / "metrics.json").write_text(json.dumps(rec))
    shutil.rmtree(out, ignore_errors=True)  # don't leave big silver tables lying around


# --------------------------------------------------------------------------- orchestrator


def ensure_ref_silver(tier: str) -> Path:
    """Materialize silver once (via DuckDB) to a parquet file, cached, for write_only."""
    out = C.tier_dir(tier) / "_diag" / "silver_ref.parquet"
    if out.exists():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    paths = C.table_paths(tier)
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL delta; LOAD delta")
    con.execute(f"CREATE VIEW trips AS SELECT * FROM delta_scan('{paths['trips']}')")
    con.execute(f"CREATE VIEW zones AS SELECT * FROM delta_scan('{paths['zones']}')")
    print(f"  precomputing reference silver for {tier} -> {out} ...", flush=True)
    con.execute(f"COPY ({W.SILVER_SQL}) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
    return out


def run_experiment(exp: str, tier: str, run_dir: Path, ref_silver: Path | None, timeout: int) -> dict:
    workdir = run_dir / "work" / exp
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    cmd = [sys.executable, "-m", "bench.diagnose_deltars", "--worker", "--experiment", exp,
           "--tier", tier, "--workdir", str(workdir)]
    if ref_silver is not None:
        cmd += ["--ref-silver", str(ref_silver)]
    log = open(workdir / "worker.log", "w")
    swap0 = _swap_used_mb()
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=C.ROOT)
    sampler = _RSSSampler(proc.pid)
    sampler.start()
    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        rc = "timeout"
    wall = time.perf_counter() - t0
    sampler.stop()
    log.close()
    swap1 = _swap_used_mb()

    mpath = workdir / "metrics.json"
    m = json.loads(mpath.read_text()) if mpath.exists() else {"experiment": exp, "tier": tier}
    m["peak_rss_mb"] = round(sampler.peak / 2**20, 1)
    m["swap_growth_mb"] = round(swap1 - swap0, 1) if (swap0 is not None and swap1 is not None) else None
    m["wall_s"] = round(wall, 2)
    m["exit"] = rc
    if rc != 0:
        m["error"] = (workdir / "worker.log").read_text(errors="replace")[-600:]
    return m


def _fmt(x):
    return "-" if x is None else (f"{x:.2f}" if isinstance(x, float) else str(x))


def write_report(run_dir: Path, tier: str, rows: list[dict]) -> None:
    cols = ["experiment", "seconds", "first_batch_s", "exec_s", "rows", "file_count",
            "peak_rss_mb", "swap_growth_mb", "exit"]
    import csv  # local: only needed here, keeps the hot worker path from importing it

    with open(run_dir / "diagnose.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["tier"] + cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in ["tier"] + cols})

    lines = [f"# delta-rs diagnosis — tier {tier}", "",
             "Each experiment ran in its own subprocess. `seconds` is wall time inside the worker;",
             "`swap_growth_mb` is macOS swap-in-use delta across the run (memory-pressure signal).", "",
             "| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows:
        lines.append("| " + " | ".join(_fmt(r.get(c)) for c in cols) + " |")
    (run_dir / "diagnose.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nresults: {run_dir}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="XL")
    ap.add_argument("--experiments", default=",".join(DEFAULT_EXPERIMENTS))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--timeout", type=int, default=7200, help="seconds per experiment")
    ap.add_argument("--run-id", default=None)
    # worker-mode flags
    ap.add_argument("--worker", action="store_true")
    ap.add_argument("--experiment")
    ap.add_argument("--workdir")
    ap.add_argument("--ref-silver")
    a = ap.parse_args(argv)

    if a.worker:
        worker(a.experiment, a.tier, Path(a.workdir), a.ref_silver)
        return

    exps = a.experiments.split(",")
    if not (C.tier_dir(a.tier) / "manifest.json").exists():
        sys.exit(f"tier {a.tier} not prepared (python -m bench.ingest prepare --tier {a.tier})")
    run_id = a.run_id or f"diag-{time.strftime('%Y%m%d-%H%M%S')}-{a.tier}"
    run_dir = C.RESULTS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ref = ensure_ref_silver(a.tier) if "write_only" in exps else None

    rows = []
    for rep in range(a.repeats):
        for exp in exps:
            m = run_experiment(exp, a.tier, run_dir, ref, a.timeout)
            m["rep"] = rep
            rows.append(m)
            print(f"  {exp:14s} {_fmt(m.get('seconds')):>8}s  first={_fmt(m.get('first_batch_s'))}s"
                  f"  peak={_fmt(m.get('peak_rss_mb'))}MB  swap+={_fmt(m.get('swap_growth_mb'))}MB"
                  + ("" if m["exit"] == 0 else f"  [{m['exit']}]"), flush=True)
    write_report(run_dir, a.tier, rows)


if __name__ == "__main__":
    main()
