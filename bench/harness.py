"""Run the benchmark: every engine x tier x repeat in a fresh process, with memory
sampling, correctness checks, and a summary.

  python -m bench.harness --tiers S,M --repeats 3
  python -m bench.harness --tiers SYN --repeats 1          # smoke test

runs.csv is appended after every engine run, so an interrupted run keeps what finished;
rebuild the summary with `python -m bench.report results/<run_id>`.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
from importlib import metadata

import psutil

from bench import config as C
from bench import verify as V
from bench import workload as W
from bench.engines import ENGINES, MERGE_CAPABLE

ALL_STAGES = ["startup"] + W.STAGES
FIELDS = ["run_id", "tier", "rows", "engine", "rep", "stage", "status", "seconds", "peak_rss_mb",
          "verified", "detail", "notes"]
RECORDED_ENV = ["BENCH_SPARK_NO_DELTA", "SPARK_DRIVER_MEMORY", "SPARK_EXTRA_CONF", "POLARS_ENGINE",
                "POLARS_MAX_THREADS", "OMP_NUM_THREADS"]


class MemorySampler(threading.Thread):
    """Samples RSS of a process and all its children (e.g. Spark's JVM)."""

    def __init__(self, pid: int, interval: float = 0.1):
        super().__init__(daemon=True)
        self.pid, self.interval = pid, interval
        self.samples: list[tuple[float, int]] = []
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
            self.samples.append((time.time(), rss))
            self._halt.wait(self.interval)

    def stop(self):
        self._halt.set()
        self.join()

    def peak(self, start: float | None = None, end: float | None = None) -> float | None:
        # widen the window by one interval so sub-interval stages still get a sample
        lo = None if start is None else start - self.interval
        hi = None if end is None else end + self.interval
        vals = [r for t, r in self.samples if (lo is None or t >= lo) and (hi is None or t <= hi)]
        return round(max(vals) / 2**20, 1) if vals else None


def env_info() -> dict:
    def v(pkg):
        try:
            return metadata.version(pkg)
        except metadata.PackageNotFoundError:
            return None

    java = None
    try:
        out = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.splitlines()
        java = next(line for line in out if not line.startswith("Picked up"))
    except (OSError, StopIteration):
        pass
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_logical": psutil.cpu_count(),
        "cpu_physical": psutil.cpu_count(logical=False),
        "ram_gb": round(psutil.virtual_memory().total / 2**30, 1),
        "python": platform.python_version(),
        "java": java,
        "versions": {p: v(p) for p in ["pyspark", "delta-spark", "duckdb", "polars", "deltalake", "pyarrow"]},
        "settings": {k: os.environ[k] for k in RECORDED_ENV if k in os.environ},
    }


def run_one(engine: str, tier: str, workdir, stages: list[str], timeout: int, warmup: bool = False) -> dict:
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    if "merge" in stages and engine in MERGE_CAPABLE and not warmup:
        # each MERGE starts from a pristine copy (copy time is not measured)
        shutil.copytree(C.table_paths(tier)["trips"], workdir / "trips_copy")

    cmd = [sys.executable, "-m", "bench.run_engine", "--engine", engine, "--tier", tier,
           "--workdir", str(workdir), "--stages", ",".join(stages)] + (["--warmup"] if warmup else [])
    log = open(workdir / "worker.log", "w")
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=C.ROOT)
    sampler = MemorySampler(proc.pid)
    sampler.start()
    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            for p in psutil.Process(proc.pid).children(recursive=True):
                p.kill()
        except psutil.NoSuchProcess:
            pass
        proc.kill()
        proc.wait()
        rc = "timeout"
    wall = time.perf_counter() - t0
    sampler.stop()
    log.close()

    metrics_path = workdir / "metrics.json"
    m = json.loads(metrics_path.read_text()) if metrics_path.exists() else {"stages": {}, "notes": []}
    if rc != 0:
        if rc == "timeout":
            status, reason = "timeout", f"exceeded {timeout}s"
        elif rc in (-9, 137):
            status, reason = "killed", "SIGKILL, usually the OS out-of-memory killer"
        else:
            status, reason = "crashed", f"worker exited with code {rc}"
        tail = (workdir / "worker.log").read_text(errors="replace")[-1500:]
        m["crash"] = f"{reason}\n{tail}"
        # stages that never finished get the crash status; finished ones keep theirs
        for st in ["startup"] + ([] if warmup else stages):
            if st not in m["stages"]:
                m["stages"][st] = {"status": status, "reason": reason, "seconds": None}
    for rec in m["stages"].values():
        rec["peak_rss_mb"] = sampler.peak(rec["start"], rec["end"]) if "start" in rec else None
    m.update(process_wall_s=wall, process_peak_rss_mb=sampler.peak(), exit=rc)
    return m


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tiers", default="S,M,L,XL")
    ap.add_argument("--engines", default=",".join(ENGINES))
    ap.add_argument("--stages", default=",".join(W.STAGES))
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=7200, help="seconds per engine run")
    ap.add_argument("--no-warmup", action="store_true", help="skip the untimed start/stop warm-up per engine")
    ap.add_argument("--keep", action="store_true", help="keep silver/merge outputs")
    ap.add_argument("--run-id", default=time.strftime("%Y%m%d-%H%M%S"))
    a = ap.parse_args(argv)

    engines, stages = a.engines.split(","), a.stages.split(",")
    bad = [e for e in engines if e not in ENGINES] + [s for s in stages if s not in W.STAGES]
    if bad:
        ap.error(f"unknown engine/stage {bad}; engines: {ENGINES}, stages: {W.STAGES}")
    if any(s in stages for s in ("aggregate", "top_zones")) and "transform_write" not in stages:
        ap.error("aggregate/top_zones read the silver table, so transform_write must run too")

    tiers = []
    for tier in a.tiers.split(","):
        if (C.tier_dir(tier) / "manifest.json").exists():
            tiers.append(tier)
        else:
            print(f"skip {tier}: not prepared (python -m bench.ingest prepare --tier {tier})")
    if not tiers:
        sys.exit("nothing to run: no prepared tiers")

    run_dir = C.RESULTS_DIR / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "env.json").write_text(json.dumps(env_info(), indent=2))
    csv_path = run_dir / "runs.csv"
    new_file = not csv_path.exists()
    csv_file = open(csv_path, "a", newline="")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
    if new_file:
        writer.writeheader()

    if not a.no_warmup:
        print("warm-up (untimed): " + ", ".join(engines), flush=True)
        for engine in engines:
            m = run_one(engine, tiers[0], run_dir / "work" / "_warmup" / engine, stages, a.timeout, warmup=True)
            if m.get("crash"):
                print(f"  ! {engine} warm-up failed: {m['crash'].splitlines()[0]}")

    for tier in tiers:
        manifest = json.loads((C.tier_dir(tier) / "manifest.json").read_text())
        print(f"\n=== tier {tier}: {manifest['rows']:,} rows, {manifest['trips_bytes'] / 1e9:.2f} GB ===", flush=True)
        t0 = time.time()
        ref = V.Reference(tier)
        print(f"  reference computed in {time.time() - t0:.0f}s", flush=True)

        for rep in range(a.repeats):
            order = engines[rep % len(engines):] + engines[: rep % len(engines)]  # rotate start position
            for engine in order:
                wd = run_dir / "work" / tier / engine / str(rep)
                pipe_stages = [s for s in stages if s != "merge"]
                # The pipeline and the MERGE run in separate fresh processes, so MERGE's
                # time and memory don't inherit state (caches, retained RSS) from earlier stages.
                m = run_one(engine, tier, wd, pipe_stages, a.timeout) if pipe_stages else None
                checks = V.check_run(wd, tier, m, manifest, ref) if m else {}
                if "merge" in stages:
                    mm = run_one(engine, tier, wd / "merge", ["merge"], a.timeout)
                    checks.update(V.check_run(wd / "merge", tier, mm, manifest, ref))
                    if m is None:
                        m = mm
                    else:
                        m["stages"]["merge"] = mm["stages"]["merge"]
                        m["notes"] = list(dict.fromkeys(m.get("notes", []) + mm.get("notes", [])))
                note = "; ".join(m.get("notes", []))
                base = dict(run_id=a.run_id, tier=tier, rows=manifest["rows"], engine=engine, rep=rep, notes=note)
                for st in ["startup"] + stages:
                    rec = m["stages"].get(st, {"status": "missing"})
                    secs = rec.get("seconds")
                    writer.writerow(dict(base, stage=st, status=rec["status"],
                                         seconds="" if secs is None else round(secs, 3),
                                         peak_rss_mb=rec.get("peak_rss_mb") or "",
                                         verified=checks.get(st, ""), detail=rec.get("reason", "")))
                writer.writerow(dict(base, stage="process", status=str(m["exit"]), seconds=round(m["process_wall_s"], 3),
                                     peak_rss_mb=m["process_peak_rss_mb"] or "", verified="", detail=""))
                csv_file.flush()

                summary = ", ".join(
                    f"{s}={m['stages'][s]['seconds']:.1f}s" if m["stages"].get(s, {}).get("status") == "ok"
                    else f"{s}={m['stages'].get(s, {}).get('status', '?')}"
                    for s in ["startup"] + stages
                )
                bad = [f"{k}:{v}" for k, v in checks.items() if v != "pass"]
                print(f"  rep {rep} {engine:9s} {summary} | peak {m['process_peak_rss_mb']} MB"
                      + (f" | CHECK FAILED {bad}" if bad else ""), flush=True)
                if not a.keep:
                    for sub in ("silver", "trips_copy", "merge/trips_copy"):
                        shutil.rmtree(wd / sub, ignore_errors=True)
    csv_file.close()

    from bench import report

    report.build(run_dir)
    print(f"\nresults: {run_dir}")


if __name__ == "__main__":
    main()
