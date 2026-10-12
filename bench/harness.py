"""Run the benchmark: every engine x tier x repeat in a fresh process, with memory
sampling, correctness checks, and a summary.

  python -m bench.harness --tiers S,M --repeats 3
  python -m bench.harness --tiers SYN --repeats 1          # smoke test
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import shutil
import subprocess
import sys
import threading
import time
from importlib import metadata
from pathlib import Path

import psutil

from bench import config as C
from bench import verify as V
from bench import workload as W
from bench.engines import ENGINES

ALL_STAGES = ["startup"] + W.STAGES


class MemorySampler(threading.Thread):
    """Samples RSS of a process and all its children (e.g. Spark's JVM)."""

    def __init__(self, pid: int, interval: float = 0.1):
        super().__init__(daemon=True)
        self.pid, self.interval = pid, interval
        self.samples: list[tuple[float, int]] = []
        self._stop = threading.Event()

    def run(self):
        try:
            root = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return
        while not self._stop.is_set():
            try:
                procs = [root] + root.children(recursive=True)
                rss = 0
                for p in procs:
                    try:
                        rss += p.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                self.samples.append((time.time(), rss))
            except psutil.NoSuchProcess:
                break
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
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
    }


def run_one(engine: str, tier: str, workdir: Path, stages: list[str], timeout: int) -> dict:
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    if "merge" in stages:
        # each MERGE starts from a pristine copy (copy time is not measured)
        shutil.copytree(C.table_paths(tier)["trips"], workdir / "trips_copy")

    cmd = [sys.executable, "-m", "bench.run_engine", "--engine", engine, "--tier", tier,
           "--workdir", str(workdir), "--stages", ",".join(stages)]
    log = open(workdir / "worker.log", "w")
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=C.ROOT)
    sampler = MemorySampler(proc.pid)
    sampler.start()
    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        for p in psutil.Process(proc.pid).children(recursive=True):
            p.kill()
        proc.kill()
        rc = "timeout"
    wall = time.perf_counter() - t0
    sampler.stop()
    log.close()

    metrics_path = workdir / "metrics.json"
    if metrics_path.exists():
        m = json.loads(metrics_path.read_text())
    else:
        tail = (workdir / "worker.log").read_text()[-1500:]
        if rc == "timeout":
            reason = "timeout"
        elif rc in (-9, 137):
            reason = "killed (SIGKILL; usually the OS out-of-memory killer)"
        else:
            reason = f"worker exited with code {rc}"
        m = {"stages": {}, "notes": [], "crash": f"{reason}\n{tail}"}
    for name, rec in m["stages"].items():
        rec["peak_rss_mb"] = sampler.peak(rec["start"], rec["end"])
    m.update(process_wall_s=wall, process_peak_rss_mb=sampler.peak(), exit=rc)
    return m


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tiers", default="S,M,L,XL")
    ap.add_argument("--engines", default=",".join(ENGINES))
    ap.add_argument("--stages", default=",".join(W.STAGES))
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=7200, help="seconds per engine run")
    ap.add_argument("--keep", action="store_true", help="keep silver/merge outputs")
    ap.add_argument("--run-id", default=time.strftime("%Y%m%d-%H%M%S"))
    a = ap.parse_args(argv)

    engines, stages = a.engines.split(","), a.stages.split(",")
    bad = [e for e in engines if e not in ENGINES] + [s for s in stages if s not in W.STAGES]
    if bad:
        ap.error(f"unknown engine/stage {bad}; engines: {ENGINES}, stages: {W.STAGES}")
    run_dir = C.RESULTS_DIR / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    env = env_info()
    (run_dir / "env.json").write_text(json.dumps(env, indent=2))

    rows_out = []
    tiers = []
    for tier in a.tiers.split(","):
        mf = C.tier_dir(tier) / "manifest.json"
        if not mf.exists():
            print(f"skip {tier}: not prepared (python -m bench.ingest prepare --tier {tier})")
            continue
        tiers.append(tier)
        manifest = json.loads(mf.read_text())
        silver_ref = V.silver_reference(tier)
        print(f"\n=== tier {tier}: {manifest['rows']:,} rows, {manifest['trips_bytes'] / 1e9:.2f} GB ===")

        refs: dict[str, object] = {}
        for rep in range(a.repeats):
            order = engines[rep % len(engines):] + engines[: rep % len(engines)]  # rotate to spread cache effects
            for engine in order:
                wd = run_dir / "work" / tier / engine / str(rep)
                m = run_one(engine, tier, wd, stages, a.timeout)
                checks = V.check_run(wd, tier, m, manifest, silver_ref, refs)
                note = "; ".join(m.get("notes", []))
                for st in ALL_STAGES:
                    if st not in m["stages"]:
                        if st == "startup" or st in stages:
                            rows_out.append(dict(run_id=a.run_id, tier=tier, rows=manifest["rows"], engine=engine,
                                                 rep=rep, stage=st, status="crashed" if "crash" in m else "missing",
                                                 seconds="", peak_rss_mb="", verified="", detail=m.get("crash", "")[:300], notes=note))
                        continue
                    rec = m["stages"][st]
                    rows_out.append(dict(run_id=a.run_id, tier=tier, rows=manifest["rows"], engine=engine, rep=rep,
                                         stage=st, status=rec["status"], seconds=round(rec["seconds"], 3),
                                         peak_rss_mb=rec.get("peak_rss_mb"), verified=checks.get(st, ""),
                                         detail=rec.get("reason", ""), notes=note))
                rows_out.append(dict(run_id=a.run_id, tier=tier, rows=manifest["rows"], engine=engine, rep=rep,
                                     stage="process", status=str(m["exit"]), seconds=round(m["process_wall_s"], 3),
                                     peak_rss_mb=m["process_peak_rss_mb"], verified="", detail="", notes=note))
                summary = ", ".join(
                    f"{s}={m['stages'][s]['seconds']:.1f}s" if m["stages"].get(s, {}).get("status") == "ok"
                    else f"{s}={m['stages'].get(s, {}).get('status', 'crash')}"
                    for s in ["startup"] + stages
                )
                bad = [f"{k}:{v}" for k, v in checks.items() if v not in ("pass", "")]
                print(f"  rep {rep} {engine:9s} {summary} | peak {m['process_peak_rss_mb']} MB"
                      + (f" | CHECK FAILED {bad}" if bad else ""), flush=True)
                if not a.keep:
                    for sub in ("silver", "trips_copy"):
                        shutil.rmtree(wd / sub, ignore_errors=True)

    with open(run_dir / "runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        w.writeheader()
        w.writerows(rows_out)

    from bench import report

    report.build(run_dir)
    print(f"\nresults: {run_dir}")


if __name__ == "__main__":
    main()
