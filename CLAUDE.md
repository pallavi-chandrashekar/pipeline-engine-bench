# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# pipeline-engine-bench

Benchmark of one Delta Lake pipeline on Spark, DuckDB, Polars and delta-rs. Results are published
publicly, so correctness and fairness come before speed of development.

- Current status and next steps: `PLAN.md`
- Method and fairness rules: `README.md`

## Commands

- `make setup`: creates `.venv` from `requirements.txt`. Spark needs Java 17 or 21.
- `make test`: offline tests on synthetic data. Run these after every change.
- `make smoke`: 2M synthetic rows through all engines, including real Spark + Delta.
- `.venv/bin/python -m bench.harness --tiers SYN --engines duckdb,polars --repeats 1`: quick loop.
- `.venv/bin/python -m bench.report results/<run_id>`: rebuild the summary and charts.
- Single test: `.venv/bin/python -m pytest tests/test_bench.py::test_reference_catches_a_bug_in_the_shared_sql -q`
- Narrow a harness run with `--stages` (e.g. `--stages transform_write,aggregate`); `aggregate`
  and `top_zones` require `transform_write` in the same run because they read its silver output.

## Architecture

The pipeline is **bronze → silver → gold** over NYC taxi trips; `bench/workload.py` defines the
stages (`transform_write`, `aggregate`, `top_zones`, `merge`) and holds the shared SQL.

Execution is split across processes so no engine inherits another's state:

- `bench/harness.py` (orchestrator) loops engine × tier × repeat. For each it spawns a **fresh
  `bench/run_engine.py` worker subprocess** via `subprocess.Popen`, samples the memory of that
  worker *and all its children* (so Spark's JVM counts) every 100 ms, and enforces the timeout.
  The pipeline stages and `merge` run in **separate** worker processes, so MERGE's time/memory
  don't inherit caches from earlier stages. A warm-up run per engine (untimed) installs the DuckDB
  delta extension and resolves Spark's Delta jars first.
- `bench/run_engine.py` (worker) runs one engine through the requested stages in one process,
  timing each and writing `metrics.json` into its workdir **after every stage** — so a crash
  mid-run still records the finished stages. The harness reads that file back.
- `bench/engines/` has one adapter per engine, all implementing the same 7-method interface
  documented at the top of `bench/engines/__init__.py` (`start`, `register`, `transform_write`,
  `register_silver`, `aggregate`, `top_zones`, `merge`, `stop`).
- `bench/verify.py` computes the correctness reference and checks each run against it.
- `bench/ingest.py` downloads from Delta Sharing / TLC and writes local Delta tiers; `bench/synth.py`
  builds the synthetic `SYN` tier used by the offline tests and `make smoke`. `bench/config.py`
  defines tiers, paths and the canonical `TRIP_SCHEMA`. `bench/report.py` writes `summary.md`, CSVs
  and charts.

Each run writes `results/<run_id>/`: `runs.csv` (one row per stage, appended live), `env.json`,
and the report. Stage `status` is one of `ok`, `n/a` (NotSupported, e.g. DuckDB MERGE), `error`,
`skipped`, `crashed`, `killed` (SIGKILL/OOM), `timeout`, `missing`; `verified` is `pass`/`FAIL:…`.

## Rules for changes

- Spark, DuckDB and delta-rs must keep running the **same SQL text** from `bench/workload.py`.
  Polars mirrors that logic in its expression API. A change to one means changing all four.
- `bench/verify.py` must stay independent of the engines and the shared SQL (pyarrow only).
  `tests/test_bench.py::test_reference_catches_a_bug_in_the_shared_sql` guards this.
- Any non-default engine setting must be disclosed in the README's "Fairness notes" and recorded
  in `env.json` (`RECORDED_ENV` in `bench/harness.py`).
- Never publish numbers from `BENCH_SPARK_NO_DELTA=1` runs.
- New engine: implement the interface in `bench/engines/__init__.py`, add it to `ENGINES`,
  `MERGE_CAPABLE` (if it can MERGE) and `load()`.
- Data and work dirs are gitignored. Commit only `results/<run_id>/` summaries, CSVs, charts and env.
