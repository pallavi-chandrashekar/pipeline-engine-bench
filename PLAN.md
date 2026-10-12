# Plan

Working plan for getting from "code complete" to "published results + posts".
Tick boxes as you go, and keep this file in the repo so the status is visible.

## Where things stand

**Done and tested** (in a Linux sandbox without access to Maven Central or the data sources):

- Ingest → local Delta tiers, synthetic tier, four engine adapters, harness, independent
  correctness reference, report + charts
- 6 offline tests passing (`make test`), plus 20M-row synthetic runs. All four engines produced
  correct results, with Spark in its no-Delta test mode.

**Written but never run for real.** These are the first things to verify:

| Path | Risk | Verified by |
|---|---|---|
| Spark + Delta (jar resolution, Delta read/write, MERGE) | medium: never executed | Phase 1 `make smoke` |
| Spark's cached Delta jars (`data/.spark_delta_jars.json`) | medium: depends on what `spark.jars` holds after Ivy resolution | Phase 1, 2nd run's notes show `delta_jars=local` |
| DuckDB's native `delta_scan` | low | Phase 1 notes show `read=delta_scan` |
| Delta Sharing download (table name, schema, partitioning) | medium: table name and columns are from memory, not a live listing | Phase 2 `make list-share` |
| NYC TLC downloads for XL | low | Phase 4 |

## Phase 1: Local setup and smoke test (~30 min)

- [ ] `java -version` shows 17 or 21 (`brew install openjdk@21` on macOS)
- [ ] `make setup`
- [ ] `make test`: 6 passed
- [ ] `make smoke`, twice. Then check `results/smoke/summary.md`:
  - [ ] all four engines show `pass` in Checks
  - [ ] Spark MERGE has a time, not `n/a` or `error`
  - [ ] the 2nd run's engine notes say `delta_jars=local` and `read=delta_scan`
- If it fails, look at the worker log: `results/smoke/work/SYN/<engine>/0/worker.log`.
  Likely suspects:
  - **Delta jar errors** → delete `data/.spark_delta_jars.json` and rerun. If that fixes it, the cache
    is the bug.
  - **`TIMESTAMP_NTZ` errors from Spark** → protocol mismatch between delta-rs and delta-spark; pin
    versions.

## Phase 2: Real data (~1 hr, mostly downloading)

- [ ] `make list-share` and confirm:
  - [ ] `delta_sharing.default.nyctaxi_2019` exists. If not, pick the closest table with
    `BENCH_SHARE_TABLE=...`, or use `SOURCE=tlc`.
  - [ ] the column list maps onto `COLUMN_ALIASES` in `bench/config.py`, adding aliases if needed
  - [ ] the table is unpartitioned (ingest refuses partitioned shares)
- [ ] `make data-S`. Read the per-file lines: rows kept should be ~99%+, with **no unexpected
  nulls after cast**.
- [ ] Sanity check the timezone handling: also build S from TLC and compare row counts and the
  hour-of-day histogram for 2019-01. They should match closely.
  ```
  BENCH_DATA_DIR=data_tlc .venv/bin/python -m bench.ingest prepare --tier S --source tlc
  ```
- [ ] `make data-M data-L`

## Phase 3: Benchmark S, M, L (~1–3 hrs)

- [ ] Close heavy apps, plug in power, and don't let the laptop sleep
  (`caffeinate -dims make bench` on macOS)
- [ ] `make bench` (3 repeats)
- [ ] Review `summary.md`. Every row should be `3/3` valid; investigate anything that isn't before
  quoting numbers.
- [ ] Optional comparison runs, each with its own `--run-id`:
  - [ ] `SPARK_DRIVER_MEMORY=4g`: how Spark behaves when constrained (answers the "the heap setting
    inflates Spark's memory" objection)
  - [ ] `POLARS_ENGINE=auto`: Polars' default engine
- [ ] Commit `results/<run_id>/`. The work dirs are gitignored.

## Phase 4: XL (optional, a long run)

- [ ] ~40 GB free disk
- [ ] `make data-XL && make bench-xl`
- [ ] An engine that crashes or gets `killed` at XL is a result, not a failure. It shows up in
  the charts.

## Phase 5: Publish

- [ ] Replace the README's "Results" pointer with the headline chart + 3–4 bullet findings
- [ ] Add a short "Findings" section: where Spark becomes competitive (if it does), MERGE
  support, memory behaviour
- [ ] Pin the repo on your GitHub profile; add topics: `delta-lake`, `spark`, `duckdb`, `polars`,
  `benchmark`, `data-engineering`

## Phase 6: LinkedIn posts built from the results

1. **Benchmark headline.** The pipeline chart, the crossover finding, repo link in first comment.
2. **"Only 2 of 4 engines can MERGE into Delta."** Includes the fact that Polars delegates to delta-rs.
3. **Method post.** "A fast wrong answer isn't a result": how the correctness reference works.
   This is the senior-engineer angle.
4. **Memory post.** Spark heap at 60% vs 4g: what peak memory actually measures.

Post only numbers from your own runs, on your personal laptop.

## Backlog (not blocking publication)

- [ ] Memory: report USS, or cgroup `memory.peak` on Linux, alongside RSS (RSS counts mmapped
  file pages)
- [ ] DuckDB: try native `INSERT` into a pre-created Delta table instead of writing via delta-rs,
  as an extra variant
- [ ] Add engines: DataFusion Python, Daft, chDB, Spark Connect
- [ ] Cold-cache mode (drop OS caches between runs; needs sudo)
- [ ] GitHub Actions: run `make test` on push

## Confidentiality checklist (before every push or post)

- [ ] Only public data (open share, NYC TLC, synthetic)
- [ ] Personal laptop and personal accounts; no work machines, credentials or cloud credits
- [ ] No employer systems, volumes, configs or incidents in code, results, README or posts
