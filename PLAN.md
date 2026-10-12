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
| ~~Spark + Delta (jar resolution, Delta read/write, MERGE)~~ | ~~medium: never executed~~ | ✅ Phase 1 `make smoke`: pass, MERGE 4.8s |
| ~~Spark's cached Delta jars (`data/.spark_delta_jars.json`)~~ | ~~medium~~ | ✅ 2nd run notes `delta_jars=local`, no Maven re-fetch |
| ~~DuckDB's native `delta_scan`~~ | ~~low~~ | ✅ Phase 1 notes show `read=delta_scan` |
| ~~Delta Sharing download (table name, schema, partitioning)~~ | risk hit: share schema has no location IDs | ⚠️ Phase 2 — share unusable, fell back to TLC |
| ~~NYC TLC downloads~~ | ~~low~~ | ✅ Phase 2: S/M/L built from TLC, 100% rows kept |

## Phase 1: Local setup and smoke test (~30 min) — ✅ DONE (2026-10-12)

- [x] `java -version` shows 17 or 21 (`brew install openjdk@21` on macOS) — installed OpenJDK 21.0.12.1
- [x] `make setup` — `.venv` on Python 3.14.3; all deps installed (pyspark 4.0.1, delta-spark 4.0.0, duckdb 1.5.6, polars 2.0.0, deltalake 1.6.6)
- [x] `make test`: 6 passed
- [x] `make smoke`, twice. Then check `results/smoke/summary.md`:
  - [x] all four engines show `pass` in Checks
  - [x] Spark MERGE has a time, not `n/a` or `error` — 4.8s
  - [x] the 2nd run's engine notes say `delta_jars=local` and `read=delta_scan`

> **openjdk@21 is keg-only** (not symlinked onto PATH). Every command that launches Spark —
> `make smoke`, `make bench`, `make bench-xl` — needs `JAVA_HOME` exported first:
> `export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"`
> (or add it to `~/.zshrc`). `make test`, `make data-*` and the non-Spark engines don't need it.
- If it fails, look at the worker log: `results/smoke/work/SYN/<engine>/0/worker.log`.
  Likely suspects:
  - **Delta jar errors** → delete `data/.spark_delta_jars.json` and rerun. If that fixes it, the cache
    is the bug.
  - **`TIMESTAMP_NTZ` errors from Spark** → protocol mismatch between delta-rs and delta-spark; pin
    versions.

## Phase 2: Real data — ✅ DONE via TLC (2026-10-12)

> **The open share can't feed this pipeline.** `make list-share` shows
> `delta_sharing.default.nyctaxi_2019` exists (31 files, 1.74 GB, ~84.4M rows) but it is the
> **old lat/long schema** (`pickup_longitude/latitude`, `dropoff_longitude/latitude`) with **no
> `PULocationID`/`DOLocationID`**. The silver stage joins trips to the zone lookup on
> `pu_location_id`/`do_location_id`, so ingest would hard-fail (`no column for 'pu_location_id'`).
> `nyctaxi_2019_part` is the partitioned variant (ingest refuses it) and has the same columns.
> **Resolution: built S/M/L from `SOURCE=tlc`**, which uses the location-id schema. Published
> results therefore say `source: tlc`, not the share.

- [x] `make list-share`: target table found, but schema lacks location IDs (see note above)
- [x] `make data-S SOURCE=tlc`: 7,696,617/7,696,617 rows kept (100%); only null is `passenger_count`
  (~0.4%), a column no stage uses — harmless. Validated end-to-end (duckdb+polars, 1 repeat): all
  stages `pass` against the independent reference.
- [x] `make data-M SOURCE=tlc`: 22,611,807 rows, 0.50 GB
- [x] `make data-L SOURCE=tlc`: 84,597,002 rows, 1.87 GB (matches the share's ~84.4M)
- [n/a] Timezone cross-check (build S from share vs TLC): the share is unusable, so there is
  nothing to compare against. TLC is the only source.

> **Makefile fix applied:** `data-S/M/L/XL` were listed in `.PHONY`, so GNU Make skipped the
> `data-%` pattern rule and `make data-S` printed "Nothing to be done". Removed them from `.PHONY`
> (the pattern rule handles them); the README quick-start commands now work.

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
