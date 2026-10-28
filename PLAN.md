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

## Phase 3: Benchmark S, M, L — ✅ DONE (2026-10-12)

- [x] `caffeinate -dims` used to keep the laptop awake during the runs
- [x] Main run `bench-20261012` (3 repeats): **every row 3/3 valid, all checks pass**. Only
  non-runs are DuckDB MERGE (`n/a`, expected). Headline medians:
  - S (7.7M): duckdb 1.6s / polars 1.5s / delta-rs 1.6s / spark 10s
  - M (22.6M): duckdb 2.4s / polars 2.0s / delta-rs 2.5s / spark 15s
  - L (84.6M): duckdb 5.5s / polars 5.0s / delta-rs 22s / spark 34s
  - Spark never becomes competitive at one-machine sizes; at L delta-rs slows to ~Spark territory.
- [x] Comparison runs, each own `--run-id`, all 3/3 pass:
  - [x] `bench-20261012-spark4g` (`SPARK_DRIVER_MEMORY=4g`): Spark peak RSS at L drops **11.3 GB → 4.9 GB**
    (M 7.3 → 4.8) with pipeline time essentially unchanged (L 34s → 30s). Confirms Spark's memory
    number mostly reflects its heap setting, not what it needs — the "memory post" result.
  - [x] `bench-20261012-polarsauto` (`POLARS_ENGINE=auto`): ~indistinguishable from streaming at these
    sizes (L polars 10.8 → 10.5 GB, times within noise). Streaming buys little until data > RAM.
- [x] Committed `results/bench-20261012{,-spark4g,-polarsauto}/` (work dirs gitignored).

> Machine: macOS, 14 cores, 24 GB RAM, OpenJDK 21. Full env in each run's `env.json`.

## Phase 4: XL — ✅ DONE (2026-10-12)

- [x] ~40 GB free disk (tier is ~5 GB on disk; 117 GB free at the time)
- [x] `make data-XL SOURCE=tlc && make bench-xl` → run id `bench-20261012-xl`. **`SOURCE=tlc` is
  required** — the default share source can't feed the pipeline (see Phase 2).
- [x] XL = 218,116,121 rows (2019–2023), 100% rows kept. All four engines 3/3 valid, all checks
  pass; **no crashes or `killed`** (macOS swaps rather than OOM-kills).
  - Pipeline medians: duckdb 13s / polars 16s / spark 63s / **delta-rs 792s** — delta-rs's embedded
    DataFusion query path collapses (~60× DuckDB; 790s of 792s in `transform_write`).
  - Spark's gap vs DuckDB narrows from 6.2× (L) to 4.9× (XL) — the first sign of a crossover.
  - README Results table + findings updated with the XL row.

## Phase 5: Publish

- [x] README "Results" section added: pipeline-time table + pipeline/memory charts + findings
      (Spark not competitive ≤85M rows on one box; delta-rs scales worst of the fast three (query path, not the writer); Spark gap is proportional ~6×;
      3/4 engines MERGE; Spark memory = heap setting). Based on `bench-20261012`.
- [x] Corrected the now-inaccurate share provenance throughout the README (Data tiers table,
      "Why local, and why TLC?", Quick start) — published tiers were built from TLC because the
      open share's `nyctaxi_2019` has no location ids. Added the keg-only `JAVA_HOME` note.
- [ ] (you) Pin the repo on your GitHub profile; add topics: `delta-lake`, `spark`, `duckdb`,
  `polars`, `benchmark`, `data-engineering`

## Spark 3 vs Spark 4 comparison

Does Spark 4's engine close any of the one-machine gap vs Spark 3? Spark 3 and Spark 4 **cannot
share a venv** (conflicting `pyspark` / `delta-spark`), so this runs as a separate environment, not
a fifth engine in the code.

- [x] `requirements-spark3.txt` + a separate `.venv-spark3` (`pyspark 3.5.3`, `delta-spark 3.2.1`;
  both support the `TIMESTAMP_NTZ` schema). Spark 3.5 needs **Java ≤ 17**, so use `openjdk@17`
  (Spark 4 uses 21). Set `JAVA_HOME` to the 17 home for this run.
- [x] `bench/engines/spark_engine.py`: `_to_arrow` shim — `DataFrame.toArrow()` is Spark 4.0+;
  Spark 3.x uses the private `_collect_as_arrow()`. Backward compatible, no change to Spark 4 runs.
- [x] **Set aside the jar cache** (`data/.spark_delta_jars.json`) before the Spark 3 run — it holds
  delta-spark 4.0 jar paths; loading those into Spark 3.5 breaks it. Restore the Spark 4 cache after.
- [x] Run: `JAVA_HOME=<17> .venv-spark3/bin/python -m bench.harness --engines spark
  --tiers S,M,L,XL --repeats 3 --run-id bench-20261012-spark3`.
- [x] Relabel `spark`→`spark4` (existing runs) / `spark3` (new run) in `results/bench-20261012-all`
  and regenerate: Spark 3 now shows as a fifth bar (light blue). Added a Spark 3 column + finding to
  the README Results. **Result:** Spark 3 edges Spark 4 at S/M (within noise), Spark 4 is ~17% faster
  at L and ~21% at XL — its engine work pays off at scale; neither nears DuckDB/Polars on one machine.

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
