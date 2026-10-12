# pipeline-engine-bench

**One Delta Lake pipeline, four engines, one machine. When do you actually need Spark?**

This repo runs the *same* data pipeline on **Apache Spark**, **DuckDB**, **Polars** and **delta-rs**
over the same local Delta tables, at four data sizes, and records:

- wall-clock time per stage, including Spark's JVM startup
- peak memory of the whole process tree, including the JVM
- whether each engine got the **right answer**. Every run is checked against an independent reference. A fast wrong answer is not a result.

> Results: see [`results/`](results/). Each run folder has a `summary.md`, the raw `runs.csv`,
> machine/version info in `env.json`, and charts.

---

## The pipeline

A realistic bronze → silver → gold flow over NYC yellow-taxi trips:

| Stage | What it does | Why it's here |
|---|---|---|
| `startup` | import, create session/connection, register the Delta tables | JVM startup is a real cost for small jobs |
| `transform_write` | clean bad rows, enrich with pickup/dropoff borough (2 joins), **write a Delta table partitioned by year/month** | the bread-and-butter ETL step, read → transform → write |
| `aggregate` | revenue, trips, and average distance by borough × year × month × hour | scan + join + group-by |
| `top_zones` | top 10 pickup zones per borough by revenue (`ROW_NUMBER() OVER`) | window functions |
| `merge` | **MERGE** a corrections batch into a fresh copy of the bronze table: ~0.1% of trips get a tip adjustment and the same number of new trips are inserted | upserts are where Delta pipelines usually hurt |

Spark, DuckDB and delta-rs (via its embedded DataFusion) run **the exact same SQL text**
([`bench/workload.py`](bench/workload.py)). Polars runs the same logic through its lazy expression API.

**Pipeline time** = `startup + transform_write + aggregate + top_zones`. MERGE is reported
separately because not every engine can MERGE into Delta (see below).

## Data tiers

| Tier | Data | Source |
|---|---|---|
| **S** | 1 month of 2019 | open Delta Sharing server |
| **M** | 3 months of 2019 | open Delta Sharing server |
| **L** | all of 2019 | open Delta Sharing server |
| **XL** | 2019 + 2020–2023 | share (2019) + NYC TLC public parquet (2020–2023) |

All tiers come from **one table family**, so the pipeline code is identical at every size and only
the volume changes. Exact row counts are written to `data/tiers/<tier>/manifest.json` at build time
and appear in every results table.

**Why download first?** Delta Sharing is a transfer protocol. If engines read through it during the
benchmark you'd be timing your Wi-Fi, and only Spark has a native sharing connector anyway. So
`bench.ingest` downloads the shared table once, normalizes it to a canonical schema (timezone-naive
timestamps, typed ids, a stable `trip_id`), and writes **local Delta tables** that every engine reads.

Open Delta Sharing server: <https://github.com/delta-io/delta-sharing> (profile:
`open-datasets.share`). NYC TLC trip records: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.

## Quick start

Requirements: Python 3.10+, **Java 17 or 21** (for Spark), internet access to PyPI and Maven Central
(Spark downloads the Delta jars on first run).

```bash
git clone https://github.com/pallavi-chandrashekar/pipeline-engine-bench
cd pipeline-engine-bench
make setup          # .venv + dependencies
make test           # offline tests on synthetic data
make smoke          # 2M synthetic rows through all 4 engines, incl. Spark + Delta

make list-share     # see what the open Delta Sharing server offers
make data-S data-M data-L
make bench          # S, M, L x 4 engines x 3 repeats
make data-XL bench-xl
```

If the share is unavailable, build any tier purely from TLC files: `make data-L SOURCE=tlc`.
To use a different shared table: `BENCH_SHARE_TABLE=share.schema.table make data-S`.

Useful knobs:

```bash
.venv/bin/python -m bench.harness --tiers S --engines duckdb,polars --stages aggregate,top_zones --repeats 5
SPARK_DRIVER_MEMORY=12g make bench                                   # default: 60% of RAM
SPARK_EXTRA_CONF='{"spark.sql.shuffle.partitions":"16"}' make bench  # tuned Spark run
```

## Method

- **Fresh process per run.** Each engine × tier × repeat runs in a new Python process
  ([`bench/run_engine.py`](bench/run_engine.py)), so no engine benefits from another's warm state.
- **Repeats and rotation.** Default 3 repeats; summaries report the **median** (min is in `summary.csv`).
  Engine order rotates every repeat so no engine is always first on a cold OS page cache.
- **Memory.** The harness samples RSS of the worker **and all its child processes** every 100 ms
  (Spark's JVM is a child of the Python process). Reported as process peak.
- **Fresh MERGE target.** Each run merges into its own copy of the bronze table (copy time not measured).
- **Correctness** ([`bench/verify.py`](bench/verify.py)):
  - silver table: row count and `SUM(total_amount)` vs. a reference computed independently from the bronze files
  - query results: compared row-by-row with an independent reference (relative tolerance 1e-6 for floating point)
  - MERGE: final row count and `SUM(total_amount)` vs. the expected outcome recorded when the corrections batch was built
- **Failures are data.** OOM kills, timeouts and unsupported operations are recorded and shown in the
  charts (`crashed`, `timeout`, `n/a`), not silently dropped.

## Fairness notes (read before quoting numbers)

- **Spark runs in `local[*]` mode on one machine.** That is not what Spark is designed for. Its
  strength is distributing work across a cluster. This benchmark answers a narrower question: *for
  data that fits on one machine, what does that design cost you?*
- **Defaults, not tuning.** Every engine runs with defaults, except Spark's driver memory (60% of
  RAM, since local mode does everything in the driver), its Delta extensions, and a disabled UI.
  Tuned runs are welcome; record the settings with the results.
- **Who writes the Delta files?** Spark writes and MERGEs with Delta Lake (JVM). delta-rs is the Rust
  implementation. **Polars delegates Delta writes and MERGE to delta-rs** (`sink_delta`, `write_delta`).
  **DuckDB writes by streaming Arrow batches to delta-rs**, and **cannot MERGE into Delta**, so its
  MERGE shows `n/a`. These are facts about each engine today, and part of the result.
- **DuckDB reads** Delta with its `delta` extension (`delta_scan`). If the extension can't be installed
  (offline machine), it falls back to the parquet files of the current snapshot, resolved through the
  Delta log. Which mode ran is recorded in the engine notes.
- **The corrections batch is scattered** (every 1000th trip), so MERGE has to rewrite most data files.
  That is the worst case, and it's also what late-arriving corrections often look like.
- One machine, one OS, one set of versions. Results are in `env.json`; your numbers will differ.

## Layout

```
bench/
  config.py         tiers, sources, canonical schema
  ingest.py         Delta Sharing / TLC download -> staging -> local Delta tiers
  synth.py          synthetic taxi-shaped tier for offline tests
  workload.py       the SQL + stage definitions
  engines/          one adapter per engine (same 6-method interface)
  run_engine.py     worker: one engine, one tier, one fresh process
  harness.py        orchestration, memory sampling, repeats
  verify.py         correctness checks
  report.py         summary.md, CSVs, charts
tests/              offline tests (ingest shapes, checks, end-to-end smoke)
```

**Adding an engine:** implement the interface in `bench/engines/__init__.py`, register it in
`load()` and `ENGINES`, run `make test`. Pull requests welcome: DataFusion Python, Daft, chDB and
Spark Connect would all be interesting additions.

## License

MIT. Data: NYC TLC trip records are published by the NYC Taxi & Limousine Commission and are
subject to its terms of use.
