# pipeline-engine-bench

**One Delta Lake pipeline, four engines, one machine. When do you actually need Spark?**

This repo runs the *same* data pipeline on **Apache Spark**, **DuckDB**, **Polars** and **delta-rs**
over the same local Delta tables, at four data sizes, and records:

- wall-clock time per stage, including Spark's JVM startup
- peak memory of the whole process tree, including the JVM
- whether each engine got the **right answer**. Every run is checked against a reference computed without any of the benchmarked engines or their SQL. A run that gets the wrong answer doesn't count, however fast it was.

> Results: see [`results/`](results/). Each run folder has a `summary.md`, the raw `runs.csv`,
> machine/version info in `env.json`, and charts.

---

## The pipeline

A bronze → silver → gold flow over NYC yellow-taxi trips:

| Stage | What it does | Why it's here |
|---|---|---|
| `startup` | import, create session/connection, register the bronze Delta tables | JVM startup is a real cost for small jobs |
| `transform_write` | clean bad rows, enrich with pickup/dropoff borough (2 joins), **write a silver Delta table partitioned by year/month** | the bread-and-butter ETL step, read → transform → write |
| `aggregate` | gold: revenue, trips and average distance by borough × year × month × hour, **read from the engine's own silver output** | scan + group-by, and proves the engine can read back what it wrote |
| `top_zones` | gold: top 10 pickup zones per borough by revenue (silver ⋈ zones, `ROW_NUMBER() OVER`) | join + window function |
| `merge` | **MERGE** a corrections batch into a fresh copy of the bronze table: ~0.1% of trips get a tip adjustment, and the same number of new trips are inserted | upserts are where Delta pipelines usually hurt |

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
Note that the shared table is a single 2019 table, so `make data-S` downloads all of 2019 once;
M and L then reuse the download.

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
Ingest prints, per file, how many rows were kept and any columns that came out NULL after casting.
Check those lines the first time.
To use a different shared table: `BENCH_SHARE_TABLE=share.schema.table make data-S`.

Useful knobs:

```bash
.venv/bin/python -m bench.harness --tiers S --engines duckdb,polars --stages aggregate,top_zones --repeats 5
SPARK_DRIVER_MEMORY=12g make bench                                   # default: 60% of RAM
SPARK_EXTRA_CONF='{"spark.sql.shuffle.partitions":"16"}' make bench  # tuned Spark run
POLARS_ENGINE=auto make bench                                        # Polars' default engine
```

These settings are recorded in each run's `env.json` and `summary.md`.

## Method

- **Fresh processes.** For each engine × tier × repeat, the pipeline stages run in one new Python
  process and MERGE in another ([`bench/run_engine.py`](bench/run_engine.py)). No engine benefits from
  another's state, and MERGE's time and memory don't inherit anything from earlier stages.
- **Untimed warm-up.** Before measuring, each engine is started and stopped once. This installs DuckDB's
  `delta` extension and resolves Spark's Delta jars through Ivy; timed Spark runs then load those
  jars from local disk. So **startup** = interpreter imports + session/JVM launch + reading the
  bronze Delta logs, not package downloads.
- **Repeats.** Default 3. Summaries report the **median over valid runs** (min is in `summary.csv`), with
  a "valid runs" column, e.g. `2/3`. Engine order rotates every repeat.
- **Warm OS cache.** Inputs are small enough to sit in the OS page cache after the first read, and each
  MERGE target is copied just before its run, so treat all numbers as **warm-cache** numbers.
- **Memory.** The harness samples RSS of the worker **and all its child processes** every 100 ms
  (Spark's JVM is a child of the Python process). It reports the peak during the pipeline stages, and
  separately the peak of the MERGE process. RSS includes memory-mapped file pages, and 100 ms
  sampling can miss very short spikes.
- **Correctness** ([`bench/verify.py`](bench/verify.py)). The reference is computed **without any
  benchmarked engine and without the shared SQL**: a streaming pass over the bronze snapshot using
  pyarrow compute kernels. A bug in the shared SQL therefore fails every engine instead of passing
  them all. There's a test for exactly that.
  - silver: row count and `SUM(total_amount)` per year × month × pickup borough × dropoff borough
  - gold queries: compared row by row (relative tolerance 1e-6 for floating point)
  - MERGE: final row count and `SUM(total_amount)` vs. the expected outcome recorded when the
    corrections batch was built
- **Failures are data.** Wrong answers, errors, OOM kills (`killed`), timeouts and unsupported
  operations (`n/a`) are recorded and shown in the tables and charts, not silently dropped.
  `runs.csv` is written after every engine run, so an interrupted run keeps what finished.

## Fairness notes (read before quoting numbers)

- **Spark runs in `local[*]` mode on one machine.** That is not what Spark is designed for. Its
  strength is distributing work across a cluster. This benchmark answers a narrower question: *for
  data that fits on one machine, what does that design cost you?*
- **Defaults, with two disclosed exceptions.**
  - **Spark:** driver memory is set to 60% of RAM, because local mode does everything in the driver.
    The Delta extensions are on, the session time zone is UTC, and the UI is off.
  - **Polars:** runs on its **streaming** engine (the default is `auto`). Streaming is what Polars
    recommends for data that may not fit in memory, which is the point of the L/XL tiers. Use
    `POLARS_ENGINE=auto` to compare.
  - DuckDB and delta-rs run with defaults. Tuned runs are welcome; the settings are recorded with the
    results.
- **Spark's memory number mostly reflects its heap setting.** A JVM with a large heap grows into it
  instead of collecting garbage early, so Spark's peak RSS shows what it *used* given 60% of RAM, not
  the minimum it *needs*. Run with `SPARK_DRIVER_MEMORY=4g` to see how it behaves when constrained.
- **Who writes the Delta files?**
  - **Spark** writes and MERGEs with Delta Lake (JVM).
  - **delta-rs** is the Rust implementation.
  - **Polars** delegates Delta writes and MERGE to delta-rs (`sink_delta`, `write_delta`), so the
    Polars and delta-rs MERGE bars measure the same MERGE code path.
  - **DuckDB**'s `delta` extension can INSERT into an *existing* Delta table, but it can't create one or
    UPDATE/MERGE. So DuckDB writes silver by streaming Arrow batches to delta-rs, and its MERGE shows `n/a`.
  - These are facts about each engine today, and part of the result.
- **DuckDB reads** Delta with its `delta` extension (`delta_scan`). If the extension can't be installed
  (offline machine), it falls back to the parquet files of the current snapshot, resolved through the
  Delta log. Which mode ran is recorded in the engine notes.
- **The corrections batch is scattered** (every 1000th trip), so MERGE has to rewrite most data files.
  That is the worst case, and it's also what late-arriving corrections often look like.
- **macOS** compresses and swaps memory rather than killing processes early, so a memory-hungry
  engine on a Mac tends to get *slow* instead of `killed`.
- One machine, one OS, one set of versions. Results are in `env.json`; your numbers will differ.

## Layout

```
bench/
  config.py         tiers, sources, canonical schema
  ingest.py         Delta Sharing / TLC download -> staging -> local Delta tiers
  synth.py          synthetic taxi-shaped tier for offline tests
  workload.py       the SQL + stage definitions
  engines/          one adapter per engine (same 7-method interface)
  run_engine.py     worker: one engine, one tier, one fresh process
  harness.py        orchestration, memory sampling, repeats
  verify.py         engine-independent reference + correctness checks
  report.py         summary.md, CSVs, charts
tests/              offline tests (ingest shapes, share download, reference independence,
                    report rules, end-to-end smoke)
```

**Adding an engine:** implement the interface in `bench/engines/__init__.py`, register it in
`load()` and `ENGINES`, run `make test`. Pull requests welcome: DataFusion Python, Daft, chDB and
Spark Connect would all be interesting additions.

## License

MIT. Data: NYC TLC trip records are published by the NYC Taxi & Limousine Commission and are
subject to its terms of use.
