# pipeline-engine-bench

**One Delta Lake pipeline, four engines, one machine. When do you actually need Spark?**

This repo runs the *same* data pipeline on **Apache Spark**, **DuckDB**, **Polars** and **delta-rs**
over the same local Delta tables, at four data sizes, and records:

- wall-clock time per stage, including Spark's JVM startup
- peak memory of the whole process tree, including the JVM
- whether each engine got the **right answer**. Every run is checked against a reference computed without any of the benchmarked engines or their SQL. A run that gets the wrong answer doesn't count, however fast it was.

> Status and next steps: [`PLAN.md`](PLAN.md). Headline numbers are in [Results](#results) below;
> the full per-stage tables, raw `runs.csv`, machine/version `env.json` and charts for every run are
> under [`results/`](results/).

---

## Results

One run on a MacBook (macOS, 14 cores, 24 GB RAM, OpenJDK 21), 3 repeats per engine × tier, source
**NYC TLC** yellow-taxi data (2019 for S/M/L; 2019–2023 for XL). Full tables and environment under
[`results/`](results/) — `bench-20261012` for S/M/L, `bench-20261012-xl` for XL. Your numbers will
differ — see [Fairness notes](#fairness-notes-read-before-quoting-numbers).

**Pipeline time** — `startup + transform_write + aggregate + top_zones`, median of 3 valid runs:

| Tier | Rows | DuckDB | Polars | delta-rs | Spark |
|---|---:|---:|---:|---:|---:|
| S | 7.7M | 1.6s | 1.5s | 1.6s | 10s |
| M | 22.6M | 2.4s | 2.0s | 2.5s | 15s |
| L | 84.6M | 5.5s | 5.0s | 22s | 34s |
| XL | 218.1M | 13s | 16s | **792s** | 63s |

![Pipeline time by engine and tier](results/bench-20261012-all/pipeline_time.png)

- **You don't need Spark on one machine, even at 218M rows.** DuckDB and Polars finish the whole
  pipeline in ~5s at L (84.6M rows) and ~13–16s at XL (218M); Spark takes 34s and 63s. DuckDB/Polars
  win at every size tested.
- **Spark's gap stays wide through L, then narrows at XL.** Spark is a flat 6.2–6.3× slower than
  DuckDB through L (S 6.3×, M 6.2×, L 6.2×) — not startup-dominated (startup is ~3s of Spark's 10s at
  S), just a higher per-row cost. Only at XL does the ratio fall, to 4.9×, the first sign Spark's
  fixed costs amortize. So the crossover where Spark pays off sits beyond one machine / past 218M
  rows, not within the sizes most jobs hit.
- **delta-rs scales worst of the fast engines, and the cause is its query path, not Delta writes.**
  It tracks DuckDB/Polars at S and M, then jumps to 22s at L and **792s at XL** (~60× slower than
  DuckDB), almost all of it in `transform_write`. DuckDB writes its silver table through the *same*
  delta-rs writer and finishes that stage in 4.7s (L) / 11s (XL), so the slowdown is in delta-rs's
  embedded DataFusion execution of the joins (`QueryBuilder`), not in writing Delta.
- **Only 3 of 4 engines can MERGE into Delta.** Spark, delta-rs and Polars can; DuckDB's `delta`
  extension can INSERT but not MERGE/UPDATE, so its MERGE is `n/a`. Polars delegates its writes and
  MERGE to delta-rs, so those two share a code path (and near-identical MERGE bars).
- **Spark's memory number is mostly its heap setting.** At L, Spark peaks at 11.3 GB on the default
  (60% of RAM) but 4.9 GB under `SPARK_DRIVER_MEMORY=4g` — at the *same* pipeline time. It grows
  into the heap it's given rather than needing it. Nothing is OOM-killed even at XL (macOS swaps
  instead): there Spark peaks at 12.0 GB, delta-rs 11.0, polars 9.2, and DuckDB stays leanest (5.7 GB).

![Peak memory by engine and tier](results/bench-20261012-all/peak_memory.png)

Every run above passed its correctness check (`3/3` valid) against an engine-independent reference —
a fast wrong answer wouldn't have counted. See
[`summary.md`](results/bench-20261012/summary.md) for the full S/M/L per-stage breakdown,
[`bench-20261012-xl/`](results/bench-20261012-xl/) for XL (its own charts), and the
[`-spark4g`](results/bench-20261012-spark4g/) / [`-polarsauto`](results/bench-20261012-polarsauto/)
folders for the constrained-Spark and default-Polars comparison runs.

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
| **S** | 1 month of 2019 | NYC TLC public parquet |
| **M** | 3 months of 2019 | NYC TLC public parquet |
| **L** | all of 2019 | NYC TLC public parquet |
| **XL** | 2019 + 2020–2023 | NYC TLC public parquet |

All tiers come from the same NYC TLC yellow-taxi parquet family, so the pipeline code is identical at
every size and only the volume changes. Exact row counts are written to
`data/tiers/<tier>/manifest.json` at build time and appear in every results table.

**Why local, and why TLC?** Engines read **local Delta tables**, never over the network: Delta
Sharing is a transfer protocol, so reading through it during the benchmark would time your Wi-Fi,
and only Spark has a native sharing connector anyway. `bench.ingest` downloads the source once,
normalizes it to a canonical schema (timezone-naive timestamps, typed ids, a stable `trip_id`), and
writes the local tables every engine reads.

The tiers here were built from **NYC TLC public parquet** (`SOURCE=tlc`). The project also supports
the open Delta Sharing server, but its `nyctaxi_2019` table is the old coordinate schema
(`pickup_longitude/latitude`) with no `PULocationID`/`DOLocationID`, so it can't feed the zone-join
pipeline — the TLC files carry location ids. Ingest still accepts any unpartitioned shared table
whose columns map onto `COLUMN_ALIASES` in `bench/config.py`.

Open Delta Sharing server: <https://github.com/delta-io/delta-sharing> (profile:
`open-datasets.share`). NYC TLC trip records: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.

## Quick start

Requirements: Python 3.10+, **Java 17 or 21** (for Spark), internet access to PyPI and Maven Central
(Spark downloads the Delta jars on first run). On macOS with Homebrew's keg-only `openjdk@21`, export
`JAVA_HOME` before any Spark command (`make smoke`/`bench`/`bench-xl`):
`export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"`.

```bash
git clone https://github.com/pallavi-chandrashekar/pipeline-engine-bench
cd pipeline-engine-bench
make setup          # .venv + dependencies
make test           # offline tests on synthetic data
make smoke          # 2M synthetic rows through all 4 engines, incl. Spark + Delta

make data-S SOURCE=tlc && make data-M SOURCE=tlc && make data-L SOURCE=tlc
make bench          # S, M, L x 4 engines x 3 repeats
make data-XL SOURCE=tlc && make bench-xl   # optional XL tier (~40 GB disk)
```

Ingest prints, per file, how many rows were kept and any columns that came out NULL after casting —
check those lines the first time. To pull from an unpartitioned shared table instead of TLC:
`make list-share` to see what's offered, then `BENCH_SHARE_TABLE=share.schema.table make data-S`
(dropping `SOURCE=tlc`); see "Why local, and why TLC?" for why the default open share doesn't fit.

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
  the minimum it *needs*. Constrained to `SPARK_DRIVER_MEMORY=4g` it peaks at 4.9 GB on L at the same
  pipeline time (see [Results](#results)).
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
