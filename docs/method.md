# Method & fairness notes

[← back to README](../README.md)

## Method

- **Fresh processes.** For each engine × tier × repeat, the pipeline stages run in one new Python
  process and MERGE in another ([`bench/run_engine.py`](../bench/run_engine.py)). No engine benefits from
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
- **Correctness** ([`bench/verify.py`](../bench/verify.py)). The reference is computed **without any
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
  pipeline time (see [Results](../README.md#results)).
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
