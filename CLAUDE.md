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
