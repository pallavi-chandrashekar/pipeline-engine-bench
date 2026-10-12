# Results: run `bench-20261012-polarsauto`

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O | 14 cores / 14 threads | 24.0 GB RAM | Python 3.14.3 | openjdk version "21.0.12.1" 2026-08-18

Versions: pyspark 4.0.1, delta-spark 4.0.0, duckdb 1.5.6, polars 2.0.0, deltalake 1.6.6, pyarrow 23.0.1

**Pipeline time** = startup + transform_write + aggregate + top_zones, median over valid runs.
A run counts as valid only if it finished AND passed its correctness check.
MERGE is reported separately because not every engine can MERGE into Delta.
Memory = peak RSS of the whole process tree (incl. Spark's JVM) during those stages.

| Tier | Rows | Engine | Pipeline | Valid runs | Peak memory | MERGE | MERGE peak | Checks |
|---|---:|---|---:|---:|---:|---:|---:|---|
| S | 7,696,080 | spark | 9.35s | 3/3 | 4.7 GB | 5.51s | 5.2 GB | pass |
| S | 7,696,080 | duckdb | 1.55s | 3/3 | 1.6 GB | n/a | - | pass |
| S | 7,696,080 | polars | 1.49s | 3/3 | 2.4 GB | 0.97s | 1.4 GB | pass |
| S | 7,696,080 | delta-rs | 1.59s | 3/3 | 2.5 GB | 0.94s | 1.4 GB | pass |
| M | 22,611,807 | spark | 12s | 3/3 | 11.0 GB | 8.88s | 9.2 GB | pass |
| M | 22,611,807 | duckdb | 2.44s | 3/3 | 2.7 GB | n/a | - | pass |
| M | 22,611,807 | polars | 1.94s | 3/3 | 6.0 GB | 2.73s | 3.0 GB | pass |
| M | 22,611,807 | delta-rs | 2.40s | 3/3 | 5.1 GB | 2.65s | 3.0 GB | pass |
| L | 84,597,002 | spark | 35s | 3/3 | 10.9 GB | 19s | 8.2 GB | pass |
| L | 84,597,002 | duckdb | 5.35s | 3/3 | 4.6 GB | n/a | - | pass |
| L | 84,597,002 | polars | 5.50s | 3/3 | 10.5 GB | 10s | 6.6 GB | pass |
| L | 84,597,002 | delta-rs | 22s | 3/3 | 11.6 GB | 10s | 6.7 GB | pass |

## Per-stage medians (valid runs)

| Tier | Engine | Stage | Median | Min | Valid/total | Peak memory | Check |
|---|---|---|---:|---:|---:|---:|---|
| S | spark | startup | 3.19s | 3.18s | 3/3 | 651 MB | - |
| S | spark | transform_write | 4.77s | 4.56s | 3/3 | 4.6 GB | pass |
| S | spark | aggregate | 0.73s | 0.73s | 3/3 | 4.7 GB | pass |
| S | spark | top_zones | 0.65s | 0.65s | 3/3 | 4.7 GB | pass |
| S | spark | merge | 5.51s | 5.35s | 3/3 | 5.2 GB | pass |
| S | duckdb | startup | 0.06s | 0.06s | 3/3 | 82 MB | - |
| S | duckdb | transform_write | 1.40s | 1.39s | 3/3 | 1.6 GB | pass |
| S | duckdb | aggregate | 0.04s | 0.04s | 3/3 | 831 MB | pass |
| S | duckdb | top_zones | 0.05s | 0.05s | 3/3 | 827 MB | pass |
| S | duckdb | merge | n/a | - | 0/3 | - | - |
| S | polars | startup | 0.06s | 0.06s | 3/3 | 75 MB | - |
| S | polars | transform_write | 1.36s | 1.35s | 3/3 | 2.4 GB | pass |
| S | polars | aggregate | 0.03s | 0.03s | 3/3 | 2.0 GB | pass |
| S | polars | top_zones | 0.04s | 0.03s | 3/3 | 1.9 GB | pass |
| S | polars | merge | 0.97s | 0.96s | 3/3 | 1.4 GB | pass |
| S | delta-rs | startup | 0.01s | 0.01s | 3/3 | 546 MB | - |
| S | delta-rs | transform_write | 1.39s | 1.39s | 3/3 | 2.5 GB | pass |
| S | delta-rs | aggregate | 0.16s | 0.15s | 3/3 | 2.0 GB | pass |
| S | delta-rs | top_zones | 0.03s | 0.03s | 3/3 | 2.0 GB | pass |
| S | delta-rs | merge | 0.94s | 0.94s | 3/3 | 1.4 GB | pass |
| M | spark | startup | 3.33s | 3.32s | 3/3 | 639 MB | - |
| M | spark | transform_write | 6.84s | 6.69s | 3/3 | 11.0 GB | pass |
| M | spark | aggregate | 1.04s | 0.99s | 3/3 | 10.9 GB | pass |
| M | spark | top_zones | 0.92s | 0.84s | 3/3 | 10.5 GB | pass |
| M | spark | merge | 8.88s | 8.72s | 3/3 | 9.2 GB | pass |
| M | duckdb | startup | 0.15s | 0.06s | 3/3 | 92 MB | - |
| M | duckdb | transform_write | 2.09s | 2.08s | 3/3 | 2.7 GB | pass |
| M | duckdb | aggregate | 0.07s | 0.07s | 3/3 | 1.4 GB | pass |
| M | duckdb | top_zones | 0.12s | 0.11s | 3/3 | 1.3 GB | pass |
| M | duckdb | merge | n/a | - | 0/3 | - | - |
| M | polars | startup | 0.10s | 0.06s | 3/3 | 114 MB | - |
| M | polars | transform_write | 1.67s | 1.63s | 3/3 | 6.0 GB | pass |
| M | polars | aggregate | 0.08s | 0.07s | 3/3 | 4.8 GB | pass |
| M | polars | top_zones | 0.09s | 0.08s | 3/3 | 4.8 GB | pass |
| M | polars | merge | 2.73s | 2.67s | 3/3 | 3.0 GB | pass |
| M | delta-rs | startup | 0.01s | 0.01s | 3/3 | 706 MB | - |
| M | delta-rs | transform_write | 2.13s | 2.12s | 3/3 | 5.1 GB | pass |
| M | delta-rs | aggregate | 0.17s | 0.17s | 3/3 | 5.0 GB | pass |
| M | delta-rs | top_zones | 0.09s | 0.09s | 3/3 | 2.9 GB | pass |
| M | delta-rs | merge | 2.65s | 2.62s | 3/3 | 3.0 GB | pass |
| L | spark | startup | 3.95s | 3.27s | 3/3 | 626 MB | - |
| L | spark | transform_write | 25s | 23s | 3/3 | 10.9 GB | pass |
| L | spark | aggregate | 3.14s | 2.99s | 3/3 | 6.9 GB | pass |
| L | spark | top_zones | 2.98s | 1.80s | 3/3 | 7.5 GB | pass |
| L | spark | merge | 19s | 19s | 3/3 | 8.2 GB | pass |
| L | duckdb | startup | 0.17s | 0.17s | 3/3 | 220 MB | - |
| L | duckdb | transform_write | 4.58s | 4.51s | 3/3 | 4.6 GB | pass |
| L | duckdb | aggregate | 0.23s | 0.23s | 3/3 | 2.4 GB | pass |
| L | duckdb | top_zones | 0.37s | 0.35s | 3/3 | 2.4 GB | pass |
| L | duckdb | merge | n/a | - | 0/3 | - | - |
| L | polars | startup | 0.11s | 0.11s | 3/3 | 132 MB | - |
| L | polars | transform_write | 4.81s | 4.79s | 3/3 | 10.5 GB | pass |
| L | polars | aggregate | 0.28s | 0.28s | 3/3 | 6.1 GB | pass |
| L | polars | top_zones | 0.30s | 0.28s | 3/3 | 5.7 GB | pass |
| L | polars | merge | 10s | 10s | 3/3 | 6.6 GB | pass |
| L | delta-rs | startup | 0.01s | 0.01s | 3/3 | 92 MB | - |
| L | delta-rs | transform_write | 22s | 21s | 3/3 | 11.6 GB | pass |
| L | delta-rs | aggregate | 0.31s | 0.30s | 3/3 | 1.5 GB | pass |
| L | delta-rs | top_zones | 0.25s | 0.24s | 3/3 | 1.5 GB | pass |
| L | delta-rs | merge | 10s | 10s | 3/3 | 6.7 GB | pass |

## Not run / failed

- S / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table
- M / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table
- L / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table

## Engine notes

- spark: driver_memory=14g; delta_jars=local
- duckdb: read=delta_scan; write=delta-rs
- polars: polars engine=auto; write+merge=delta-rs

Settings: `POLARS_ENGINE=auto`
