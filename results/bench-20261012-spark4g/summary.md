# Results: run `bench-20261012-spark4g`

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O | 14 cores / 14 threads | 24.0 GB RAM | Python 3.14.3 | openjdk version "21.0.12.1" 2026-08-18

Versions: pyspark 4.0.1, delta-spark 4.0.0, duckdb 1.5.6, polars 2.0.0, deltalake 1.6.6, pyarrow 23.0.1

**Pipeline time** = startup + transform_write + aggregate + top_zones, median over valid runs.
A run counts as valid only if it finished AND passed its correctness check.
MERGE is reported separately because not every engine can MERGE into Delta.
Memory = peak RSS of the whole process tree (incl. Spark's JVM) during those stages.

| Tier | Rows | Engine | Pipeline | Valid runs | Peak memory | MERGE | MERGE peak | Checks |
|---|---:|---|---:|---:|---:|---:|---:|---|
| S | 7,696,080 | spark | 9.47s | 3/3 | 4.5 GB | 5.46s | 4.3 GB | pass |
| S | 7,696,080 | duckdb | 1.56s | 3/3 | 1.5 GB | n/a | - | pass |
| S | 7,696,080 | polars | 1.49s | 3/3 | 2.4 GB | 0.95s | 1.4 GB | pass |
| S | 7,696,080 | delta-rs | 1.54s | 3/3 | 2.6 GB | 0.95s | 1.3 GB | pass |
| M | 22,611,807 | spark | 13s | 3/3 | 4.8 GB | 9.49s | 4.7 GB | pass |
| M | 22,611,807 | duckdb | 2.52s | 3/3 | 2.6 GB | n/a | - | pass |
| M | 22,611,807 | polars | 1.97s | 3/3 | 6.1 GB | 2.70s | 2.8 GB | pass |
| M | 22,611,807 | delta-rs | 2.44s | 3/3 | 5.1 GB | 2.68s | 2.9 GB | pass |
| L | 84,597,002 | spark | 30s | 3/3 | 4.9 GB | 20s | 4.8 GB | pass |
| L | 84,597,002 | duckdb | 5.23s | 3/3 | 4.7 GB | n/a | - | pass |
| L | 84,597,002 | polars | 5.47s | 3/3 | 10.7 GB | 10s | 6.8 GB | pass |
| L | 84,597,002 | delta-rs | 23s | 3/3 | 11.7 GB | 10s | 6.2 GB | pass |

## Per-stage medians (valid runs)

| Tier | Engine | Stage | Median | Min | Valid/total | Peak memory | Check |
|---|---|---|---:|---:|---:|---:|---|
| S | spark | startup | 3.23s | 3.20s | 3/3 | 602 MB | - |
| S | spark | transform_write | 4.86s | 4.66s | 3/3 | 4.4 GB | pass |
| S | spark | aggregate | 0.74s | 0.73s | 3/3 | 4.5 GB | pass |
| S | spark | top_zones | 0.64s | 0.62s | 3/3 | 4.5 GB | pass |
| S | spark | merge | 5.46s | 5.44s | 3/3 | 4.3 GB | pass |
| S | duckdb | startup | 0.06s | 0.06s | 3/3 | 376 MB | - |
| S | duckdb | transform_write | 1.41s | 1.38s | 3/3 | 1.5 GB | pass |
| S | duckdb | aggregate | 0.04s | 0.04s | 3/3 | 993 MB | pass |
| S | duckdb | top_zones | 0.06s | 0.05s | 3/3 | 816 MB | pass |
| S | duckdb | merge | n/a | - | 0/3 | - | - |
| S | polars | startup | 0.06s | 0.05s | 3/3 | 1.5 GB | - |
| S | polars | transform_write | 1.37s | 1.33s | 3/3 | 2.4 GB | pass |
| S | polars | aggregate | 0.03s | 0.03s | 3/3 | 1.9 GB | pass |
| S | polars | top_zones | 0.04s | 0.03s | 3/3 | 1.9 GB | pass |
| S | polars | merge | 0.95s | 0.95s | 3/3 | 1.4 GB | pass |
| S | delta-rs | startup | 0.01s | 0.01s | 3/3 | 506 MB | - |
| S | delta-rs | transform_write | 1.34s | 1.34s | 3/3 | 2.6 GB | pass |
| S | delta-rs | aggregate | 0.15s | 0.15s | 3/3 | 2.0 GB | pass |
| S | delta-rs | top_zones | 0.03s | 0.03s | 3/3 | 2.0 GB | pass |
| S | delta-rs | merge | 0.95s | 0.94s | 3/3 | 1.3 GB | pass |
| M | spark | startup | 3.25s | 3.24s | 3/3 | 614 MB | - |
| M | spark | transform_write | 7.54s | 7.46s | 3/3 | 4.7 GB | pass |
| M | spark | aggregate | 1.03s | 1.02s | 3/3 | 4.8 GB | pass |
| M | spark | top_zones | 0.94s | 0.86s | 3/3 | 4.5 GB | pass |
| M | spark | merge | 9.49s | 9.08s | 3/3 | 4.7 GB | pass |
| M | duckdb | startup | 0.15s | 0.06s | 3/3 | 393 MB | - |
| M | duckdb | transform_write | 2.17s | 1.99s | 3/3 | 2.6 GB | pass |
| M | duckdb | aggregate | 0.07s | 0.07s | 3/3 | 1.4 GB | pass |
| M | duckdb | top_zones | 0.12s | 0.11s | 3/3 | 1.3 GB | pass |
| M | duckdb | merge | n/a | - | 0/3 | - | - |
| M | polars | startup | 0.10s | 0.06s | 3/3 | 132 MB | - |
| M | polars | transform_write | 1.70s | 1.64s | 3/3 | 6.1 GB | pass |
| M | polars | aggregate | 0.08s | 0.07s | 3/3 | 4.4 GB | pass |
| M | polars | top_zones | 0.09s | 0.08s | 3/3 | 4.2 GB | pass |
| M | polars | merge | 2.70s | 2.69s | 3/3 | 2.8 GB | pass |
| M | delta-rs | startup | 0.01s | 0.01s | 3/3 | 504 MB | - |
| M | delta-rs | transform_write | 2.17s | 2.16s | 3/3 | 5.1 GB | pass |
| M | delta-rs | aggregate | 0.17s | 0.17s | 3/3 | 5.0 GB | pass |
| M | delta-rs | top_zones | 0.08s | 0.08s | 3/3 | 3.1 GB | pass |
| M | delta-rs | merge | 2.68s | 2.67s | 3/3 | 2.9 GB | pass |
| L | spark | startup | 3.96s | 3.24s | 3/3 | 607 MB | - |
| L | spark | transform_write | 22s | 21s | 3/3 | 4.8 GB | pass |
| L | spark | aggregate | 2.20s | 2.02s | 3/3 | 4.9 GB | pass |
| L | spark | top_zones | 1.88s | 1.65s | 3/3 | 4.9 GB | pass |
| L | spark | merge | 20s | 20s | 3/3 | 4.8 GB | pass |
| L | duckdb | startup | 0.17s | 0.15s | 3/3 | 274 MB | - |
| L | duckdb | transform_write | 4.46s | 4.39s | 3/3 | 4.7 GB | pass |
| L | duckdb | aggregate | 0.23s | 0.23s | 3/3 | 2.4 GB | pass |
| L | duckdb | top_zones | 0.37s | 0.35s | 3/3 | 2.4 GB | pass |
| L | duckdb | merge | n/a | - | 0/3 | - | - |
| L | polars | startup | 0.11s | 0.11s | 3/3 | 124 MB | - |
| L | polars | transform_write | 4.79s | 4.72s | 3/3 | 10.7 GB | pass |
| L | polars | aggregate | 0.28s | 0.27s | 3/3 | 5.6 GB | pass |
| L | polars | top_zones | 0.30s | 0.29s | 3/3 | 5.6 GB | pass |
| L | polars | merge | 10s | 10s | 3/3 | 6.8 GB | pass |
| L | delta-rs | startup | 0.01s | 0.01s | 3/3 | 446 MB | - |
| L | delta-rs | transform_write | 22s | 20s | 3/3 | 11.7 GB | pass |
| L | delta-rs | aggregate | 0.31s | 0.30s | 3/3 | 1.5 GB | pass |
| L | delta-rs | top_zones | 0.27s | 0.26s | 3/3 | 1.5 GB | pass |
| L | delta-rs | merge | 10s | 10s | 3/3 | 6.2 GB | pass |

## Not run / failed

- S / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table
- M / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table
- L / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table

## Engine notes

- spark: driver_memory=4g; delta_jars=local
- duckdb: read=delta_scan; write=delta-rs
- polars: polars engine=streaming; write+merge=delta-rs

Settings: `SPARK_DRIVER_MEMORY=4g`
