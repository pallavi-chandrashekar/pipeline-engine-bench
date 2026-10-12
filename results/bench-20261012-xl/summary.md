# Results: run `bench-20261012-xl`

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O | 14 cores / 14 threads | 24.0 GB RAM | Python 3.14.3 | openjdk version "21.0.12.1" 2026-08-18

Versions: pyspark 4.0.1, delta-spark 4.0.0, duckdb 1.5.6, polars 2.0.0, deltalake 1.6.6, pyarrow 23.0.1

**Pipeline time** = startup + transform_write + aggregate + top_zones, median over valid runs.
A run counts as valid only if it finished AND passed its correctness check.
MERGE is reported separately because not every engine can MERGE into Delta.
Memory = peak RSS of the whole process tree (incl. Spark's JVM) during those stages.

| Tier | Rows | Engine | Pipeline | Valid runs | Peak memory | MERGE | MERGE peak | Checks |
|---|---:|---|---:|---:|---:|---:|---:|---|
| XL | 218,116,121 | spark | 63s | 3/3 | 12.0 GB | 40s | 7.6 GB | pass |
| XL | 218,116,121 | duckdb | 13s | 3/3 | 5.7 GB | n/a | - | pass |
| XL | 218,116,121 | polars | 16s | 3/3 | 9.2 GB | 27s | 8.4 GB | pass |
| XL | 218,116,121 | delta-rs | 792s | 3/3 | 11.0 GB | 27s | 8.7 GB | pass |

## Per-stage medians (valid runs)

| Tier | Engine | Stage | Median | Min | Valid/total | Peak memory | Check |
|---|---|---|---:|---:|---:|---:|---|
| XL | spark | startup | 4.14s | 3.33s | 3/3 | 636 MB | - |
| XL | spark | transform_write | 51s | 49s | 3/3 | 12.0 GB | pass |
| XL | spark | aggregate | 4.27s | 3.78s | 3/3 | 10.5 GB | pass |
| XL | spark | top_zones | 3.31s | 3.26s | 3/3 | 9.2 GB | pass |
| XL | spark | merge | 40s | 37s | 3/3 | 7.6 GB | pass |
| XL | duckdb | startup | 0.18s | 0.18s | 3/3 | 308 MB | - |
| XL | duckdb | transform_write | 11s | 11s | 3/3 | 5.7 GB | pass |
| XL | duckdb | aggregate | 0.61s | 0.56s | 3/3 | 3.4 GB | pass |
| XL | duckdb | top_zones | 0.87s | 0.86s | 3/3 | 3.5 GB | pass |
| XL | duckdb | merge | n/a | - | 0/3 | - | - |
| XL | polars | startup | 0.13s | 0.12s | 3/3 | 119 MB | - |
| XL | polars | transform_write | 14s | 13s | 3/3 | 9.2 GB | pass |
| XL | polars | aggregate | 0.76s | 0.68s | 3/3 | 5.0 GB | pass |
| XL | polars | top_zones | 0.77s | 0.72s | 3/3 | 5.0 GB | pass |
| XL | polars | merge | 27s | 27s | 3/3 | 8.4 GB | pass |
| XL | delta-rs | startup | 0.03s | 0.03s | 3/3 | 526 MB | - |
| XL | delta-rs | transform_write | 791s | 788s | 3/3 | 11.0 GB | pass |
| XL | delta-rs | aggregate | 0.86s | 0.84s | 3/3 | 1.7 GB | pass |
| XL | delta-rs | top_zones | 0.64s | 0.64s | 3/3 | 1.7 GB | pass |
| XL | delta-rs | merge | 27s | 27s | 3/3 | 8.7 GB | pass |

## Not run / failed

- XL / duckdb / merge (rep 0): **n/a**: DuckDB cannot MERGE/UPDATE a Delta table

## Engine notes

- spark: driver_memory=14g; delta_jars=local
- duckdb: read=delta_scan; write=delta-rs
- polars: polars engine=streaming; write+merge=delta-rs
