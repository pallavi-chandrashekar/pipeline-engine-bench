# Results: run `bench-20261012-spark3`

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O | 14 cores / 14 threads | 24.0 GB RAM | Python 3.14.3 | openjdk version "17.0.21.0.3" 2026-10-24

Versions: pyspark 3.5.3, delta-spark 3.2.1, deltalake 1.6.6, pyarrow 26.0.0

**Pipeline time** = startup + transform_write + aggregate + top_zones, median over valid runs.
A run counts as valid only if it finished AND passed its correctness check.
MERGE is reported separately because not every engine can MERGE into Delta.
Memory = peak RSS of the whole process tree (incl. Spark's JVM) during those stages.

| Tier | Rows | Engine | Pipeline | Valid runs | Peak memory | MERGE | MERGE peak | Checks |
|---|---:|---|---:|---:|---:|---:|---:|---|
| S | 7,696,080 | spark | 9.29s | 3/3 | 4.7 GB | 5.89s | 4.1 GB | pass |
| M | 22,611,807 | spark | 13s | 3/3 | 7.1 GB | 9.64s | 5.6 GB | pass |
| L | 84,597,002 | spark | 41s | 3/3 | 10.1 GB | 22s | 7.2 GB | pass |
| XL | 218,116,121 | spark | 80s | 3/3 | 10.5 GB | 44s | 7.0 GB | pass |

## Per-stage medians (valid runs)

| Tier | Engine | Stage | Median | Min | Valid/total | Peak memory | Check |
|---|---|---|---:|---:|---:|---:|---|
| S | spark | startup | 2.91s | 2.90s | 3/3 | 538 MB | - |
| S | spark | transform_write | 4.70s | 4.66s | 3/3 | 4.5 GB | pass |
| S | spark | aggregate | 0.98s | 0.94s | 3/3 | 4.6 GB | pass |
| S | spark | top_zones | 0.69s | 0.66s | 3/3 | 4.7 GB | pass |
| S | spark | merge | 5.89s | 5.47s | 3/3 | 4.1 GB | pass |
| M | spark | startup | 2.88s | 2.83s | 3/3 | 536 MB | - |
| M | spark | transform_write | 7.53s | 7.29s | 3/3 | 7.1 GB | pass |
| M | spark | aggregate | 1.45s | 1.26s | 3/3 | 5.6 GB | pass |
| M | spark | top_zones | 1.37s | 0.94s | 3/3 | 5.5 GB | pass |
| M | spark | merge | 9.64s | 9.02s | 3/3 | 5.6 GB | pass |
| L | spark | startup | 3.38s | 2.94s | 3/3 | 527 MB | - |
| L | spark | transform_write | 31s | 26s | 3/3 | 10.1 GB | pass |
| L | spark | aggregate | 4.45s | 2.67s | 3/3 | 7.8 GB | pass |
| L | spark | top_zones | 2.29s | 2.03s | 3/3 | 6.8 GB | pass |
| L | spark | merge | 22s | 20s | 3/3 | 7.2 GB | pass |
| XL | spark | startup | 3.50s | 3.42s | 3/3 | 534 MB | - |
| XL | spark | transform_write | 68s | 66s | 3/3 | 10.5 GB | pass |
| XL | spark | aggregate | 5.66s | 5.52s | 3/3 | 7.6 GB | pass |
| XL | spark | top_zones | 3.09s | 3.00s | 3/3 | 4.8 GB | pass |
| XL | spark | merge | 44s | 42s | 3/3 | 7.0 GB | pass |

## Engine notes

- spark: driver_memory=14g; delta_jars=local
