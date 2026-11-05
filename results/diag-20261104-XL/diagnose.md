# delta-rs diagnosis — tier XL

Each experiment ran in its own subprocess. `seconds` is wall time inside the worker;
`swap_growth_mb` is macOS swap-in-use delta across the run (memory-pressure signal).

| experiment | seconds | first_batch_s | exec_s | rows | file_count | peak_rss_mb | swap_growth_mb | exit |
|---|---|---|---|---|---|---|---|---|
| probe | 149.60 | 149.59 | 0.01 | - | - | 6747.10 | 4128.60 | 0 |
| query_only | 598.71 | 134.34 | 0.01 | 214272757 | - | 12977.30 | 5911.10 | 0 |
| write_only | 9.55 | - | - | - | 74 | 13600.00 | -7702.60 | 0 |
| deltars_e2e | 676.81 | - | - | - | 74 | 13691.20 | 6292.80 | 0 |
| duckdb_e2e | 10.64 | - | - | - | 74 | 6495.60 | -5368.10 | 0 |
| ablate_filter | 1.02 | 0.05 | 0.02 | 214272757 | - | 1643.00 | 0.00 | 0 |
| ablate_inner | 1.19 | 0.04 | 0.01 | 214272757 | - | 1771.30 | -8.00 | 0 |
| ablate_left | 1.14 | 0.04 | 0.01 | 214272757 | - | 1772.90 | 0.00 | 0 |
| ablate_bothjoins | 563.23 | 97.66 | 0.01 | 214272757 | - | 13186.40 | 8756.60 | 0 |
