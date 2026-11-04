# delta-rs diagnosis — tier L

Each experiment ran in its own subprocess. `seconds` is wall time inside the worker;
`swap_growth_mb` is macOS swap-in-use delta across the run (memory-pressure signal).

| experiment | seconds | first_batch_s | exec_s | rows | file_count | peak_rss_mb | swap_growth_mb | exit |
|---|---|---|---|---|---|---|---|---|
| probe | 2.96 | 2.96 | 0.01 | - | - | 9074.80 | 0.00 | 0 |
| query_only | 18.99 | 2.65 | 0.06 | 83688137 | - | 12236.90 | 47.60 | 0 |
| write_only | 3.46 | - | - | - | 24 | 12374.00 | 0.00 | 0 |
| deltars_e2e | 24.00 | - | - | - | 24 | 10708.20 | 39.10 | 0 |
| duckdb_e2e | 4.62 | - | - | - | 24 | 5137.90 | -8.00 | 0 |
| ablate_filter | 0.47 | 0.05 | 0.02 | 83688137 | - | 1527.00 | 0.00 | 0 |
| ablate_inner | 0.55 | 0.04 | 0.01 | 83688137 | - | 1587.20 | 0.00 | 0 |
| ablate_left | 0.54 | 0.03 | 0.00 | 83688137 | - | 1540.70 | 0.00 | 0 |
| ablate_bothjoins | 20.62 | 2.54 | 0.01 | 83688137 | - | 11353.30 | 132.70 | 0 |
