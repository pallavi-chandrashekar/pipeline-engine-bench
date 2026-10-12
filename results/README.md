# Results

Each published run lives in its own folder (`results/<run_id>/`) with:

- `summary.md`: headline table + per-stage medians + anything that failed or was n/a
- `runs.csv`: every stage of every run (raw data)
- `summary.csv`, `totals.csv`: aggregated numbers
- `env.json`: machine, OS, Java and library versions
- `pipeline_time.png`, `peak_memory.png`, `merge_time.png`

Rebuild a summary with `make report RUN=results/<run_id>`.
