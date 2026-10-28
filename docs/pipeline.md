# The pipeline & data tiers

[← back to README](../README.md)

A bronze → silver → gold flow over NYC yellow-taxi trips, run identically on every engine and at
every data size.

## Stages

| Stage | What it does | Why it's here |
|---|---|---|
| `startup` | import, create session/connection, register the bronze Delta tables | JVM startup is a real cost for small jobs |
| `transform_write` | clean bad rows, enrich with pickup/dropoff borough (2 joins), **write a silver Delta table partitioned by year/month** | the bread-and-butter ETL step, read → transform → write |
| `aggregate` | gold: revenue, trips and average distance by borough × year × month × hour, **read from the engine's own silver output** | scan + group-by, and proves the engine can read back what it wrote |
| `top_zones` | gold: top 10 pickup zones per borough by revenue (silver ⋈ zones, `ROW_NUMBER() OVER`) | join + window function |
| `merge` | **MERGE** a corrections batch into a fresh copy of the bronze table: ~0.1% of trips get a tip adjustment, and the same number of new trips are inserted | upserts are where Delta pipelines usually hurt |

Spark, DuckDB and delta-rs (via its embedded DataFusion) run **the exact same SQL text**
([`bench/workload.py`](../bench/workload.py)). Polars runs the same logic through its lazy expression API.

**Pipeline time** = `startup + transform_write + aggregate + top_zones`. MERGE is reported
separately because not every engine can MERGE into Delta (see
[Fairness notes](method.md#fairness-notes-read-before-quoting-numbers)).

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
whose columns map onto `COLUMN_ALIASES` in [`bench/config.py`](../bench/config.py).

Open Delta Sharing server: <https://github.com/delta-io/delta-sharing> (profile:
`open-datasets.share`). NYC TLC trip records: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.
