"""Correctness checks. A fast wrong answer is not a result.

The reference is computed WITHOUT any of the benchmarked engines and without the shared
SQL: a streaming pass over the bronze Delta snapshot with pyarrow compute kernels
(filters, a dictionary lookup for the zone join, partial group-bys merged at the end).
If the shared SQL had a bug, the engines running it would fail against this reference.

Checks
  transform_write  silver grouped by year/month/pu_borough/do_borough: row count and
                   SUM(total_amount) per group must match the reference
  aggregate        full result, row by row (floats: relative tolerance 1e-6)
  top_zones        full result, row by row
  merge            final row count and SUM(total_amount) == expected outcome recorded
                   when the corrections batch was built
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from deltalake import DeltaTable

from bench import config as C
from bench import workload as W

RTOL = 1e-6


def _dataset(path: Path) -> ds.Dataset:
    if (Path(path) / "_delta_log").exists():
        return DeltaTable(str(path)).to_pyarrow_dataset()  # honours deletion vectors / log state
    return ds.dataset(str(path), format="parquet", partitioning="hive")  # Spark no-delta test mode


def _sumgroup(parts: list[pa.Table], keys: list[str], sums: list[str]) -> pa.Table:
    t = pa.concat_tables(parts)
    out = t.group_by(keys, use_threads=False).aggregate([(c, "sum") for c in sums])
    return out.rename_columns([n.removesuffix("_sum") for n in out.column_names])


class Reference:
    """One streaming pass over bronze produces every reference for a tier."""

    def __init__(self, tier: str):
        p = C.table_paths(tier)
        zones = DeltaTable(str(p["zones"])).to_pyarrow_table()
        self.zone_ids = zones["location_id"]
        self.zone_borough = zones["borough"]
        self.zone_name = zones["zone"]

        silver_parts, agg_parts, zone_parts = [], [], []
        cols = ["pickup_ts", "dropoff_ts", "trip_distance", "pu_location_id", "do_location_id",
                "fare_amount", "tip_amount", "total_amount"]
        for batch in _dataset(p["trips"]).to_batches(columns=cols, batch_size=1_000_000):
            b = pa.Table.from_batches([batch])
            keep = pc.and_(pc.and_(pc.greater(b["trip_distance"], 0), pc.greater(b["total_amount"], 0)),
                           pc.greater(b["dropoff_ts"], b["pickup_ts"]))
            b = b.filter(pc.fill_null(keep, False))
            pu_idx = pc.index_in(b["pu_location_id"], value_set=self.zone_ids)
            b = b.filter(pc.is_valid(pu_idx))  # inner join on pickup zone
            pu_idx = pu_idx.filter(pc.is_valid(pu_idx))
            do_idx = pc.index_in(b["do_location_id"], value_set=self.zone_ids)  # left join
            ts = b["pickup_ts"]
            fare_pos = pc.greater(b["fare_amount"], 0)
            ratio = pc.if_else(fare_pos, pc.divide(b["tip_amount"], b["fare_amount"]), pa.scalar(None, pa.float64()))
            t = pa.table({
                "year": pc.cast(pc.year(ts), pa.int32()),
                "month": pc.cast(pc.month(ts), pa.int32()),
                "hour": pc.cast(pc.hour(ts), pa.int32()),
                "pu_borough": pc.take(self.zone_borough, pu_idx),
                "do_borough": pc.take(self.zone_borough, do_idx),
                "location_id": b["pu_location_id"],
                "n": pa.array(np.ones(len(b), dtype=np.int64)),
                "total_amount": b["total_amount"],
                "trip_distance": b["trip_distance"],
                "ratio_sum": pc.fill_null(ratio, 0.0),
                "ratio_n": pc.cast(pc.fill_null(pc.is_valid(ratio), False), pa.int64()),
            })
            silver_parts.append(_sumgroup([t], W.SILVER_CHECK_KEYS, ["n", "total_amount"]))
            agg_parts.append(_sumgroup([t], ["pu_borough", "year", "month", "hour"], ["n", "total_amount", "trip_distance"]))
            zone_parts.append(_sumgroup([t], ["pu_borough", "location_id"], ["n", "total_amount", "ratio_sum", "ratio_n"]))
            # keep partial lists short so memory stays flat on XL
            if len(silver_parts) >= 32:
                silver_parts = [_sumgroup(silver_parts, W.SILVER_CHECK_KEYS, ["n", "total_amount"])]
                agg_parts = [_sumgroup(agg_parts, ["pu_borough", "year", "month", "hour"], ["n", "total_amount", "trip_distance"])]
                zone_parts = [_sumgroup(zone_parts, ["pu_borough", "location_id"], ["n", "total_amount", "ratio_sum", "ratio_n"])]

        self.silver = _sumgroup(silver_parts, W.SILVER_CHECK_KEYS, ["n", "total_amount"])

        a = _sumgroup(agg_parts, ["pu_borough", "year", "month", "hour"], ["n", "total_amount", "trip_distance"]).to_pandas()
        self.aggregate = pa.Table.from_pandas(pd.DataFrame({
            "borough": a.pu_borough, "year": a.year, "month": a.month, "hour": a.hour,
            "trips": a.n, "revenue": a.total_amount, "avg_distance": a.trip_distance / a.n,
        }), preserve_index=False)

        z = _sumgroup(zone_parts, ["pu_borough", "location_id"], ["n", "total_amount", "ratio_sum", "ratio_n"]).to_pandas()
        names = dict(zip(self.zone_ids.to_pylist(), self.zone_name.to_pylist()))
        z = pd.DataFrame({
            "borough": z.pu_borough, "zone": z.location_id.map(names), "location_id": z.location_id,
            "trips": z.n, "revenue": z.total_amount,
            "avg_tip_ratio": np.where(z.ratio_n > 0, z.ratio_sum / z.ratio_n.where(z.ratio_n > 0, 1), np.nan),
        }).sort_values(["borough", "revenue", "location_id"], ascending=[True, False, True])
        z["rnk"] = z.groupby("borough").cumcount() + 1
        self.top_zones = pa.Table.from_pandas(z[z.rnk <= 10].reset_index(drop=True), preserve_index=False)


def silver_summary(path: Path) -> pa.Table:
    parts = []
    for batch in _dataset(path).to_batches(columns=W.SILVER_CHECK_KEYS + ["total_amount"], batch_size=1_000_000):
        t = pa.Table.from_batches([batch])
        t = t.append_column("n", pa.array(np.ones(len(t), dtype=np.int64)))
        t = t.set_column(t.schema.get_field_index("year"), "year", pc.cast(t["year"], pa.int32()))
        t = t.set_column(t.schema.get_field_index("month"), "month", pc.cast(t["month"], pa.int32()))
        parts.append(_sumgroup([t], W.SILVER_CHECK_KEYS, ["n", "total_amount"]))
        if len(parts) >= 32:
            parts = [_sumgroup(parts, W.SILVER_CHECK_KEYS, ["n", "total_amount"])]
    return _sumgroup(parts, W.SILVER_CHECK_KEYS, ["n", "total_amount"])


def count_sum(path: Path) -> tuple[int, float]:
    n, s = 0, 0.0
    for batch in _dataset(path).to_batches(columns=["total_amount"], batch_size=1_000_000):
        n += len(batch)
        s += pc.sum(batch.column(0)).as_py() or 0.0
    return n, s


def _close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=RTOL, abs_tol=1e-6)


def compare_tables(ref: pa.Table, got: pa.Table, keys: list[str]) -> str:
    missing = [c for c in ref.column_names if c not in got.column_names]
    if missing:
        return f"FAIL: missing columns {missing}"

    def norm(t):
        df = t.select(ref.column_names).to_pandas()
        for k in keys:  # null keys (e.g. unknown dropoff borough) must sort the same way
            if df[k].dtype == object:
                df[k] = df[k].fillna("<null>").astype(str)
        return df.sort_values(keys).reset_index(drop=True)

    r, g = norm(ref), norm(got)
    if len(r) != len(g):
        return f"FAIL: {len(g)} rows, expected {len(r)}"
    for col in r.columns:
        rc, gc = r[col], g[col]
        if rc.dtype.kind in "fc" or gc.dtype.kind in "fc":
            if not np.allclose(rc.astype(float), gc.astype(float), rtol=RTOL, atol=1e-9, equal_nan=True):
                return f"FAIL: column {col} differs"
        elif rc.dtype.kind in "iu":
            if not (rc.astype("int64").values == gc.astype("int64").values).all():
                return f"FAIL: column {col} differs"
        elif not (rc.astype(str).values == gc.astype(str).values).all():
            return f"FAIL: column {col} differs"
    return "pass"


def check_run(wd: Path, tier: str, metrics: dict, manifest: dict, ref: Reference) -> dict:
    out = {}
    st = metrics.get("stages", {})

    def guarded(name, fn):
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001 - an unreadable output is a failed check
            out[name] = f"FAIL: {type(e).__name__}: {e}"[:300]

    if st.get("transform_write", {}).get("status") == "ok":
        guarded("transform_write", lambda: compare_tables(ref.silver, silver_summary(wd / "silver"), W.SILVER_CHECK_KEYS))
    for q in ("aggregate", "top_zones"):
        if st.get(q, {}).get("status") == "ok":
            guarded(q, lambda q=q: compare_tables(getattr(ref, q), pq.read_table(wd / f"{q}.parquet"), W.RESULT_KEYS[q]))
    if st.get("merge", {}).get("status") == "ok":
        exp = manifest["merge_expected"]

        def merge_check():
            n, s = count_sum(wd / "trips_copy")
            if n == exp["rows_after"] and _close(s, exp["total_after"]):
                return "pass"
            return f"FAIL: {n} rows / {s:.2f}, expected {exp['rows_after']} / {exp['total_after']:.2f}"

        guarded("merge", merge_check)
    return out
