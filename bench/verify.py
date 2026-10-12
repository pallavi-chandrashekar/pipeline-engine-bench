"""Correctness checks. A fast wrong answer is not a result.

References are computed independently of the engine adapters: DuckDB scanning the
bronze snapshot's parquet files (resolved through the Delta log), plus the expected
MERGE outcome recorded at ingest time.
"""

from __future__ import annotations

import math
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from bench import config as C
from bench import workload as W

RTOL = 1e-6


def _files(path: Path) -> list[str]:
    if (path / "_delta_log").exists():
        return DeltaTable(str(path)).file_uris()
    return [str(p) for p in path.rglob("*.parquet")]  # Spark no-delta test mode


def _ref_con(tier: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    p = C.table_paths(tier)
    for name in ("trips", "zones"):
        files = ", ".join(f"'{f}'" for f in _files(p[name]))
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet([{files}])")
    return con


def _count_sum(files: list[str]) -> tuple[int, float]:
    flist = ", ".join(f"'{f}'" for f in files)
    n, s = duckdb.sql(f"SELECT count(*), coalesce(sum(total_amount), 0) FROM read_parquet([{flist}])").fetchone()
    return n, s


def silver_reference(tier: str) -> tuple[int, float]:
    con = _ref_con(tier)
    return con.execute(f"SELECT count(*), coalesce(sum(total_amount), 0) FROM ({W.SILVER_SQL})").fetchone()


def query_reference(tier: str, stage: str) -> pa.Table:
    sql = {"aggregate": W.AGGREGATE_SQL, "top_zones": W.TOP_ZONES_SQL}[stage]
    return _ref_con(tier).execute(sql).to_arrow_table()


def _close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=RTOL, abs_tol=1e-6)


def compare_tables(ref: pa.Table, got: pa.Table, keys: list[str]) -> str:
    missing = [c for c in ref.column_names if c not in got.column_names]
    if missing:
        return f"FAIL: missing columns {missing}"
    r = ref.to_pandas().sort_values(keys).reset_index(drop=True)
    g = got.select(ref.column_names).to_pandas().sort_values(keys).reset_index(drop=True)
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


def check_run(wd: Path, tier: str, metrics: dict, manifest: dict, silver_ref, refs: dict) -> dict:
    out = {}
    st = metrics.get("stages", {})

    if st.get("transform_write", {}).get("status") == "ok":
        n, s = _count_sum(_files(wd / "silver"))
        out["transform_write"] = "pass" if n == silver_ref[0] and _close(s, silver_ref[1]) else (
            f"FAIL: silver {n} rows / {s:.2f}, expected {silver_ref[0]} / {silver_ref[1]:.2f}")

    for q in ("aggregate", "top_zones"):
        if st.get(q, {}).get("status") == "ok":
            if q not in refs:
                refs[q] = query_reference(tier, q)
            out[q] = compare_tables(refs[q], pq.read_table(wd / f"{q}.parquet"), W.RESULT_KEYS[q])

    if st.get("merge", {}).get("status") == "ok":
        exp = manifest["merge_expected"]
        n, s = _count_sum(_files(wd / "trips_copy"))
        out["merge"] = "pass" if n == exp["rows_after"] and _close(s, exp["total_after"]) else (
            f"FAIL: {n} rows / {s:.2f}, expected {exp['rows_after']} / {exp['total_after']:.2f}")
    return out
