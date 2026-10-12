"""Materialize benchmark tiers as local Delta tables.

Sources
  share  - the open Delta Sharing server (default for 2019 data)
  tlc    - NYC TLC public parquet files (XL top-up, or a fallback for everything)

Pipeline: download raw files -> normalize each file once into data/staging -> build
each tier (trips, zones, corrections) as a local Delta table with delta-rs.

Engines never read over the network during the benchmark: Delta Sharing is a
transfer protocol, so timing reads through it would measure bandwidth, not engines.

Usage
  python -m bench.ingest list                    # show tables on the open share
  python -m bench.ingest prepare --tier S        # download + build one tier
  python -m bench.ingest prepare --tier XL --source tlc
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import duckdb
import requests
from deltalake import write_deltalake

from bench import config as C

RAW = C.DATA_DIR / "raw"
STAGING = C.DATA_DIR / "staging"


# --------------------------------------------------------------------------- download


def _download(url: str, dest: Path, expected_size: int | None = None, attempts: int = 5) -> None:
    if dest.exists() and (expected_size is None or dest.stat().st_size == expected_size):
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 << 20):
                        f.write(chunk)
            if expected_size is not None and tmp.stat().st_size != expected_size:
                raise requests.ConnectionError(f"short read: {tmp.stat().st_size} of {expected_size} bytes")
            tmp.rename(dest)
            return
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
            if attempt == attempts:
                raise
            wait = 2 ** attempt
            print(f"    retry {attempt}/{attempts - 1} in {wait}s ({e})", flush=True)
            time.sleep(wait)


def _share_client():
    import delta_sharing
    from delta_sharing.protocol import DeltaSharingProfile
    from delta_sharing.rest_client import DataSharingRestClient

    profile_path = RAW / "open-datasets.share"
    _download(C.SHARE_PROFILE_URL, profile_path)
    profile = DeltaSharingProfile.read_from_file(str(profile_path))
    return delta_sharing.SharingClient(profile), DataSharingRestClient(profile)


def _share_table():
    from delta_sharing.protocol import Table

    share, schema, name = C.SHARE_TABLE.split(".", 2)
    return Table(name=name, share=share, schema=schema)


def cmd_list() -> None:
    client, rest = _share_client()
    tables = client.list_all_tables()
    print(f"{len(tables)} tables on the open share:")
    for t in tables:
        print(f"  {t.share}.{t.schema}.{t.name}")
    target = _share_table()
    if not any(t.name == target.name and t.schema == target.schema for t in tables):
        print(f"\n! {C.SHARE_TABLE} not found. Set BENCH_SHARE_TABLE or use --source tlc.")
        return
    resp = rest.list_files_in_table(target)
    size = sum(f.size for f in resp.add_files)
    rows = 0
    for f in resp.add_files:
        try:
            rows += json.loads(f.stats or "{}").get("numRecords", 0)
        except ValueError:
            pass
    print(f"\n{C.SHARE_TABLE}: {len(resp.add_files)} files, {size / 1e9:.2f} GB, ~{rows:,} rows")
    print("columns:", [f["name"] for f in json.loads(resp.metadata.schema_string)["fields"]])


def download_share() -> list[Path]:
    _, rest = _share_client()
    files = sorted(rest.list_files_in_table(_share_table()).add_files, key=lambda f: f.id)
    if any(f.partition_values for f in files):
        # partition columns are not stored inside the data files; normalization would lose them
        raise SystemExit(f"{C.SHARE_TABLE} is partitioned; partitioned shared tables aren't supported yet. "
                         "Use an unpartitioned table or --source tlc.")
    out = []
    print(f"share: {len(files)} files")
    for i, f in enumerate(files):
        dest = RAW / "share" / f"{i:05d}.parquet"
        try:
            _download(f.url, dest, f.size)
        except requests.HTTPError:
            # Pre-signed URLs expire; re-list once and retry this file.
            fresh = {x.id: x for x in rest.list_files_in_table(_share_table()).add_files}
            _download(fresh[f.id].url, dest, f.size)
        out.append(dest)
        print(f"  [{i + 1}/{len(files)}] {dest.name} {f.size / 1e6:.0f} MB", flush=True)
    return out


def download_tlc(month_list: list[str]) -> list[Path]:
    out = []
    for ym in month_list:
        dest = RAW / "tlc" / f"yellow_tripdata_{ym}.parquet"
        _download(C.TLC_URL.format(ym=ym), dest)
        out.append(dest)
        print(f"  tlc {ym} ok", flush=True)
    return out


def download_zones() -> Path:
    dest = RAW / "taxi_zone_lookup.csv"
    _download(C.ZONES_URL, dest)
    return dest


# --------------------------------------------------------------------------- normalize


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def _normalize_select(con, path: Path, file_index: int) -> str:
    cols = {r[0].lower(): r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    exprs = [f"CAST({file_index} AS BIGINT) * 1000000000 + file_row_number AS trip_id"]
    for field in C.TRIP_SCHEMA:
        if field.name == "trip_id":
            continue
        src = next((cols[a] for a in C.COLUMN_ALIASES[field.name] if a in cols), None)
        if src is None:
            raise SystemExit(f"{path.name}: no column for '{field.name}'. Found: {sorted(cols.values())}")
        sql_type = {
            "int32": "INTEGER",
            "int64": "BIGINT",
            "double": "DOUBLE",
            "timestamp[us]": "TIMESTAMP",
        }[str(field.type)]
        # TRY_CAST: an unexpected encoding (e.g. payment_type 'CRD') becomes NULL instead of
        # aborting ingest; null counts are reported after staging
        exprs.append(f'TRY_CAST("{src}" AS {sql_type}) AS {field.name}')
    return (
        f"SELECT {', '.join(exprs)} FROM read_parquet('{path}', file_row_number=true)"
    )


def stage(source: str, files: list[Path], index_of) -> list[Path]:
    """Normalize raw files into the canonical schema (once per file)."""
    con = _con()
    out = []
    for f in files:
        dest = STAGING / source / f.name
        stamp = dest.with_suffix(".src")  # cache key: the raw file's size + mtime
        sig = f"{f.stat().st_size}:{int(f.stat().st_mtime)}:{C.SHARE_TABLE if source == 'share' else ''}"
        if not (dest.exists() and stamp.exists() and stamp.read_text() == sig):
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".parquet.part")
            sql = _normalize_select(con, f, index_of(f))
            con.execute(
                f"COPY ({sql} WHERE pickup_ts IS NOT NULL AND pu_location_id IS NOT NULL) "
                f"TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)"
            )
            tmp.rename(dest)
            stamp.write_text(sig)
            raw_rows = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
            nulls = con.execute(
                "SELECT " + ", ".join(f"count(*) - count({c.name})" for c in C.TRIP_SCHEMA)
                + f", count(*) FROM read_parquet('{dest}')"
            ).fetchone()
            kept = nulls[-1]
            null_cols = {c.name: n for c, n in zip(C.TRIP_SCHEMA, nulls[:-1]) if n}
            msg = f"    staged {f.name}: {kept:,}/{raw_rows:,} rows kept"
            if null_cols:
                msg += f"; nulls after cast: {null_cols}"
            print(msg, flush=True)
        out.append(dest)
    return out


# --------------------------------------------------------------------------- build


def _month_filter(month_list: list[str] | None) -> str:
    if not month_list:
        return "TRUE"
    quoted = ", ".join(f"'{m}'" for m in month_list)
    return f"strftime(pickup_ts, '%Y-%m') IN ({quoted})"


def build_tier(tier: str, parts: list[tuple[list[Path], list[str] | None]], zones_csv: Path, meta: dict) -> dict:
    """parts: [(staged files, months to keep or None for all)]"""
    paths = C.table_paths(tier)
    (C.tier_dir(tier) / "manifest.json").unlink(missing_ok=True)  # a failed rebuild must not look prepared
    for p in paths.values():
        if p.exists():
            shutil.rmtree(p)
    paths["trips"].parent.mkdir(parents=True, exist_ok=True)
    con = _con()

    selects = []
    for files, month_list in parts:
        if not files:
            continue
        flist = ", ".join(f"'{f}'" for f in files)
        selects.append(f"SELECT * FROM read_parquet([{flist}]) WHERE {_month_filter(month_list)}")
    con.execute(f"CREATE VIEW trips AS {' UNION ALL '.join(selects)}")

    t0 = time.time()
    reader = con.execute("SELECT * FROM trips").to_arrow_reader(1_000_000)
    write_deltalake(str(paths["trips"]), reader, mode="overwrite")
    print(f"  trips written in {time.time() - t0:.0f}s", flush=True)

    con.execute(
        f"""CREATE TABLE zones AS SELECT
              CAST(LocationID AS INTEGER) AS location_id,
              CAST(Borough AS VARCHAR) AS borough,
              CAST(Zone AS VARCHAR) AS zone,
              CAST(service_zone AS VARCHAR) AS service_zone
            FROM read_csv('{zones_csv}', header=true)"""
    )
    write_deltalake(str(paths["zones"]), con.execute("SELECT * FROM zones").to_arrow_table(), mode="overwrite")

    # Corrections batch: scattered updates (a $1 tip adjustment) + an equal number of new trips.
    m, d = C.CORRECTION_MODULUS, C.CORRECTION_TIP_DELTA
    stats = con.execute("SELECT count(*), sum(total_amount), max(trip_id) FROM trips").fetchone()
    rows, total, max_id = stats
    con.execute(
        f"""CREATE TABLE corrections AS
            WITH picked AS (SELECT * FROM trips WHERE trip_id % {m} = 7)
            SELECT * REPLACE (tip_amount + {d} AS tip_amount, total_amount + {d} AS total_amount) FROM picked
            UNION ALL
            SELECT * REPLACE ({max_id} + row_number() OVER (ORDER BY trip_id) AS trip_id) FROM picked"""
    )
    n_upd = con.execute(f"SELECT count(*) FROM corrections WHERE trip_id <= {max_id}").fetchone()[0]
    ins_total = con.execute(f"SELECT coalesce(sum(total_amount), 0) FROM corrections WHERE trip_id > {max_id}").fetchone()[0]
    write_deltalake(
        str(paths["corrections"]),
        con.execute("SELECT * FROM corrections ORDER BY trip_id").to_arrow_table().cast(C.TRIP_SCHEMA),
        mode="overwrite",
    )

    trips_bytes = sum(f.stat().st_size for f in paths["trips"].rglob("*.parquet"))
    manifest = {
        "tier": tier,
        **meta,
        "rows": rows,
        "trips_bytes": trips_bytes,
        "total_amount": total,
        "merge_expected": {
            "updates": n_upd,
            "inserts": n_upd,
            "rows_after": rows + n_upd,
            "total_after": total + n_upd * d + ins_total,
        },
        "created": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    (C.tier_dir(tier) / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  {tier}: {rows:,} rows, {trips_bytes / 1e9:.2f} GB on disk", flush=True)
    return manifest


def prepare(tier_name: str, source: str) -> dict:
    tier = C.TIERS[tier_name]
    zones = download_zones()
    parts = []
    if source == "share":
        files = download_share()
        staged = stage("share", files, lambda f: int(f.stem))
        parts.append((staged, tier.share_months))
        tlc_months = tier.tlc_months
    elif source == "tlc":
        tlc_months = tier.share_months + tier.tlc_months
    else:
        raise SystemExit(f"unknown source {source}")
    if tlc_months:
        files = download_tlc(tlc_months)
        staged = stage("tlc", files, lambda f: int(f.stem[-7:].replace("-", "")))
        # each TLC file contains a few stray rows from other months; keep only the tier's months
        parts.append((staged, tlc_months))
    return build_tier(tier_name, parts, zones, {"source": source, "description": tier.description})


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    p = sub.add_parser("prepare")
    p.add_argument("--tier", choices=list(C.TIERS), required=True)
    p.add_argument("--source", choices=["share", "tlc"], default="share")
    a = ap.parse_args(argv)
    if a.cmd == "list":
        cmd_list()
    else:
        prepare(a.tier, a.source)


if __name__ == "__main__":
    sys.exit(main())
