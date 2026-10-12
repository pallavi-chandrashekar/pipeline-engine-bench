"""Generate a synthetic, taxi-shaped tier for smoke tests (no network needed).

  python -m bench.synth --rows 2000000          # builds tier "SYN"
"""

from __future__ import annotations

import argparse

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from bench import config as C
from bench.ingest import STAGING, RAW, build_tier

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", "Unknown"]


def write_zones(n_zones: int = 265):
    RAW.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(1)
    path = RAW / "synthetic_zones.csv"
    with open(path, "w") as f:
        f.write("LocationID,Borough,Zone,service_zone\n")
        for i in range(1, n_zones + 1):
            b = BOROUGHS[rng.choice(len(BOROUGHS), p=[0.3, 0.25, 0.25, 0.1, 0.05, 0.01, 0.04])]
            f.write(f'{i},{b},"Zone {i:03d}",Boro Zone\n')
    return path


def write_trips(rows: int, chunk: int = 1_000_000):
    out_dir = STAGING / "synthetic"
    if out_dir.exists():
        for p in out_dir.glob("*.parquet"):
            p.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    start = np.datetime64("2019-01-01T00:00:00", "us")
    files, done, idx = [], 0, 0
    while done < rows:
        n = min(chunk, rows - done)
        pickup = start + rng.integers(0, 90 * 24 * 3600, n).astype("timedelta64[s]")
        duration = rng.gamma(2.0, 400.0, n).astype("timedelta64[s]")
        dist = np.round(rng.gamma(1.5, 2.0, n), 2)
        dist[rng.random(n) < 0.01] = 0.0  # some junk rows for the cleaning stage
        fare = np.round(2.5 + dist * 2.5 + rng.normal(0, 1, n), 2)
        tip = np.round(np.where(rng.random(n) < 0.6, fare * rng.uniform(0.1, 0.3, n), 0.0), 2)
        total = np.round(fare + tip + 0.5 + 0.3, 2)
        tbl = pa.table(
            {
                "trip_id": pa.array(idx * 1_000_000_000 + np.arange(n), pa.int64()),
                "vendor_id": pa.array(rng.integers(1, 3, n), pa.int32()),
                "pickup_ts": pa.array(pickup, pa.timestamp("us")),
                "dropoff_ts": pa.array(pickup + duration, pa.timestamp("us")),
                "passenger_count": pa.array(rng.integers(1, 5, n), pa.int32()),
                "trip_distance": pa.array(dist, pa.float64()),
                "pu_location_id": pa.array(rng.integers(1, 266, n), pa.int32()),
                "do_location_id": pa.array(rng.integers(1, 266, n), pa.int32()),
                "payment_type": pa.array(rng.integers(1, 5, n), pa.int32()),
                "fare_amount": pa.array(fare, pa.float64()),
                "tip_amount": pa.array(tip, pa.float64()),
                "total_amount": pa.array(total, pa.float64()),
            },
            schema=C.TRIP_SCHEMA,
        )
        path = out_dir / f"{idx:05d}.parquet"
        pq.write_table(tbl, path, compression="zstd")
        files.append(path)
        done += n
        idx += 1
    return files


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--tier", default="SYN")
    a = ap.parse_args(argv)
    zones = write_zones()
    files = write_trips(a.rows)
    build_tier(a.tier, [(files, None)], zones, {"source": "synthetic", "description": f"{a.rows:,} synthetic trips"})


if __name__ == "__main__":
    main()
