"""Paths, data tiers, and the canonical trip schema shared by every engine."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("BENCH_DATA_DIR", ROOT / "data"))
RESULTS_DIR = Path(os.environ.get("BENCH_RESULTS_DIR", ROOT / "results"))

# Open Delta Sharing server run by the Delta Lake project (no sign-up needed).
SHARE_PROFILE_URL = (
    "https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/"
    "delta-sharing/share/open-datasets.share"
)
SHARE_TABLE = os.environ.get("BENCH_SHARE_TABLE", "delta_sharing.default.nyctaxi_2019")

# NYC TLC public trip records (used for the XL tier and as a fallback source).
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def months(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' strings."""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


@dataclass(frozen=True)
class Tier:
    name: str
    description: str
    share_months: list[str] = field(default_factory=list)  # months taken from the Delta Share
    tlc_months: list[str] = field(default_factory=list)  # extra months from TLC parquet


TIERS: dict[str, Tier] = {
    "S": Tier("S", "1 month of 2019 yellow-cab trips", months("2019-01", "2019-01")),
    "M": Tier("M", "3 months of 2019", months("2019-01", "2019-03")),
    "L": Tier("L", "all of 2019", months("2019-01", "2019-12")),
    "XL": Tier(
        "XL",
        "2019 from the share + 2020-2023 from NYC TLC",
        months("2019-01", "2019-12"),
        months("2020-01", "2023-12"),
    ),
}

# Canonical schema every source is normalized into. Timestamps are timezone-naive
# (Delta TIMESTAMP_NTZ) so hour/month extraction never depends on a session time zone.
TRIP_SCHEMA = pa.schema(
    [
        ("trip_id", pa.int64()),
        ("vendor_id", pa.int32()),
        ("pickup_ts", pa.timestamp("us")),
        ("dropoff_ts", pa.timestamp("us")),
        ("passenger_count", pa.int32()),
        ("trip_distance", pa.float64()),
        ("pu_location_id", pa.int32()),
        ("do_location_id", pa.int32()),
        ("payment_type", pa.int32()),
        ("fare_amount", pa.float64()),
        ("tip_amount", pa.float64()),
        ("total_amount", pa.float64()),
    ]
)

# Source column name -> canonical name. Matching is case-insensitive.
COLUMN_ALIASES: dict[str, list[str]] = {
    "vendor_id": ["vendorid", "vendor_id"],
    "pickup_ts": ["tpep_pickup_datetime", "pickup_datetime", "pickup_ts"],
    "dropoff_ts": ["tpep_dropoff_datetime", "dropoff_datetime", "dropoff_ts"],
    "passenger_count": ["passenger_count"],
    "trip_distance": ["trip_distance"],
    "pu_location_id": ["pulocationid", "pickup_location_id", "pu_location_id"],
    "do_location_id": ["dolocationid", "dropoff_location_id", "do_location_id"],
    "payment_type": ["payment_type"],
    "fare_amount": ["fare_amount"],
    "tip_amount": ["tip_amount"],
    "total_amount": ["total_amount"],
}

# The corrections batch used by the MERGE stage: every trip whose id hits this
# modulus gets a $1.00 tip adjustment, and the same number of new trips is inserted.
CORRECTION_MODULUS = 1000
CORRECTION_TIP_DELTA = 1.0


def tier_dir(tier: str) -> Path:
    return DATA_DIR / "tiers" / tier


def table_paths(tier: str) -> dict[str, Path]:
    d = tier_dir(tier)
    return {"trips": d / "trips", "zones": d / "zones", "corrections": d / "corrections"}
