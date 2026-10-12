import os

import polars as pl

from bench import workload as W

# Polars' default is engine="auto". We use its streaming engine because it is the one
# Polars recommends for data that may not fit in memory, which is the point of the L/XL
# tiers. Override with POLARS_ENGINE=auto (or in-memory) to compare. Disclosed in README.
ENGINE = os.environ.get("POLARS_ENGINE", "streaming")


class PolarsEngine:
    """Polars lazy API. Reads Delta via pl.scan_delta; Delta writes and MERGE are
    delegated to delta-rs under the hood (that's how Polars writes Delta)."""

    name = "polars"

    def start(self):
        return f"polars engine={ENGINE}; write+merge=delta-rs"

    def register(self, trips_path, zones_path):
        self.trips = pl.scan_delta(trips_path)
        self.zones = pl.scan_delta(zones_path)

    def transform_write(self, out_path):
        pz = self.zones.select(pl.col("location_id"), pl.col("borough").alias("pu_borough"))
        dz = self.zones.select(pl.col("location_id"), pl.col("borough").alias("do_borough"))
        silver = (
            self.trips.filter(
                (pl.col("trip_distance") > 0)
                & (pl.col("total_amount") > 0)
                & (pl.col("dropoff_ts") > pl.col("pickup_ts"))
            )
            .join(pz, left_on="pu_location_id", right_on="location_id", how="inner")
            .join(dz, left_on="do_location_id", right_on="location_id", how="left")
            .with_columns(
                pl.col("pickup_ts").dt.year().cast(pl.Int32).alias("year"),
                pl.col("pickup_ts").dt.month().cast(pl.Int32).alias("month"),
            )
        )
        silver.sink_delta(
            out_path,
            mode="overwrite",
            delta_write_options={"partition_by": W.SILVER_PARTITIONS},
            engine=ENGINE,
        )

    def register_silver(self, silver_path):
        self.silver = pl.scan_delta(silver_path)

    def aggregate(self):
        return (
            self.silver.group_by(
                pl.col("pu_borough").alias("borough"),
                pl.col("year"),
                pl.col("month"),
                pl.col("pickup_ts").dt.hour().cast(pl.Int32).alias("hour"),
            )
            .agg(
                pl.len().alias("trips"),
                pl.col("total_amount").sum().alias("revenue"),
                pl.col("trip_distance").mean().alias("avg_distance"),
            )
            .collect(engine=ENGINE)
            .to_arrow()
        )

    def top_zones(self):
        zr = (
            self.silver.join(
                self.zones.select("location_id", "zone"), left_on="pu_location_id", right_on="location_id", how="inner"
            )
            .group_by(pl.col("pu_borough").alias("borough"), "zone", pl.col("pu_location_id").alias("location_id"))
            .agg(
                pl.len().alias("trips"),
                pl.col("total_amount").sum().alias("revenue"),
                pl.when(pl.col("fare_amount") > 0)
                .then(pl.col("tip_amount") / pl.col("fare_amount"))
                .mean()
                .alias("avg_tip_ratio"),
            )
        )
        return (
            zr.sort(["borough", "revenue", "location_id"], descending=[False, True, False])
            .with_columns(pl.int_range(1, pl.len() + 1).over("borough").alias("rnk"))
            .filter(pl.col("rnk") <= 10)
            .collect(engine=ENGINE)
            .to_arrow()
        )

    def merge(self, target_path, corrections_path):
        (
            pl.read_delta(corrections_path)
            .write_delta(
                target_path,
                mode="merge",
                delta_merge_options={
                    "predicate": f"t.{W.MERGE_KEY} = s.{W.MERGE_KEY}",
                    "source_alias": "s",
                    "target_alias": "t",
                },
            )
            .when_matched_update(updates={c: f"s.{c}" for c in W.MERGE_UPDATE_COLS})
            .when_not_matched_insert_all()
            .execute()
        )

    def stop(self):
        pass
