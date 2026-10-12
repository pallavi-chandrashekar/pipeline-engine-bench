"""The pipeline every engine runs. Spark, DuckDB and delta-rs (DataFusion) execute the
exact same SQL text below; Polars implements the same logic with its expression API.

Stages
  transform_write  bronze -> silver: clean, enrich with zones (2 joins), write a Delta
                   table partitioned by year/month
  aggregate        revenue by pickup borough x year x month x hour
  top_zones        top 10 pickup zones per borough by revenue (window function)
  merge            upsert a corrections batch (scattered updates + new trips) into a
                   fresh copy of the bronze table
"""

STAGES = ["transform_write", "aggregate", "top_zones", "merge"]

SILVER_SQL = """
SELECT
    t.*,
    pz.borough AS pu_borough,
    dz.borough AS do_borough,
    CAST(EXTRACT(YEAR FROM t.pickup_ts) AS INT) AS year,
    CAST(EXTRACT(MONTH FROM t.pickup_ts) AS INT) AS month
FROM trips t
JOIN zones pz ON t.pu_location_id = pz.location_id
LEFT JOIN zones dz ON t.do_location_id = dz.location_id
WHERE t.trip_distance > 0
  AND t.total_amount > 0
  AND t.dropoff_ts > t.pickup_ts
"""

AGGREGATE_SQL = """
SELECT
    z.borough AS borough,
    CAST(EXTRACT(YEAR FROM t.pickup_ts) AS INT) AS year,
    CAST(EXTRACT(MONTH FROM t.pickup_ts) AS INT) AS month,
    CAST(EXTRACT(HOUR FROM t.pickup_ts) AS INT) AS hour,
    COUNT(*) AS trips,
    SUM(t.total_amount) AS revenue,
    AVG(t.trip_distance) AS avg_distance
FROM trips t
JOIN zones z ON t.pu_location_id = z.location_id
GROUP BY
    z.borough,
    CAST(EXTRACT(YEAR FROM t.pickup_ts) AS INT),
    CAST(EXTRACT(MONTH FROM t.pickup_ts) AS INT),
    CAST(EXTRACT(HOUR FROM t.pickup_ts) AS INT)
"""

TOP_ZONES_SQL = """
WITH zr AS (
    SELECT
        z.borough AS borough,
        z.zone AS zone,
        t.pu_location_id AS location_id,
        COUNT(*) AS trips,
        SUM(t.total_amount) AS revenue,
        AVG(CASE WHEN t.fare_amount > 0 THEN t.tip_amount / t.fare_amount END) AS avg_tip_ratio
    FROM trips t
    JOIN zones z ON t.pu_location_id = z.location_id
    GROUP BY z.borough, z.zone, t.pu_location_id
),
ranked AS (
    SELECT
        borough, zone, location_id, trips, revenue, avg_tip_ratio,
        ROW_NUMBER() OVER (PARTITION BY borough ORDER BY revenue DESC, location_id) AS rnk
    FROM zr
)
SELECT borough, zone, location_id, trips, revenue, avg_tip_ratio, rnk
FROM ranked
WHERE rnk <= 10
"""

# MERGE semantics (identical for every engine that supports it):
#   ON target.trip_id = source.trip_id
#   WHEN MATCHED     -> update tip_amount, total_amount
#   WHEN NOT MATCHED -> insert all columns
MERGE_KEY = "trip_id"
MERGE_UPDATE_COLS = ["tip_amount", "total_amount"]

SILVER_PARTITIONS = ["year", "month"]

# Result keys used to compare engines' outputs.
RESULT_KEYS = {
    "aggregate": ["borough", "year", "month", "hour"],
    "top_zones": ["borough", "rnk"],
}
