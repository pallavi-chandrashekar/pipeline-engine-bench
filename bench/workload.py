"""The pipeline every engine runs. Spark, DuckDB and delta-rs (DataFusion) execute the
exact same SQL text below; Polars implements the same logic with its expression API.

Stages (bronze -> silver -> gold)
  transform_write  bronze -> silver: clean, enrich with pickup/dropoff borough (2 joins),
                   write a Delta table partitioned by year/month
  aggregate        gold, read from the engine's OWN silver output: revenue by pickup
                   borough x year x month x hour
  top_zones        gold, from silver + zones: top 10 pickup zones per borough by revenue
                   (window function)
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
    pu_borough AS borough,
    year,
    month,
    CAST(EXTRACT(HOUR FROM pickup_ts) AS INT) AS hour,
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    AVG(trip_distance) AS avg_distance
FROM silver
GROUP BY pu_borough, year, month, CAST(EXTRACT(HOUR FROM pickup_ts) AS INT)
"""

TOP_ZONES_SQL = """
WITH zr AS (
    SELECT
        s.pu_borough AS borough,
        z.zone AS zone,
        s.pu_location_id AS location_id,
        COUNT(*) AS trips,
        SUM(s.total_amount) AS revenue,
        AVG(CASE WHEN s.fare_amount > 0 THEN s.tip_amount / s.fare_amount END) AS avg_tip_ratio
    FROM silver s
    JOIN zones z ON s.pu_location_id = z.location_id
    GROUP BY s.pu_borough, z.zone, s.pu_location_id
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
SILVER_CHECK_KEYS = ["year", "month", "pu_borough", "do_borough"]

RESULT_KEYS = {
    "aggregate": ["borough", "year", "month", "hour"],
    "top_zones": ["borough", "rnk"],
}
