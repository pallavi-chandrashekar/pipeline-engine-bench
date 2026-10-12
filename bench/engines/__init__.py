"""Engine adapters. Each exposes the same interface:

    start()                                   create the session / connection
    register(trips_path, zones_path)          point the engine at the bronze tables
    transform_write(out_path)                 run SILVER_SQL and write a Delta table
    aggregate() -> pyarrow.Table
    top_zones() -> pyarrow.Table
    merge(target_path, corrections_path)      upsert; return a note string, or raise
                                              NotSupported
    stop()
"""

ENGINES = ["spark", "duckdb", "polars", "delta-rs"]


class NotSupported(Exception):
    pass


def load(name: str):
    if name == "spark":
        from bench.engines.spark_engine import SparkEngine as E
    elif name == "duckdb":
        from bench.engines.duckdb_engine import DuckDBEngine as E
    elif name == "polars":
        from bench.engines.polars_engine import PolarsEngine as E
    elif name == "delta-rs":
        from bench.engines.deltars_engine import DeltaRsEngine as E
    else:
        raise ValueError(f"unknown engine {name}; choose from {ENGINES}")
    return E()
