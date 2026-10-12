import json
import os

import psutil

from bench import workload as W
from bench.engines import NotSupported


def _driver_memory() -> str:
    # Local mode runs everything in the driver JVM. Default: 60% of RAM.
    if "SPARK_DRIVER_MEMORY" in os.environ:
        return os.environ["SPARK_DRIVER_MEMORY"]
    return f"{max(2, int(psutil.virtual_memory().total / 2**30 * 0.6))}g"


class SparkEngine:
    """Apache Spark (local[*]) with Delta Lake.

    Defaults are left alone apart from driver memory, the Delta extensions and
    disabling the UI. Extra settings can be passed as JSON in SPARK_EXTRA_CONF,
    e.g. SPARK_EXTRA_CONF='{"spark.sql.shuffle.partitions": "16"}'.

    BENCH_SPARK_NO_DELTA=1 reads the Delta snapshot's parquet files and writes
    plain parquet instead (MERGE is skipped). Only for machines that cannot download
    the Delta jars from Maven Central; never use it for published numbers.
    """

    name = "spark"

    def start(self):
        from pyspark.sql import SparkSession

        self.no_delta = os.environ.get("BENCH_SPARK_NO_DELTA") == "1"
        b = (
            SparkSession.builder.master("local[*]")
            .appName("pipeline-engine-bench")
            .config("spark.driver.memory", _driver_memory())
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        )
        for k, v in json.loads(os.environ.get("SPARK_EXTRA_CONF", "{}")).items():
            b = b.config(k, v)
        if not self.no_delta:
            from delta import configure_spark_with_delta_pip

            b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            b = configure_spark_with_delta_pip(b)
        self.spark = b.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        return f"driver_memory={_driver_memory()}" + ("; NO-DELTA test mode" if self.no_delta else "")

    def _read(self, path):
        if self.no_delta:
            from deltalake import DeltaTable

            return self.spark.read.parquet(*DeltaTable(path).file_uris())
        return self.spark.read.format("delta").load(path)

    def register(self, trips_path, zones_path):
        self._read(trips_path).createOrReplaceTempView("trips")
        self._read(zones_path).createOrReplaceTempView("zones")

    def transform_write(self, out_path):
        w = self.spark.sql(W.SILVER_SQL).write.mode("overwrite").partitionBy(*W.SILVER_PARTITIONS)
        if self.no_delta:
            w.parquet(out_path)
        else:
            w.format("delta").save(out_path)

    def aggregate(self):
        return self.spark.sql(W.AGGREGATE_SQL).toArrow()

    def top_zones(self):
        return self.spark.sql(W.TOP_ZONES_SQL).toArrow()

    def merge(self, target_path, corrections_path):
        if self.no_delta:
            raise NotSupported("skipped in NO-DELTA test mode")
        from delta.tables import DeltaTable

        source = self.spark.read.format("delta").load(corrections_path)
        (
            DeltaTable.forPath(self.spark, target_path)
            .alias("t")
            .merge(source.alias("s"), f"t.{W.MERGE_KEY} = s.{W.MERGE_KEY}")
            .whenMatchedUpdate(set={c: f"s.{c}" for c in W.MERGE_UPDATE_COLS})
            .whenNotMatchedInsertAll()
            .execute()
        )

    def stop(self):
        self.spark.stop()
