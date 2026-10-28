import json
import os
from pathlib import Path

import psutil
import pyarrow as pa

from bench import config as C
from bench import workload as W
from bench.engines import NotSupported

# After the first (untimed warm-up) session resolves the Delta jars through Ivy, their
# local paths are cached here so timed runs load them directly. Otherwise every timed
# startup would include Maven/Ivy dependency resolution, which a real Spark install
# with jars on the classpath never pays.
JARS_CACHE = C.DATA_DIR / ".spark_delta_jars.json"


def _driver_memory() -> str:
    # Local mode runs everything in the driver JVM. Default: 60% of RAM.
    if "SPARK_DRIVER_MEMORY" in os.environ:
        return os.environ["SPARK_DRIVER_MEMORY"]
    return f"{max(2, int(psutil.virtual_memory().total / 2**30 * 0.6))}g"


def _cached_jars() -> list[str] | None:
    try:
        jars = json.loads(JARS_CACHE.read_text())
    except (OSError, ValueError):
        return None
    paths = [j.removeprefix("file://") for j in jars]
    return jars if jars and all(Path(p).exists() for p in paths) else None


class SparkEngine:
    """Apache Spark (local[*]) with Delta Lake.

    Defaults are left alone apart from driver memory, the Delta extensions, a UTC
    session time zone and disabling the UI. Extra settings can be passed as JSON in
    SPARK_EXTRA_CONF, e.g. SPARK_EXTRA_CONF='{"spark.sql.shuffle.partitions": "16"}'.

    BENCH_SPARK_NO_DELTA=1 reads the Delta snapshot's parquet files and writes plain
    parquet instead (MERGE is skipped). Only for machines that cannot download the
    Delta jars from Maven Central; never use it for published numbers.
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
        jars_mode = "none"
        if not self.no_delta:
            b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            cached = _cached_jars()
            if cached:
                b = b.config("spark.jars", ",".join(cached))
                jars_mode = "local"
            else:
                from delta import configure_spark_with_delta_pip

                b = configure_spark_with_delta_pip(b)
                jars_mode = "ivy-resolved"
        self.spark = b.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        if jars_mode == "ivy-resolved":
            jars = [j for j in self.spark.sparkContext.getConf().get("spark.jars", "").split(",") if j]
            if any("delta-spark" in j for j in jars):
                JARS_CACHE.parent.mkdir(parents=True, exist_ok=True)
                JARS_CACHE.write_text(json.dumps(jars))
        return f"driver_memory={_driver_memory()}; delta_jars={jars_mode}" + (
            "; NO-DELTA test mode" if self.no_delta else "")

    def _read(self, path):
        if self.no_delta:
            if not (Path(path) / "_delta_log").exists():
                return self.spark.read.parquet(path)  # silver written by this engine in test mode
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

    def register_silver(self, silver_path):
        self._read(silver_path).createOrReplaceTempView("silver")

    @staticmethod
    def _to_arrow(df):
        # DataFrame.toArrow() is Spark 4.0+; Spark 3.x exposes _collect_as_arrow().
        if hasattr(df, "toArrow"):
            return df.toArrow()
        return pa.Table.from_batches(df._collect_as_arrow())

    def aggregate(self):
        return self._to_arrow(self.spark.sql(W.AGGREGATE_SQL))

    def top_zones(self):
        return self._to_arrow(self.spark.sql(W.TOP_ZONES_SQL))

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
