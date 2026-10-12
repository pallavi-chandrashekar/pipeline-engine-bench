from deltalake import DeltaTable, QueryBuilder, write_deltalake
import pyarrow as pa

from bench import workload as W


class DeltaRsEngine:
    """delta-rs: Rust Delta Lake implementation; queries run on its embedded DataFusion."""

    name = "delta-rs"

    def start(self):
        self.qb = QueryBuilder()

    def register(self, trips_path, zones_path):
        self.qb.register("trips", DeltaTable(trips_path))
        self.qb.register("zones", DeltaTable(zones_path))

    def transform_write(self, out_path):
        write_deltalake(out_path, self.qb.execute(W.SILVER_SQL), mode="overwrite", partition_by=W.SILVER_PARTITIONS)

    def aggregate(self):
        return pa.table(self.qb.execute(W.AGGREGATE_SQL))

    def top_zones(self):
        return pa.table(self.qb.execute(W.TOP_ZONES_SQL))

    def merge(self, target_path, corrections_path):
        source = QueryBuilder().register("c", DeltaTable(corrections_path)).execute("SELECT * FROM c")
        (
            DeltaTable(target_path)
            .merge(source=source, predicate=f"t.{W.MERGE_KEY} = s.{W.MERGE_KEY}", source_alias="s", target_alias="t")
            .when_matched_update(updates={c: f"s.{c}" for c in W.MERGE_UPDATE_COLS})
            .when_not_matched_insert_all()
            .execute()
        )

    def stop(self):
        pass
