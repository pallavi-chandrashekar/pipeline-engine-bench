import duckdb
from deltalake import DeltaTable, write_deltalake

from bench import workload as W
from bench.engines import NotSupported


class DuckDBEngine:
    name = "duckdb"

    def start(self):
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        # Prefer DuckDB's native Delta reader. If the extension can't be installed
        # (offline machine), fall back to scanning the snapshot's parquet files,
        # resolved through the Delta log by delta-rs. Recorded in the results.
        try:
            self.con.execute("INSTALL delta; LOAD delta")
            self.read_mode = "delta_scan"
        except duckdb.Error:
            self.read_mode = "parquet_files"

    def _source(self, path: str) -> str:
        if self.read_mode == "delta_scan":
            return f"delta_scan('{path}')"
        files = ", ".join(f"'{f}'" for f in DeltaTable(path).file_uris())
        return f"read_parquet([{files}])"

    def register(self, trips_path, zones_path):
        self.con.execute(f"CREATE OR REPLACE VIEW trips AS SELECT * FROM {self._source(trips_path)}")
        self.con.execute(f"CREATE OR REPLACE VIEW zones AS SELECT * FROM {self._source(zones_path)}")

    def transform_write(self, out_path):
        # DuckDB has no native Delta writer; it streams Arrow batches to delta-rs.
        reader = self.con.execute(W.SILVER_SQL).to_arrow_reader(1_000_000)
        write_deltalake(out_path, reader, mode="overwrite", partition_by=W.SILVER_PARTITIONS)
        return f"read={self.read_mode}; write=delta-rs"

    def aggregate(self):
        return self.con.execute(W.AGGREGATE_SQL).to_arrow_table()

    def top_zones(self):
        return self.con.execute(W.TOP_ZONES_SQL).to_arrow_table()

    def merge(self, target_path, corrections_path):
        raise NotSupported("DuckDB cannot MERGE into a Delta table")

    def stop(self):
        self.con.close()
