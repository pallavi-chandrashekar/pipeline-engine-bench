import csv
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from bench import config as C
from bench import ingest, synth, verify


def _fake_tlc(path, n, month_start, stray=0):
    """A file shaped like the real NYC TLC parquet (column names, ns timestamps, float passenger_count)."""
    rng = np.random.default_rng(0)
    base = np.datetime64(month_start, "ns")
    pickup = base + rng.integers(0, 27 * 86400, n).astype("timedelta64[s]")
    if stray:
        pickup[:stray] = np.datetime64("2008-12-31T23:00:00", "ns")  # junk dates exist in the real data
    tbl = pa.table({
        "VendorID": pa.array(rng.integers(1, 3, n), pa.int64()),
        "tpep_pickup_datetime": pa.array(pickup, pa.timestamp("ns")),
        "tpep_dropoff_datetime": pa.array(pickup + np.timedelta64(600, "s"), pa.timestamp("ns")),
        "passenger_count": pa.array(rng.integers(0, 5, n).astype(float), pa.float64()),
        "trip_distance": pa.array(rng.random(n) * 5),
        "RatecodeID": pa.array(np.ones(n)),
        "store_and_fwd_flag": pa.array(["N"] * n),
        "PULocationID": pa.array(rng.integers(1, 266, n), pa.int64()),
        "DOLocationID": pa.array(rng.integers(1, 266, n), pa.int64()),
        "payment_type": pa.array(rng.integers(1, 5, n), pa.int64()),
        "fare_amount": pa.array(rng.random(n) * 30),
        "tip_amount": pa.array(rng.random(n) * 5),
        "total_amount": pa.array(rng.random(n) * 40),
        "airport_fee": pa.array(np.zeros(n)),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, path)


def _fake_share(path, n):
    """A file shaped like a Databricks-style taxi export (snake_case names, string dates)."""
    rng = np.random.default_rng(1)
    pickup = [(datetime(2019, 1, 1) + timedelta(seconds=int(s))) for s in rng.integers(0, 28 * 86400, n)]
    tbl = pa.table({
        "vendor_id": pa.array(rng.integers(1, 3, n), pa.int32()),
        "pickup_datetime": pa.array([p.isoformat(sep=" ") for p in pickup]),
        "dropoff_datetime": pa.array([(p + timedelta(minutes=9)).isoformat(sep=" ") for p in pickup]),
        "passenger_count": pa.array(rng.integers(1, 5, n), pa.int32()),
        "trip_distance": pa.array(rng.random(n) * 5),
        "pickup_location_id": pa.array(rng.integers(1, 266, n), pa.int32()),
        "dropoff_location_id": pa.array(rng.integers(1, 266, n), pa.int32()),
        "payment_type": pa.array(rng.integers(1, 5, n).astype(str)),
        "fare_amount": pa.array(rng.random(n) * 30),
        "tip_amount": pa.array(rng.random(n) * 5),
        "total_amount": pa.array(rng.random(n) * 40),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, path)


def test_ingest_normalizes_both_source_shapes():
    tlc = ingest.RAW / "tlc" / "yellow_tripdata_2020-01.parquet"
    share = ingest.RAW / "share" / "00000.parquet"
    _fake_tlc(tlc, 5000, "2020-01-01", stray=10)
    _fake_share(share, 4000)
    zones = synth.write_zones()

    staged_tlc = ingest.stage("tlc", [tlc], lambda f: int(f.stem[-7:].replace("-", "")))
    staged_share = ingest.stage("share", [share], lambda f: int(f.stem))
    m = ingest.build_tier("T", [(staged_share, ["2019-01"]), (staged_tlc, ["2020-01"])], zones, {"source": "test"})

    assert m["rows"] == 4000 + 5000 - 10  # stray 2008 rows dropped by the month filter
    dt = DeltaTable(str(C.table_paths("T")["trips"]))
    got = pa.schema([(f.name, f.type) for f in pa.schema(dt.schema().to_arrow())])
    assert got.equals(C.TRIP_SCHEMA), got
    ids = pa.table(dt.to_pyarrow_table(columns=["trip_id"])).column(0).to_numpy()
    assert len(np.unique(ids)) == len(ids)
    assert m["merge_expected"]["updates"] > 0


def test_compare_tables_catches_wrong_answers():
    ref = pa.table({"k": [1, 2, 3], "v": [1.0, 2.0, 3.0], "s": ["a", "b", "c"]})
    assert verify.compare_tables(ref, ref, ["k"]) == "pass"
    shuffled = pa.table({"s": ["c", "a", "b"], "k": [3, 1, 2], "v": [3.0, 1.0, 2.0]})
    assert verify.compare_tables(ref, shuffled, ["k"]) == "pass"
    wrong = pa.table({"k": [1, 2, 3], "v": [1.0, 2.0, 3.01], "s": ["a", "b", "c"]})
    assert verify.compare_tables(ref, wrong, ["k"]).startswith("FAIL")
    short = ref.slice(0, 2)
    assert verify.compare_tables(ref, short, ["k"]).startswith("FAIL")


def _java_available():
    return shutil.which("java") is not None


def test_end_to_end_smoke():
    synth.main(["--rows", "300000", "--tier", "SMOKE"])
    engines = ["duckdb", "polars", "delta-rs"]
    env = dict(os.environ)
    if _java_available() and os.environ.get("BENCH_TEST_SPARK") == "1":
        engines.insert(0, "spark")
    subprocess.run(
        [sys.executable, "-m", "bench.harness", "--tiers", "SMOKE", "--repeats", "1",
         "--engines", ",".join(engines), "--run-id", "t"],
        check=True, env=env, cwd=C.ROOT,
    )
    run = C.RESULTS_DIR / "t"
    rows = list(csv.DictReader(open(run / "runs.csv")))
    checked = [r for r in rows if r["verified"]]
    assert checked, "no checks ran"
    assert all(r["verified"] == "pass" for r in checked), [r for r in checked if r["verified"] != "pass"]
    merges = {r["engine"]: r["status"] for r in rows if r["stage"] == "merge"}
    assert merges["duckdb"] == "n/a" and merges["polars"] == "ok" and merges["delta-rs"] == "ok"
    assert (run / "summary.md").exists() and (run / "pipeline_time.png").exists()


def test_share_download_path(monkeypatch, tmp_path):
    """Exercise download_share() against a local HTTP server standing in for pre-signed URLs."""
    import functools
    import http.server
    import threading
    from types import SimpleNamespace

    serve = tmp_path / "srv"
    _fake_share(serve / "f1.parquet", 300)
    _fake_share(serve / "f2.parquet", 200)
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(serve))
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    port = httpd.server_address[1]

    files = [SimpleNamespace(id=n, url=f"http://127.0.0.1:{port}/{n}.parquet", size=(serve / f"{n}.parquet").stat().st_size)
             for n in ("f2", "f1")]
    rest = SimpleNamespace(list_files_in_table=lambda table: SimpleNamespace(add_files=files))
    monkeypatch.setattr(ingest, "_share_client", lambda: (None, rest))
    monkeypatch.setattr(ingest, "RAW", tmp_path / "raw")
    try:
        got = ingest.download_share()
    finally:
        httpd.shutdown()
    assert [p.name for p in got] == ["00000.parquet", "00001.parquet"]
    assert pq.read_metadata(got[0]).num_rows == 300  # sorted by file id: f1 first
