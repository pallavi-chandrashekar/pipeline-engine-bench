import os
import tempfile

# Isolate test data/results from real benchmark data. Must run before `bench` is imported.
_tmp = tempfile.mkdtemp(prefix="peb-test-")
os.environ["BENCH_DATA_DIR"] = os.path.join(_tmp, "data")
os.environ["BENCH_RESULTS_DIR"] = os.path.join(_tmp, "results")
