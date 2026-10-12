PY ?= .venv/bin/python
REPEATS ?= 3

.PHONY: setup test smoke list-share data-S data-M data-L data-XL bench bench-xl report clean

setup:            ## create .venv and install everything
	python3 -m venv .venv
	.venv/bin/pip install -U pip setuptools wheel
	.venv/bin/pip install -r requirements.txt

test:             ## unit + end-to-end tests on synthetic data (no network)
	$(PY) -m pytest -q

smoke:            ## 2M synthetic rows through all four engines, incl. Spark + Delta
	$(PY) -m bench.synth --rows 2000000
	$(PY) -m bench.harness --tiers SYN --repeats 1 --run-id smoke

list-share:       ## show what the open Delta Sharing server offers
	$(PY) -m bench.ingest list

data-%:           ## download + build a tier, e.g. make data-S (SOURCE=tlc to skip the share)
	$(PY) -m bench.ingest prepare --tier $* --source $(or $(SOURCE),share)

bench:            ## S, M, L x all engines x REPEATS
	$(PY) -m bench.harness --tiers S,M,L --repeats $(REPEATS)

bench-xl:         ## the XL tier on its own (long; needs ~40 GB free disk)
	$(PY) -m bench.harness --tiers XL --repeats $(REPEATS)

report:           ## rebuild summary + charts: make report RUN=results/<run_id>
	$(PY) -m bench.report $(RUN)

clean:            ## remove downloaded data and staging (keeps results)
	rm -rf data
