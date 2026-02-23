all: clean dev fmt

clean:
	rm -fr .venv clean .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	pip install hatch
	hatch env create
	hatch run pip install ".[all]"

dev: .venv/bin/python
	@hatch run which python

fmt:
	hatch run fmt
