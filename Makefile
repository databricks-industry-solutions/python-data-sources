.PHONY: all clean dev fmt lint test coverage

all: clean lint fmt test

export UV_LOCKED := 1

UV_RUN := uv run

clean:
	rm -fr .venv clean htmlcov .pytest_cache .ruff_cache .coverage coverage.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync

lint:
	$(UV_RUN) ruff format --check --diff
	$(UV_RUN) ruff check .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

fmt:
	$(UV_RUN) ruff format
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

test:
	$(UV_RUN) pytest -v --cov src --cov-report=xml --timeout 30 tests/unit

test-module:
	$(UV_RUN) pytest -v --cov src/python_data_sources/$(MODULE) --cov-report=xml --timeout 30 tests/unit/$(MODULE)

coverage:
	$(UV_RUN) pytest --cov src --cov-report=html --timeout 30 tests/unit
	open htmlcov/index.html

e2e:
	$(UV_RUN) pytest -rs --timeout 30 tests/e2e
