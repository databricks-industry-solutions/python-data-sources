.PHONY: all clean dev fmt lint test coverage lock-dependencies verify-lock build docs-install docs-build docs-serve docs-serve-dev docs-clean

all: clean lint fmt test

export UV_FROZEN := 1

UV_RUN := uv run --exact --all-extras

clean: docs-clean
	rm -fr .venv clean htmlcov .pytest_cache .ruff_cache .coverage coverage.xml dist
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

lock-dependencies: export UV_FROZEN := 0
lock-dependencies:
	uv lock --exclude-newer "7 days"
	$(UV_RUN) python -c 'import tomllib; print("\n".join(tomllib.load(open("pyproject.toml","rb"))["build-system"]["requires"]))' | \
	  uv pip compile --generate-hashes --universal --no-header - > .build-constraints-new.txt
	mv .build-constraints-new.txt .build-constraints.txt
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' uv.lock

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

verify-lock:
	@bad=$$(grep -nE 'registry = "https?://' uv.lock | grep -v 'https://pypi\.org/simple' || true); \
	if [ -n "$$bad" ]; then \
		echo "uv.lock contains non-public registry URLs:"; \
		echo "$$bad"; \
		echo "Run 'make lock' to regenerate and sanitize."; \
		exit 1; \
	fi

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

docs-install:
	yarn --cwd docs/python-data-sources install --frozen-lockfile

docs-build:
	uv run --group docs pydoc-markdown
	yarn --cwd docs/python-data-sources build

docs-serve-dev:
	uv run --group docs pydoc-markdown
	yarn --cwd docs/python-data-sources start

docs-serve: docs-build
	yarn --cwd docs/python-data-sources serve

docs-clean:
	rm -rf docs/python-data-sources/build docs/python-data-sources/.docusaurus docs/python-data-sources/.cache
	find docs/python-data-sources/docs/reference/api -mindepth 1 -not -name 'index.mdx' -exec rm -rf {} +
