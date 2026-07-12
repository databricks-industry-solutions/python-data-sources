# AGENTS.md

This file provides guidance to LLM when working with code in this repository.

You're experienced Spark developer.  You're developing custom Python data sources to
simplify work (read/write) with data in different external systems.  You follow the
architecture and development guidelines outlined below.

## Project Overview

This repository contains source code of Python data sources (part of Apache Spark API) for
working with data in different external systems in batch and/or streaming manner.

**Architecture**:  All data sources live in a single package under `src/python_data_sources/`, with one subpackage per external system (e.g. `mcap/`, `mqtt/`, `zipdcm/`) and shared helpers in `common/`. Inside each subpackage a simple, flat structure with one data source per file is preferred.

For the mechanics of the PySpark DataSource API itself — the `DataSource`, reader, writer, and streaming classes and how they fit together — follow the `spark-python-data-source` skill in the `ai-dev-kit` (linked below). This file only captures the conventions and workflow that are specific to *this* repository, so the API guidance is not duplicated here and can evolve independently in the skill.

### Documentation and examples of custom data sources using the same API

There is a number of publicly available examples that demonstrate how to implement custom Python data sources:

- https://github.com/alexott/cyber-spark-data-connectors
- https://github.com/databricks/tmm/tree/main/Lakeflow-OpenSkyNetwork
- https://github.com/allisonwang-db/pyspark-data-sources
- https://github.com/databricks-industry-solutions/python-data-sources
- https://github.com/dmatrix/spark-misc/tree/main/src/py/data_source
- https://github.com/huggingface/pyspark_huggingface
- https://github.com/dmoore247/PythonDataSources
- https://github.com/dgomez04/pyspark-hubspot
- https://github.com/dgomez04/pyspark-faker
- https://github.com/skyler-myers-db/activemq_pyspark_connector
- https://github.com/jiteshsoni/ethereum-streaming-pipeline/blob/6e06cdea573780ba09a33a334f7f07539721b85e/ethereum_block_stream_chainstack.py
- https://www.canadiandataguy.com/p/stop-waiting-for-connectors-stream
- https://www.databricks.com/blog/simplify-data-ingestion-new-python-data-source-api

More information about Spark Python data sources could be found in the documentation:

- https://docs.databricks.com/aws/en/pyspark/datasources
- https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html

The guidance in this file is also available as a reusable Databricks skill in the `ai-dev-kit`:

- https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills/spark-python-data-source


## Architecture Patterns

### Data Source Implementation Pattern

The full pattern for structuring a data source — the `DataSource` entry-point class, the
shared base writer/reader, the batch and streaming subclasses, offsets, and partitions — is
documented in the `spark-python-data-source` skill (see the link in "Project Overview").
Implement new sources by following that skill; the sections below only add the repository's
own conventions on top of it.

### Key Design Principles

1. **SIMPLE over CLEVER**: No abstract base classes, factory patterns, or complex inheritance
2. **EXPLICIT over IMPLICIT**: Direct implementations, no hidden abstractions
3. **FLAT over NESTED**: Single-level inheritance (DataSource → Writer → Batch/Stream)
4. **Imports inside methods**: For partition-level execution, import libraries within `write()` methods
5. **Row-by-row processing**: Iterate rows, batch them, send when buffer full

## Adding a New Data Source

Each data source is a subpackage under `src/python_data_sources/` (see `mcap/`, `mqtt/`,
and `zipdcm/` as templates), with its tests under `tests/unit/<source>/` and any demos
under `examples/<source>/`. A typical layout is:

```
src/python_data_sources/yoursource/
├── __init__.py               # exports the DataSource class (and readers/partitions)
└── yoursource_datasource.py  # data source implementation
tests/unit/yoursource/
├── conftest.py               # fixtures
└── test_yoursource.py        # unit tests
examples/yoursource/          # optional notebooks / demo assets
```

Shared logic (e.g. `RangePartition`) lives in `src/python_data_sources/common/`.

Follow this checklist (use existing sources as templates):

1. Create the subpackage `src/python_data_sources/yoursource/` with an `__init__.py`.
2. Create the data source module (e.g. `yoursource_datasource.py`).
3. Implement `YourSourceDataSource(DataSource)` with `name()`, `reader()`/`streamReader()`,
   and `writer()`/`streamWriter()` as needed.
4. Implement the base writer class with:
   - Options validation in `__init__`
   - `write(iterator)` method with write logic
5. Implement batch and stream writer classes (minimal boilerplate)
6. Implement the base reader class with:
   - Options validation in `__init__`
   - `read(partition)` method with read logic
   - `partitions(start, end)` method to split data into partitions (reuse
     `common.range_partition.RangePartition` where it fits)
7. Implement batch and stream reader classes (minimal boilerplate)
8. Export the public classes from the subpackage's `__init__.py`
9. Add the source's runtime dependencies as an extra in `pyproject.toml` under
   `[project.optional-dependencies]`, then run `make lock-dependencies`
10. Create unit tests under `tests/unit/yoursource/`
11. Update the top-level `README.md` with usage examples and options

### Data Source Registration

Users register data sources like this:
```python
from python_data_sources.zipdcm import ZipDCMDataSource
spark.dataSource.register(ZipDCMDataSource)

# Then use with .format("zipdcm")
df.read.format("zipdcm").option("...", "...").load()
```

## 🚨 SENIOR DEVELOPER GUIDELINES 🚨

**CRITICAL: This project follows SIMPLE, MAINTAINABLE patterns. DO NOT over-engineer!**

### Forbidden Patterns (DO NOT ADD THESE)

- ❌ **Abstract base classes** or complex inheritance hierarchies
- ❌ **Factory patterns** or dependency injection containers
- ❌ **Decorators for cross-cutting concerns** (logging, caching, performance monitoring)
- ❌ **Complex configuration classes** with nested structures
- ❌ **Async/await patterns** unless absolutely necessary
- ❌ **Connection pooling** or caching layers
- ❌ **Generic "framework" code** or reusable utilities
- ❌ **Complex error handling systems** or custom exceptions
- ❌ **Performance optimization** patterns (premature optimization)
- ❌ **Enterprise patterns** like singleton, observer, strategy, etc.

### Required Patterns (ALWAYS USE THESE)
- ✅ **Direct function calls** - no indirection or abstraction layers
- ✅ **Simple classes** with clear, single responsibilities
- ✅ **Environment variables** for configuration (no complex config objects)
- ✅ **Explicit imports** - import exactly what you need
- ✅ **Basic error handling** with try/catch and simple return dictionaries
- ✅ **Straightforward control flow** - avoid complex conditional logic
- ✅ **Standard library first** - only add dependencies when absolutely necessary

### Implementation Rules

1. **One concept per file**: Each module should have a single, clear purpose
2. **Minimal, flat classes**: The PySpark DataSource API is class-based — every source must
   extend the base classes (`DataSource`, `DataSourceReader`/`DataSourceStreamReader`,
   `DataSourceWriter`/`DataSourceStreamWriter`). Keep the hierarchy flat and the classes
   thin; use plain functions for helper logic that does not need to hold state.
3. **Direct SDK calls**: Call Databricks SDK directly, no wrapper layers
4. **Simple data structures**: Use dicts and lists, avoid custom data classes
5. **Basic testing**: Simple unit tests with basic mocking, no complex test frameworks
6. **Minimal dependencies**: Only add new dependencies if critically needed

### Code Review Questions

Before adding any code, ask yourself:
- "Is this the simplest way to solve this problem?"
- "Would a new developer understand this immediately?"
- "Am I adding abstraction for a real need or hypothetical flexibility?"
- "Can I solve this with standard library or existing dependencies?"
- "Does this follow the existing patterns in the codebase?"

## Development Commands

The project uses [`uv`](https://docs.astral.sh/uv/) for packaging and a top-level `Makefile`
that wraps the common workflows. Run these from the repository root and prefer the `make`
targets over ad-hoc commands so behavior stays consistent.

| Target | What it does |
|--------|--------------|
| `make dev` | Sync the environment with all extras (`uv sync --all-extras`) |
| `make fmt` | Auto-format and fix (`ruff format`, `ruff check --fix`, `pylint`) |
| `make lint` | Check formatting and lint without modifying files (`ruff` + `pylint`) |
| `make test` | Run unit tests with coverage (`pytest tests/unit`) |
| `make coverage` | Run unit tests and open the HTML coverage report |
| `make e2e` | Run end-to-end tests (`pytest tests/e2e`) |
| `make lock-dependencies` | Regenerate and sanitize `uv.lock` after changing dependencies |
| `make clean` | Remove the virtualenv, build artifacts, and caches |
| `make all` | Full local run (`clean` + `lint` + `fmt` + `test`) |

## Development Workflow

### Package Management

- Dependencies are declared in `pyproject.toml`: core deps under `[project.dependencies]`,
  per-source runtime deps as extras under `[project.optional-dependencies]` (`mcap`, `mqtt`,
  `zipdcm`, `all`), and dev tooling under `[dependency-groups]`.
- After editing dependencies, run `make lock-dependencies` to update `uv.lock`; never edit
  the lockfile by hand.
- Always check if a dependency already exists before adding a new one.
- **Principle**: Only add dependencies if absolutely critical.

### Setup
```bash
make dev             # uv sync --all-extras (creates .venv with all extras)
```

### Testing
```bash
# Run the full unit suite with coverage
make test

# Run pytest directly for finer control (via uv)
uv run pytest tests/unit                                          # all unit tests
uv run pytest tests/unit/mcap                                     # one source
uv run pytest tests/unit/mcap/test_mcap_datasource.py             # specific file
uv run pytest tests/unit/mcap/test_mcap_datasource.py::test_name  # single test
uv run pytest -v tests/unit                                       # verbose output

# Run tests for a single module via the Makefile helper
make test-module MODULE=mcap
```

### Code Quality
```bash
# Auto-format and apply fixes
make fmt

# Check only (used in CI) — ruff format --check, ruff check, pylint
make lint
```

## Testing Guidelines

- Unit tests live in `tests/unit/<source>/`; end-to-end tests live in `tests/e2e/`
- Tests use `pytest` (with `pytest-mock`/`pytest-cov`/`pytest-timeout`) and a local Spark session
- Mock external calls (brokers, HTTP, SDK) using `unittest.mock` / `pytest-mock`
- Test reader/writer initialization, option validation, and data processing logic
- See existing suites such as `tests/unit/mcap/` and `tests/unit/mqtt/` for comprehensive examples

**Test structure**:
- Put shared fixtures in a per-source `conftest.py` (see `tests/unit/mcap/conftest.py`)
- Test data source name registration
- Test reader/writer instantiation (batch and streaming)
- Test option validation (required vs optional parameters)
- Mock external responses to test read/write operations

## Important Notes

- **Python version**: 3.12–3.13 (`requires-python = ">=3.12,<3.14"` in `pyproject.toml`)
- **Spark version**: 4.0+ required (PySpark DataSource API)
- **Dependencies**: Keep minimal - only add if critically needed; declare them in `pyproject.toml`
- **Code style**: `ruff` (format + lint, line length 120) and `pylint`, run via `make fmt` / `make lint`
- **No premature optimization**: Focus on clarity over performance

## Summary: What Makes This Project "Senior Developer Approved"

- **Readable**: Any developer can understand the code immediately
- **Maintainable**: Simple patterns that are easy to modify
- **Focused**: Each module has a single, clear responsibility
- **Direct**: No unnecessary abstractions or indirection
- **Practical**: Solves the specific problem without over-engineering

When in doubt, choose the **simpler** solution. Your future self (and your teammates) will thank you.

---

## Important Instruction Reminders

**For an agent when working on this project:**

1. **Do what has been asked; nothing more, nothing less**
2. **NEVER create files unless absolutely necessary for achieving the goal**
3. **ALWAYS prefer editing an existing file to creating a new one**
4. **NEVER proactively create documentation files (*.md) or README files**
5. **Follow the SIMPLE patterns established in this codebase**
6. **When in doubt, ask "Is this the simplest way?" before implementing**

This project is intentionally simplified. **Respect that simplicity.**
