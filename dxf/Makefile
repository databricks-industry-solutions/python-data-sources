.PHONY: dev test unit style check

all: clean style test 

clean: ## Remove build artifacts and cache files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

test:
	pip install -r requirements.txt
	pytest .

dev:
	pip install -r requirements.txt

style:
	pre-commit run --all-files

check: style test
