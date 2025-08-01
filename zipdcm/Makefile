.PHONY: dev test unit style check test-cov

all:	clean check 

clean:
	rm -fr htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -rf build dist
	rm -rf __pycache__ 
	rm -rf */__pycache__ 
	rm -rf */*/__pycache__
	rm -rf */*/*/__pycache__
	rm -rf spark_warehouse

test:
	pytest .

dev:
	pip install -r requirements-dev.txt

style:
	pre-commit run --all-files

check: style test

test-cov:
	test-cov-report && open htmlcov/index.html
