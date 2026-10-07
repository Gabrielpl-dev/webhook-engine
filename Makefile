.PHONY: test lint format check

PYTHON ?= python3

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py' -v

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .

check: lint test
