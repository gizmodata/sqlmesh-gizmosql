.PHONY: install install-dev test unit-test integration-test ducklake-test all-integration-tests docker-up docker-down clean lint format

# Install the package
install:
	pip install -e .

# Install with dev dependencies
install-dev:
	pip install -e ".[dev]"

# Run all tests (unit only, integration requires Docker)
test: unit-test

# Run unit tests only
unit-test:
	pytest tests/test_gizmosql.py -v

# Bring up the PostgreSQL service the DuckLake tests rely on. The
# GizmoSQL server itself is started as a subprocess by the pytest
# fixture, so it is no longer part of compose.
docker-up:
	docker compose -f tests/integration/docker/compose.gizmosql.yaml up -d

docker-down:
	docker compose -f tests/integration/docker/compose.gizmosql.yaml down -v

# Run GizmoSQL-only integration tests (no Docker required).
integration-test:
	pytest -m "integration and not ducklake" tests/integration/ -v --tb=short

# Run DuckLake integration tests (requires PostgreSQL via docker-up).
ducklake-test: docker-up
	pytest -m ducklake tests/integration/ -v --tb=short
	$(MAKE) docker-down

# Run all integration tests (requires PostgreSQL via docker-up).
all-integration-tests: docker-up
	pytest -m integration tests/integration/ -v --tb=short
	$(MAKE) docker-down

# Run all tests including integration
all-tests: unit-test all-integration-tests

# Lint the code
lint:
	ruff check .
	mypy sqlmesh_gizmosql

# Format the code
format:
	ruff format .
	ruff check --fix .

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Build the package
build: clean
	pip install build
	python -m build

# Upload to PyPI (requires twine and credentials)
publish: build
	pip install twine
	twine upload dist/*

# Upload to TestPyPI first
publish-test: build
	pip install twine
	twine upload --repository testpypi dist/*
