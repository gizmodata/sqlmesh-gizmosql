.PHONY: install install-dev test unit-test integration-test docker-up docker-down clean lint format

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

# Start GizmoSQL Docker container
docker-up:
	docker compose -f tests/integration/docker/compose.gizmosql.yaml up -d
	./scripts/wait-for-gizmosql.sh

# Stop GizmoSQL Docker container
docker-down:
	docker compose -f tests/integration/docker/compose.gizmosql.yaml down -v

# Run integration tests (requires Docker)
integration-test: docker-up
	pytest tests/integration/ -v --tb=short
	$(MAKE) docker-down

# Run all tests including integration
all-tests: unit-test integration-test

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
