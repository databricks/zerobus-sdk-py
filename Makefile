# Default Python version (can be overridden: make build PYTHON=python3.11)
# Note: SDK requires Python 3.9+ (maturin 1.5+ requires Python 3.7+)
PYTHON ?= python3

ifeq ($(OS),Windows_NT)
    VENV = .venv/Scripts/python
else
    VENV = .venv/bin/python
endif

.PHONY: dev install build clean install-wheel help fmt lint test build-rust develop-rust clean-rust

help:
	@echo "Available targets:"
	@echo ""
	@echo "Python targets:"
	@echo "  make build          - Build wheel package (use PYTHON=python3.X to specify version)"
	@echo "  make install-wheel  - Install the built wheel"
	@echo "  make install        - Install package directly (editable mode)"
	@echo "  make dev            - Set up development environment"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make fmt            - Format code with black, autoflake, and isort"
	@echo "  make lint           - Run linting with pycodestyle"
	@echo "  make test           - Run unit tests with pytest"
	@echo "  make coverage       - Run coverage and open HTML report"
	@echo ""
	@echo "Rust targets (v0.3.0+):"
	@echo "  make build-rust     - Build Rust extension (release mode)"
	@echo "  make develop-rust   - Build and install Rust extension (dev mode, requires virtualenv)"
	@echo "  make test-rust      - Run Rust unit tests"
	@echo "  make clean-rust     - Remove Rust build artifacts"
	@echo "  make test-imports   - Test Python imports work correctly"
	@echo ""
	@echo "Example: make build PYTHON=python3.11"

dev:
	$(PYTHON) -m venv .venv
	$(VENV) -m pip install --upgrade pip
	$(VENV) -m pip install -e '.[dev]'

install:
	pip install -e .

build:
	@echo "Building wheel with $(PYTHON)..."
	$(PYTHON) -m pip install --upgrade build maturin
	$(PYTHON) -m build --wheel
	@echo ""
	@echo "✓ Wheel built successfully in dist/ directory"
	@ls -lh dist/*.whl 2>/dev/null || true

install-wheel:
	@if [ -z "$$(ls -t dist/*.whl 2>/dev/null | head -1)" ]; then \
		echo "Error: No wheel found in dist/. Run 'make build' first."; \
		exit 1; \
	fi
	@echo "Installing wheel: $$(ls -t dist/*.whl | head -1)"
	pip install --force-reinstall $$(ls -t dist/*.whl | head -1)
	@echo "✓ Wheel installed successfully"

clean:
	rm -fr dist *.egg-info .pytest_cache build htmlcov .venv

fmt:
	$(VENV) -m black zerobus examples tests
	$(VENV) -m autoflake -ri --exclude '*_pb2*.py' zerobus examples tests
	$(VENV) -m isort zerobus examples tests

lint:
	$(VENV) -m pycodestyle --exclude='*_pb2*.py' zerobus
	$(VENV) -m autoflake --check-diff --quiet --recursive --exclude '*_pb2*.py' zerobus

test:
	$(VENV) -m pytest --cov=zerobus --cov-report html --cov-report xml tests

# Rust targets (v0.3.0+)
build-rust:
	@echo "Building Rust extension (release mode)..."
	@which maturin >/dev/null 2>&1 || (echo "Error: maturin not found. Install with: pip install maturin" && exit 1)
	maturin build --release --manifest-path rust/Cargo.toml
	@echo "✓ Rust extension built successfully"
	@ls -lh target/wheels/*.whl 2>/dev/null || echo "Note: Wheel creation may require patchelf (Linux only)"

develop-rust:
	@echo "Building and installing Rust extension (development mode)..."
	@which maturin >/dev/null 2>&1 || (echo "Error: maturin not found. Install with: pip install maturin" && exit 1)
	maturin develop --manifest-path rust/Cargo.toml
	@echo "✓ Rust extension installed in development mode"

clean-rust:
	@echo "Cleaning Rust build artifacts..."
	rm -rf target/
	rm -rf rust/target/
	rm -f zerobus/_zerobus_core.so
	rm -f Cargo.lock
	@echo "✓ Rust artifacts cleaned"
