# Default Python version (can be overridden: make build PYTHON=python3.11)
PYTHON ?= python3.9

.PHONY: dev install build clean install-wheel help

help:
	@echo "Available targets:"
	@echo "  make build          - Build wheel package (use PYTHON=python3.X to specify version)"
	@echo "  make install-wheel  - Install the built wheel"
	@echo "  make install        - Install package directly (editable mode)"
	@echo "  make dev            - Set up development environment"
	@echo "  make clean          - Remove build artifacts"
	@echo ""
	@echo "Example: make build PYTHON=python3.11"

dev:
	$(PYTHON) -m venv .venv
ifeq ($(OS), Windows_NT)
	.venv\Scripts\activate
else
	. .venv/bin/activate
endif
	pip install -e '.[dev]'

install:
	pip install -e .

build:
	@echo "Building wheel with $(PYTHON)..."
	$(PYTHON) -m pip install --upgrade build
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
