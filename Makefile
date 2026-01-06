# Fraudit Makefile
# Common commands for development and operations

.PHONY: help install dev setup init sync detect tui clean test lint

# Default target
help:
	@echo "Fraudit - Government Spending Fraud Detection"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Setup:"
	@echo "  install     Install production dependencies"
	@echo "  dev         Install development dependencies"
	@echo "  setup       Full setup (venv, deps, db init)"
	@echo "  init        Initialize database tables"
	@echo ""
	@echo "Operations:"
	@echo "  sync        Run smart data sync (skip completed)"
	@echo "  sync-full   Run full data sync"
	@echo "  detect      Run fraud detection rules"
	@echo "  tui         Start terminal UI"
	@echo ""
	@echo "Development:"
	@echo "  test        Run tests"
	@echo "  lint        Run linters"
	@echo "  clean       Remove build artifacts"

# Python virtual environment
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

$(VENV)/bin/activate:
	python -m venv $(VENV)
	$(PIP) install --upgrade pip

# Installation targets
install: $(VENV)/bin/activate
	$(PIP) install -e .

dev: $(VENV)/bin/activate
	$(PIP) install -e ".[dev]"

# Setup target
setup: install
	@echo "Creating database..."
	@createdb fraudit 2>/dev/null || echo "Database already exists"
	@echo "Initializing tables..."
	$(PYTHON) -c "from fraudit.database import init_db; init_db()"
	@echo "Setup complete! Run 'make sync' to fetch data."

init:
	$(PYTHON) -c "from fraudit.database import init_db; init_db()"

# Operation targets
sync:
	$(PYTHON) -c "from fraudit.ingestion.runner import run_sync; run_sync(smart=True)"

sync-full:
	$(PYTHON) -c "from fraudit.ingestion.runner import run_sync; run_sync(smart=False)"

detect:
	$(VENV)/bin/fraudit analyze run

tui:
	$(VENV)/bin/fraudit tui

# Development targets
test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check fraudit/
	$(PYTHON) -m black --check fraudit/

format:
	$(PYTHON) -m black fraudit/
	$(PYTHON) -m ruff check --fix fraudit/

# Cleanup
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# Quick run (for development)
run: install
	./run.sh
