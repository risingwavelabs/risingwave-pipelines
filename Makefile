# Makefile for RisingWave Pipelines project

.PHONY: help test test-shell test-python demo setup clean install format lint check-format fix-lint

# Default target
help:
	@echo "RisingWave Pipelines - Available targets:"
	@echo ""
	@echo "  test         - Run all tests (default: Python runner)"
	@echo "  test-python  - Run tests using Python test runner"
	@echo "  setup        - Set up virtual environment and install dependencies"
	@echo "  install      - Install/update dependencies in existing venv"
	@echo "  clean        - Clean up Python cache files"
	@echo "  test-data    - Run only data-driven tests"
	@echo "  format       - Format Python code using Ruff"
	@echo "  lint         - Lint Python code using Ruff"
	@echo "  check-format - Check Python code formatting using Ruff"
	@echo "  fix-lint     - Fix auto-fixable lint issues using Ruff"
	@echo "  help         - Show this help message"

# Run tests using Python test runner (cross-platform)
test: test-python check-format lint

test-python:
	@echo "🐍 Running tests with Python test runner..."
	@python tests/run_tests.py

# Format Python code using Ruff
format:
	@echo "🎨 Formatting Python code..."
	@if [ -d ".venv" ]; then \
		.venv/bin/ruff format .; \
	elif [ -d "venv" ]; then \
		venv/bin/ruff format .; \
	else \
		echo "❌ No virtual environment found. Run 'make setup' first."; \
		exit 1; \
	fi
	@echo "✅ Formatting complete!"

# Lint Python code using Ruff
lint:
	@echo "🔍 Linting Python code..."
	@if [ -d ".venv" ]; then \
		.venv/bin/ruff check .; \
	elif [ -d "venv" ]; then \
		venv/bin/ruff check .; \
	else \
		echo "❌ No virtual environment found. Run 'make setup' first."; \
		exit 1; \
	fi
	@echo "✅ Linting complete!"

# Check Python code formatting using Ruff
check-format:
	@echo "🔍 Checking Python code formatting..."
	@if [ -d ".venv" ]; then \
		.venv/bin/ruff format --check .; \
	elif [ -d "venv" ]; then \
		venv/bin/ruff format --check .; \
	else \
		echo "❌ No virtual environment found. Run 'make setup' first."; \
		exit 1; \
	fi
	@echo "✅ Format check complete!"

# Fix auto-fixable lint issues using Ruff
fix-lint:
	@echo "🔧 Fixing lint issues..."
	@if [ -d ".venv" ]; then \
		.venv/bin/ruff check --fix .; \
	elif [ -d "venv" ]; then \
		venv/bin/ruff check --fix .; \
	else \
		echo "❌ No virtual environment found. Run 'make setup' first."; \
		exit 1; \
	fi
	@echo "✅ Lint fixes complete!"

# Set up virtual environment and install dependencies
setup:
	@echo "🔧 Setting up development environment..."
	@if [ ! -d ".venv" ]; then \
		echo "📦 Creating virtual environment..."; \
		python3 -m venv .venv; \
	fi
	@echo "📋 Installing dependencies..."
	@.venv/bin/pip install -r requirements.txt
	@echo "✅ Setup complete!"
	@echo ""
	@echo "To activate the virtual environment:"
	@echo "  source .venv/bin/activate"

# Install/update dependencies in existing venv
install:
	@echo "📋 Installing/updating dependencies..."
	@if [ -d ".venv" ]; then \
		.venv/bin/pip install -r requirements.txt; \
	elif [ -d "venv" ]; then \
		venv/bin/pip install -r requirements.txt; \
	else \
		echo "❌ No virtual environment found. Run 'make setup' first."; \
		exit 1; \
	fi
	@echo "✅ Dependencies updated!"

# Clean up Python cache files
clean:
	@echo "🧹 Cleaning up Python cache files..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "✅ Cleanup complete!"

test-data:
	@echo "📊 Running data-driven tests only..."
	@if [ -d ".venv" ]; then \
		.venv/bin/python tests/test_data_driven.py; \
	elif [ -d "venv" ]; then \
		venv/bin/python tests/test_data_driven.py; \
	else \
		echo "❌ No virtual environment found. Run 'make setup' first."; \
		exit 1; \
	fi 