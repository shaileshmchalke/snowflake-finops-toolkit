# =============================================================================
# Snowflake FinOps Toolkit — Makefile
# Author: Shailesh Chalke — Senior Snowflake Consultant
# Usage: make <target>
# =============================================================================

.PHONY: help install setup-env setup-sample-data run test lint format clean

# Default: show help
help:
	@echo "======================================================"
	@echo "  Snowflake FinOps Toolkit — Available Commands"
	@echo "======================================================"
	@echo ""
	@echo "  make install          Install Python dependencies"
	@echo "  make setup-env        Copy .env.example to .env"
	@echo "  make setup-sample-data  Generate and upload sample data"
	@echo "  make run              Launch Streamlit dashboard"
	@echo "  make test             Run pytest test suite"
	@echo "  make test-coverage    Run tests with coverage report"
	@echo "  make lint             Run flake8 linter"
	@echo "  make format           Auto-format code with black + isort"
	@echo "  make clean            Remove cache and build files"
	@echo ""

# ── Install ───────────────────────────────────────────────────────────────
install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed."

# ── Environment Setup ─────────────────────────────────────────────────────
setup-env:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ .env file created from .env.example"; \
		echo "⚠️  Edit .env and add your Snowflake credentials"; \
	else \
		echo "⚠️  .env already exists — not overwriting"; \
	fi

# ── Sample Data ───────────────────────────────────────────────────────────
setup-sample-data:
	@echo "Generating and uploading sample data to Snowflake..."
	python src/generate_sample_data.py
	@echo "✅ Sample data uploaded to FINOPS_DEMO.FINOPS_SAMPLE"

# ── Dashboard ─────────────────────────────────────────────────────────────
run:
	@echo "Starting Snowflake FinOps Toolkit dashboard..."
	streamlit run app/streamlit_app.py \
		--server.port 8501 \
		--server.address 0.0.0.0 \
		--theme.base dark \
		--theme.primaryColor "#56ccf2"

# ── Testing ───────────────────────────────────────────────────────────────
test:
	@echo "Running test suite..."
	pytest tests/ -v --tb=short
	@echo "✅ Tests complete."

test-coverage:
	@echo "Running tests with coverage..."
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing
	@echo "✅ Coverage report generated at htmlcov/index.html"

# ── Code Quality ──────────────────────────────────────────────────────────
lint:
	@echo "Running flake8..."
	flake8 src/ app/ tests/ --max-line-length=100 --ignore=E501,W503
	@echo "✅ Lint complete."

format:
	@echo "Formatting with black and isort..."
	black src/ app/ tests/ --line-length=100
	isort src/ app/ tests/ --profile=black
	@echo "✅ Formatting complete."

# ── Cleanup ───────────────────────────────────────────────────────────────
clean:
	@echo "Cleaning cache files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	@echo "✅ Clean complete."

# ── Full Setup (first time) ───────────────────────────────────────────────
setup-all: install setup-env
	@echo ""
	@echo "======================================================"
	@echo "  Setup complete! Next steps:"
	@echo "  1. Edit .env with your Snowflake credentials"
	@echo "  2. Run: make setup-sample-data"
	@echo "  3. Run: make run"
	@echo "======================================================"