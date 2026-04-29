# Snowflake FinOps Toolkit - Makefile
# Usage: make <target>

.PHONY: install setup data run test lint clean help

help:
	@echo ""
	@echo "Snowflake FinOps Toolkit — Available Commands"
	@echo "=============================================="
	@echo "  make install   Install Python dependencies"
	@echo "  make setup     Copy .env.example to .env (fill credentials after)"
	@echo "  make data      Generate and upload sample data to Snowflake"
	@echo "  make run       Launch the Streamlit dashboard"
	@echo "  make test      Run all unit tests with coverage"
	@echo "  make lint      Run flake8 code linter"
	@echo "  make clean     Remove __pycache__ and .pytest_cache"
	@echo ""

install:
	pip install -r requirements.txt

setup:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env created. Fill in your Snowflake credentials before running make data."; \
	else \
		echo ".env already exists."; \
	fi

data:
	python src/generate_sample_data.py

run:
	streamlit run app/streamlit_app.py

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	flake8 src/ app/ tests/ --max-line-length=110 --ignore=E501,W503

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "Cleaned."