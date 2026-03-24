.PHONY: pipeline dashboard test clean prefect-ui install setup

# ── Setup ──────────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

setup: install
	mkdir -p data_lake/raw data_lake/processed warehouse
	@echo "Project setup complete."

# ── Pipeline ───────────────────────────────────────────────────────────────
pipeline:
	@echo "🚀 Running full pipeline..."
	python pipeline/run_pipeline.py

ingest:
	@echo "📥 Running ingestion only..."
	python pipeline/ingest.py

transform:
	@echo "⚡ Running Spark transformation only..."
	python pipeline/transform.py

load:
	@echo "🏛️  Loading to DuckDB warehouse only..."
	python pipeline/load_warehouse.py

# ── Dashboard ──────────────────────────────────────────────────────────────
dashboard:
	@echo "📊 Launching Streamlit dashboard at http://localhost:8501"
	streamlit run dashboard/app.py

# ── Orchestration UI ───────────────────────────────────────────────────────
prefect-ui:
	@echo "🌐 Starting Prefect UI at http://localhost:4200"
	prefect server start

# ── Tests ──────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=pipeline --cov=transformations --cov-report=term-missing

# ── Cleanup ────────────────────────────────────────────────────────────────
clean:
	rm -rf data_lake/raw/* data_lake/processed/* warehouse/tariffs.db
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "🧹 Cleaned all generated data files."

clean-all: clean
	rm -rf venv .prefect
	@echo "🧹 Full clean done (including virtualenv)."
