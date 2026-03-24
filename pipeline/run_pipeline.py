"""
pipeline/run_pipeline.py
─────────────────────────
Master orchestrator: runs all three Prefect flows in sequence.

  1. ingest_flow   — Kaggle → data_lake/raw/
  2. transform_flow — PySpark → data_lake/processed/
  3. load_warehouse_flow — DuckDB warehouse

Run with:
  python pipeline/run_pipeline.py
  # or
  make pipeline
"""

import sys
import time
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.ingest         import ingest_flow
from pipeline.transform      import transform_flow
from pipeline.load_warehouse import load_warehouse_flow


def run_pipeline() -> None:
    start = time.time()

    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║      US Tariff Analytics — Full Pipeline         ║")
    logger.info("╚══════════════════════════════════════════════════╝")

    # ── Step 1: Ingest ─────────────────────────────────────────────────────
    logger.info("\n▶  STEP 1/3 — Data Ingestion (Kaggle → Data Lake)")
    t1 = time.time()
    ingest_result = ingest_flow()
    logger.info(f"   Completed in {time.time()-t1:.1f}s — {len(ingest_result['files'])} file(s)")

    # ── Step 2: Transform ──────────────────────────────────────────────────
    logger.info("\n▶  STEP 2/3 — PySpark Transformations")
    t2 = time.time()
    transform_result = transform_flow()
    logger.info(f"   Completed in {time.time()-t2:.1f}s — {len(transform_result)} table(s)")

    # ── Step 3: Load warehouse ─────────────────────────────────────────────
    logger.info("\n▶  STEP 3/3 — Load DuckDB Warehouse")
    t3 = time.time()
    warehouse_result = load_warehouse_flow()
    logger.info(f"   Completed in {time.time()-t3:.1f}s")

    elapsed = time.time() - start
    logger.info(f"\n✅ Pipeline complete in {elapsed:.1f}s")
    logger.info(f"   DuckDB warehouse: {warehouse_result['db_path']}")
    logger.info("\nNext step → launch the dashboard:")
    logger.info("   streamlit run dashboard/app.py")
    logger.info("   # or: make dashboard")


if __name__ == "__main__":
    run_pipeline()
