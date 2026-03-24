"""
pipeline/transform.py
──────────────────────
Prefect flow: Reads raw Parquet from data_lake/raw/,
runs PySpark transformations, writes processed Parquet
to data_lake/processed/.
"""

import sys
from pathlib import Path

from loguru import logger
from prefect import flow, task

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
RAW_LAKE       = PROJECT_ROOT / "data_lake" / "raw"
PROCESSED_LAKE = PROJECT_ROOT / "data_lake" / "processed"
PROCESSED_LAKE.mkdir(parents=True, exist_ok=True)

# Make transformations module importable
sys.path.insert(0, str(PROJECT_ROOT))
from transformations.spark_transforms import run_all_transforms


@task(name="run-spark-transforms", retries=1)
def spark_transform_task() -> dict:
    logger.info("⚡ Starting PySpark transformations …")
    output_paths = run_all_transforms(RAW_LAKE, PROCESSED_LAKE)
    logger.info(f"✅ Produced {len(output_paths)} processed datasets")
    return {k: str(v) for k, v in output_paths.items()}


@flow(name="us-tariff-transform", log_prints=True)
def transform_flow() -> dict:
    logger.info("═══ Starting Transform Flow ═══")
    result = spark_transform_task()
    logger.info("═══ Transform Flow Complete ═══")
    return result


if __name__ == "__main__":
    result = transform_flow()
    print("\nTransformation result:", result)
