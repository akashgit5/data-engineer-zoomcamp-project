"""
pipeline/ingest.py
──────────────────
Prefect flow: Downloads the US Tariffs 2025 dataset from Kaggle
and stores it as Parquet in the local data lake (data_lake/raw/).

Handles CSVs that have metadata/header rows at the top before
the actual column headers begin.
"""

from pathlib import Path

import pandas as pd
from loguru import logger
from prefect import flow, task

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_LAKE     = PROJECT_ROOT / "data_lake" / "raw"
RAW_LAKE.mkdir(parents=True, exist_ok=True)


def _smart_read_csv(csv_file: Path) -> pd.DataFrame:
    """
    Intelligently reads a CSV that may have metadata rows at the top.
    Tries standard read first, then scans for the real header row
    by looking for the line with the most comma-separated fields.
    """
    # First attempt — standard read
    try:
        df = pd.read_csv(csv_file, sep=';', low_memory=False)
        # If it parsed but has only 1 column, the header row is buried
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    # Second attempt — find the real header row by scanning lines
    logger.info(f"  CSV has metadata rows — scanning for real header...")
    with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # Count commas per line — the header row will have the most
    field_counts = [line.count(",") for line in lines]
    max_fields   = max(field_counts)

    # Find the first line that has the maximum number of commas
    header_row = next(i for i, c in enumerate(field_counts) if c == max_fields)
    logger.info(f"  Real header found at line {header_row}: {lines[header_row].strip()[:80]}")

    df = pd.read_csv(csv_file, skiprows=header_row, low_memory=False)
    return df


# ── Tasks ──────────────────────────────────────────────────────────────────

@task(name="download-kaggle-dataset", retries=2, retry_delay_seconds=10)
def download_dataset() -> Path:
    """Downloads the US Tariffs 2025 dataset via kagglehub."""
    logger.info("Downloading dataset from Kaggle...")
    import kagglehub
    path = kagglehub.dataset_download("danielcalvoglez/us-tariffs-2025")
    kaggle_path = Path(path)
    logger.info(f"Kaggle download complete -> {kaggle_path}")
    return kaggle_path


@task(name="copy-to-data-lake")
def copy_to_lake(kaggle_path: Path) -> list:
    """
    Reads CSV files from the Kaggle cache, handles metadata rows,
    and saves each as Parquet in data_lake/raw/.
    """
    csv_files = list(kaggle_path.glob("**/*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {kaggle_path}")

    written = []
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file.name}")

        # Smart read — handles files with metadata rows at top
        df = _smart_read_csv(csv_file)
        logger.info(f"  Rows: {len(df):,}  |  Columns: {list(df.columns)}")

        # Drop completely empty rows and columns
        df = df.dropna(how="all").dropna(axis=1, how="all")

        # Clean column names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-/]+", "_", regex=True)
            .str.replace(r"[^a-z0-9_]", "", regex=True)
        )
        logger.info(f"  Cleaned columns: {list(df.columns)}")

        # Add ingestion metadata
        df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
        df["_source_file"] = csv_file.name

        # Save as Parquet
        stem     = csv_file.stem.lower().replace(" ", "_")
        out_path = RAW_LAKE / f"{stem}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")
        logger.info(f"  Saved -> {out_path}")
        written.append(str(out_path))

    return written


@task(name="validate-raw-data")
def validate_raw(parquet_paths: list) -> dict:
    """Basic data quality checks on the raw lake files."""
    report = {}
    for path_str in parquet_paths:
        path = Path(path_str)
        df   = pd.read_parquet(path)
        report[path.name] = {
            "rows":         len(df),
            "columns":      len(df.columns),
            "null_pct":     round(df.isnull().mean().mean() * 100, 2),
            "column_names": list(df.columns),
        }
        logger.info(
            f"{path.name}: {report[path.name]['rows']:,} rows, "
            f"{report[path.name]['null_pct']}% nulls"
        )
    return report


@flow(name="us-tariff-ingest", log_prints=True)
def ingest_flow() -> dict:
    logger.info("Starting Ingestion Flow")
    kaggle_path   = download_dataset()
    parquet_paths = copy_to_lake(kaggle_path)
    validation    = validate_raw(parquet_paths)
    logger.info("Ingestion Flow Complete")
    return {"files": parquet_paths, "validation": validation}


if __name__ == "__main__":
    result = ingest_flow()
    print("\nIngestion result:", result)
