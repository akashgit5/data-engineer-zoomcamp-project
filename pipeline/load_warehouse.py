"""
pipeline/load_warehouse.py
───────────────────────────
Prefect flow: Reads processed Parquet files from data_lake/processed/
and loads them into the DuckDB data warehouse (warehouse/tariffs.db).

What is DuckDB?
  DuckDB is a free, embedded OLAP SQL database — think "SQLite for analytics".
  It reads Parquet files natively and supports standard SQL. You can connect
  to it from SQL Workbench J using the DuckDB JDBC driver.
"""

import sys
from pathlib import Path

import duckdb
from loguru import logger
from prefect import flow, task

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
PROCESSED_LAKE = PROJECT_ROOT / "data_lake" / "processed"
WAREHOUSE_DIR  = PROJECT_ROOT / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH        = WAREHOUSE_DIR / "tariffs.db"
SCHEMA_SQL     = WAREHOUSE_DIR / "schema.sql"

sys.path.insert(0, str(PROJECT_ROOT))


# ── Helpers ────────────────────────────────────────────────────────────────

def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


# ── Tasks ──────────────────────────────────────────────────────────────────

@task(name="create-warehouse-schema")
def create_schema() -> None:
    logger.info("Creating DuckDB schema...")
    con = get_connection()
    con.execute("DROP TABLE IF EXISTS raw_tariffs")
    con.execute("DROP TABLE IF EXISTS tariffs_by_country")
    con.execute("DROP TABLE IF EXISTS tariffs_by_sector")
    con.execute("DROP TABLE IF EXISTS country_sector_matrix")
    con.execute("DROP TABLE IF EXISTS rate_buckets")
    con.execute("DROP TABLE IF EXISTS summary_kpis")
    con.execute("""
        CREATE TABLE raw_tariffs (
            country VARCHAR, sector VARCHAR, hs_code VARCHAR,
            description VARCHAR, tariff_rate DOUBLE,
            tariff_year INTEGER, _ingested_at VARCHAR, _source_file VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE tariffs_by_country (
            country VARCHAR, avg_tariff_rate DOUBLE, min_tariff_rate DOUBLE,
            max_tariff_rate DOUBLE, product_line_count BIGINT, stddev_tariff_rate DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE tariffs_by_sector (
            sector VARCHAR, avg_tariff_rate DOUBLE, min_tariff_rate DOUBLE,
            max_tariff_rate DOUBLE, product_line_count BIGINT, country_count BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE country_sector_matrix (
            country VARCHAR, sector VARCHAR,
            avg_tariff_rate DOUBLE, product_line_count BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE rate_buckets (
            rate_bucket VARCHAR, product_line_count BIGINT, country_count BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE summary_kpis (
            total_product_lines BIGINT, total_countries BIGINT,
            total_sectors BIGINT, global_avg_tariff DOUBLE,
            max_tariff_rate DOUBLE, min_tariff_rate DOUBLE
        )
    """)
    con.close()
    logger.info("Schema created")


@task(name="load-table")
def load_table(table_name: str, parquet_subdir: str) -> int:
    """
    Load a processed Parquet dataset into a DuckDB table.
    DuckDB's read_parquet() handles directories of part files automatically.
    """
    parquet_path = PROCESSED_LAKE / parquet_subdir
    if not parquet_path.exists():
        logger.warning(f"  Skipping {table_name} — path not found: {parquet_path}")
        return 0

    # Glob all part files in the Spark output directory
    pattern = str(parquet_path / "*.parquet")

    con = get_connection()
    try:
        # Truncate first for idempotency
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{pattern}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"   {table_name}: {count:,} rows loaded")
    except Exception as e:
        logger.error(f"   Failed loading {table_name}: {e}")
        raise
    finally:
        con.close()

    return count


@task(name="verify-warehouse")
def verify_warehouse() -> dict:
    """Run quick sanity checks on the loaded warehouse."""
    con = get_connection()
    report = {}

    tables = ["raw_tariffs", "tariffs_by_country", "tariffs_by_sector",
              "country_sector_matrix", "rate_buckets", "summary_kpis"]

    for table in tables:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            report[table] = count
            logger.info(f"  {table}: {count:,} rows")
        except Exception as e:
            report[table] = f"ERROR: {e}"
            logger.error(f"   {table}: {e}")

    # Show summary KPIs
    try:
        kpis = con.execute("SELECT * FROM summary_kpis").fetchdf()
        logger.info(f"\n  Summary KPIs:\n{kpis.to_string()}")
    except Exception:
        pass

    con.close()
    return report


# ── Flow ───────────────────────────────────────────────────────────────────

@flow(name="us-tariff-load-warehouse", log_prints=True)
def load_warehouse_flow() -> dict:
    """
    Master warehouse loading flow:
      1. Create schema (tables + views)
      2. Load each processed dataset
      3. Verify row counts
    """
    logger.info("═══ Starting Warehouse Load Flow ═══")

    create_schema()

    # Map: DuckDB table → processed Parquet subdirectory name
    table_map = {
        "raw_tariffs":          "clean_tariffs",
        "tariffs_by_country":   "tariffs_by_country",
        "tariffs_by_sector":    "tariffs_by_sector",
        "country_sector_matrix":"country_sector_matrix",
        "rate_buckets":         "rate_buckets",
        "summary_kpis":         "summary_kpis",
    }

    counts = {}
    for table, parquet_dir in table_map.items():
        counts[table] = load_table(table, parquet_dir)

    report = verify_warehouse()
    logger.info("═══ Warehouse Load Flow Complete ═══")
    return {"db_path": str(DB_PATH), "row_counts": report}


if __name__ == "__main__":
    result = load_warehouse_flow()
    print("\nWarehouse load result:", result)
