"""
tests/test_warehouse.py
Tests for DuckDB warehouse loading and schema.
"""

import sys
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary DuckDB database for testing."""
    db_path = tmp_path / "test_tariffs.db"
    return db_path


@pytest.fixture
def sample_country_parquet(tmp_path):
    """Write a sample country aggregates parquet file."""
    df = pd.DataFrame({
        "country":            ["China",  "EU",    "Canada"],
        "avg_tariff_rate":    [85.0,     10.0,    12.5],
        "min_tariff_rate":    [25.0,     0.0,     0.0],
        "max_tariff_rate":    [145.0,    20.0,    25.0],
        "product_line_count": [100,      150,     80],
        "stddev_tariff_rate": [30.0,     8.0,     10.0],
    })
    out = tmp_path / "tariffs_by_country.parquet"
    df.to_parquet(out, index=False)
    return out


class TestDuckDBConnection:
    def test_can_create_and_connect(self, tmp_db):
        """Should be able to create a new DuckDB file and connect."""
        con = duckdb.connect(str(tmp_db))
        result = con.execute("SELECT 42 AS answer").fetchone()
        assert result[0] == 42
        con.close()

    def test_table_creation(self, tmp_db):
        """Should be able to create tables without error."""
        con = duckdb.connect(str(tmp_db))
        con.execute("""
            CREATE TABLE tariffs_by_country (
                country           VARCHAR PRIMARY KEY,
                avg_tariff_rate   DOUBLE,
                product_line_count BIGINT
            )
        """)
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "tariffs_by_country" in table_names
        con.close()

    def test_parquet_read(self, tmp_db, sample_country_parquet):
        """DuckDB should be able to read Parquet files directly."""
        con = duckdb.connect(str(tmp_db))
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{sample_country_parquet}')"
        ).fetchone()[0]
        assert count == 3
        con.close()

    def test_insert_from_parquet(self, tmp_db, sample_country_parquet):
        """Should be able to insert data from Parquet into a table."""
        con = duckdb.connect(str(tmp_db))
        con.execute("""
            CREATE TABLE tariffs_by_country (
                country            VARCHAR,
                avg_tariff_rate    DOUBLE,
                min_tariff_rate    DOUBLE,
                max_tariff_rate    DOUBLE,
                product_line_count BIGINT,
                stddev_tariff_rate DOUBLE
            )
        """)
        con.execute(
            f"INSERT INTO tariffs_by_country SELECT * FROM read_parquet('{sample_country_parquet}')"
        )
        count = con.execute("SELECT COUNT(*) FROM tariffs_by_country").fetchone()[0]
        assert count == 3
        con.close()

    def test_aggregation_query(self, tmp_db, sample_country_parquet):
        """Should support SQL aggregations correctly."""
        con = duckdb.connect(str(tmp_db))
        con.execute("""
            CREATE TABLE tariffs_by_country (
                country            VARCHAR,
                avg_tariff_rate    DOUBLE,
                min_tariff_rate    DOUBLE,
                max_tariff_rate    DOUBLE,
                product_line_count BIGINT,
                stddev_tariff_rate DOUBLE
            )
        """)
        con.execute(
            f"INSERT INTO tariffs_by_country SELECT * FROM read_parquet('{sample_country_parquet}')"
        )
        global_avg = con.execute(
            "SELECT AVG(avg_tariff_rate) FROM tariffs_by_country"
        ).fetchone()[0]
        expected = (85.0 + 10.0 + 12.5) / 3
        assert abs(global_avg - expected) < 0.01
        con.close()

    def test_view_creation(self, tmp_db, sample_country_parquet):
        """Should be able to create views over tables."""
        con = duckdb.connect(str(tmp_db))
        con.execute("""
            CREATE TABLE tariffs_by_country (
                country            VARCHAR,
                avg_tariff_rate    DOUBLE,
                min_tariff_rate    DOUBLE,
                max_tariff_rate    DOUBLE,
                product_line_count BIGINT,
                stddev_tariff_rate DOUBLE
            )
        """)
        con.execute(
            f"INSERT INTO tariffs_by_country SELECT * FROM read_parquet('{sample_country_parquet}')"
        )
        con.execute("""
            CREATE VIEW v_top_countries AS
            SELECT country, avg_tariff_rate
            FROM tariffs_by_country
            ORDER BY avg_tariff_rate DESC
            LIMIT 10
        """)
        top = con.execute("SELECT country FROM v_top_countries LIMIT 1").fetchone()[0]
        assert top == "China"
        con.close()

    def test_idempotent_reload(self, tmp_db, sample_country_parquet):
        """DELETE + re-INSERT should give the same row count (idempotency)."""
        con = duckdb.connect(str(tmp_db))
        con.execute("""
            CREATE TABLE tariffs_by_country (
                country            VARCHAR,
                avg_tariff_rate    DOUBLE,
                min_tariff_rate    DOUBLE,
                max_tariff_rate    DOUBLE,
                product_line_count BIGINT,
                stddev_tariff_rate DOUBLE
            )
        """)
        for _ in range(2):
            con.execute("DELETE FROM tariffs_by_country")
            con.execute(
                f"INSERT INTO tariffs_by_country SELECT * FROM read_parquet('{sample_country_parquet}')"
            )

        count = con.execute("SELECT COUNT(*) FROM tariffs_by_country").fetchone()[0]
        assert count == 3
        con.close()
