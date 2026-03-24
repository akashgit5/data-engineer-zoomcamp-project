"""
tests/test_transforms.py
Tests for the PySpark transformation logic.
"""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    """Create a minimal local SparkSession for testing."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("TestTransforms")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    """A small DataFrame that mimics the cleaned tariff dataset."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("country",     StringType(), True),
        StructField("sector",      StringType(), True),
        StructField("hs_code",     StringType(), True),
        StructField("description", StringType(), True),
        StructField("tariff_rate", DoubleType(), True),
        StructField("tariff_year", IntegerType(), True),
    ])
    data = [
        ("China",   "Electronics",  "8471.30", "Laptops",            145.0, 2025),
        ("China",   "Steel",        "7208.10", "Flat-rolled steel",  25.0,  2025),
        ("EU",      "Agriculture",  "0201.10", "Beef",               20.0,  2025),
        ("EU",      "Electronics",  "8517.12", "Mobile phones",      0.0,   2025),
        ("Canada",  "Steel",        "7208.20", "Steel coils",        25.0,  2025),
        ("Canada",  "Agriculture",  "0901.11", "Coffee",             0.0,   2025),
        ("Mexico",  "Automotive",   "8703.10", "Passenger vehicles", 25.0,  2025),
        ("Vietnam", "Electronics",  "8528.72", "Monitors",           46.0,  2025),
    ]
    return spark.createDataFrame(data, schema)


# ── Tests ──────────────────────────────────────────────────────────────────

class TestCleanRaw:
    def test_no_null_tariff_rates_remain(self, spark):
        """clean_raw() must drop rows with null tariff_rate."""
        from pyspark.sql import functions as F
        from transformations.spark_transforms import clean_raw

        data = [
            ("China", "Electronics", "8471", "Laptops", None, 2025),
            ("EU",    "Agriculture", "0201", "Beef",    20.0, 2025),
        ]
        df = spark.createDataFrame(
            data,
            ["country", "sector", "hs_code", "description", "tariff_rate", "tariff_year"]
        )
        result = clean_raw(df)
        null_count = result.filter(F.col("tariff_rate").isNull()).count()
        assert null_count == 0

    def test_tariff_year_column_added(self, spark):
        """clean_raw() must add a tariff_year column equal to 2025."""
        from transformations.spark_transforms import clean_raw

        data = [("China", "Steel", "7208", "Steel", 25.0, 2025)]
        df = spark.createDataFrame(
            data,
            ["country", "sector", "hs_code", "description", "tariff_rate", "tariff_year"]
        )
        result = clean_raw(df)
        assert "tariff_year" in result.columns
        years = [r["tariff_year"] for r in result.collect()]
        assert all(y == 2025 for y in years)

    def test_empty_country_filled(self, spark):
        """Empty country strings should be replaced with 'Unknown'."""
        from transformations.spark_transforms import clean_raw

        data = [("", "Steel", "7208", "Steel", 25.0, 2025)]
        df = spark.createDataFrame(
            data,
            ["country", "sector", "hs_code", "description", "tariff_rate", "tariff_year"]
        )
        result = clean_raw(df)
        country = result.collect()[0]["country"]
        assert country == "Unknown"


class TestCountryAggregates:
    def test_one_row_per_country(self, sample_df):
        """build_country_aggregates() must produce exactly one row per country."""
        from transformations.spark_transforms import build_country_aggregates

        result = build_country_aggregates(sample_df)
        countries = result.select("country").distinct().count()
        total_rows = result.count()
        assert countries == total_rows

    def test_avg_rate_within_bounds(self, sample_df):
        """Average tariff rates must be between 0 and 200."""
        from transformations.spark_transforms import build_country_aggregates
        from pyspark.sql import functions as F

        result = build_country_aggregates(sample_df)
        invalid = result.filter(
            (F.col("avg_tariff_rate") < 0) | (F.col("avg_tariff_rate") > 200)
        ).count()
        assert invalid == 0

    def test_china_highest_rate(self, sample_df):
        """China should have the highest average tariff rate in our test data."""
        from transformations.spark_transforms import build_country_aggregates

        result = build_country_aggregates(sample_df)
        top_country = result.orderBy("avg_tariff_rate", ascending=False).first()["country"]
        assert top_country == "China"


class TestSectorAggregates:
    def test_one_row_per_sector(self, sample_df):
        """build_sector_aggregates() must produce exactly one row per sector."""
        from transformations.spark_transforms import build_sector_aggregates

        result = build_sector_aggregates(sample_df)
        sectors  = result.select("sector").distinct().count()
        total    = result.count()
        assert sectors == total

    def test_country_count_positive(self, sample_df):
        """Every sector should have at least one country."""
        from transformations.spark_transforms import build_sector_aggregates
        from pyspark.sql import functions as F

        result = build_sector_aggregates(sample_df)
        zero_countries = result.filter(F.col("country_count") < 1).count()
        assert zero_countries == 0


class TestRateBuckets:
    def test_all_rows_bucketed(self, sample_df):
        """Total product lines across all buckets must equal input row count."""
        from transformations.spark_transforms import build_rate_buckets

        result = build_rate_buckets(sample_df)
        total_bucketed = result.agg({"product_line_count": "sum"}).collect()[0][0]
        assert total_bucketed == sample_df.count()

    def test_bucket_labels_valid(self, sample_df):
        """Bucket labels should be from the expected set."""
        from transformations.spark_transforms import build_rate_buckets

        result   = build_rate_buckets(sample_df)
        buckets  = {r["rate_bucket"] for r in result.collect()}
        valid    = {"0–5%", "5–10%", "10–25%", "25–50%", "50–100%", "100%+"}
        assert buckets.issubset(valid)
