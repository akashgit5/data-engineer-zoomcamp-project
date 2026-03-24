"""
transformations/spark_transforms.py
────────────────────────────────────
PySpark transformation logic.
Called by pipeline/transform.py.

Why PySpark?
  PySpark is the Python API for Apache Spark — the industry-standard
  distributed batch processing engine. Even locally it gives us:
  - Lazy evaluation (builds an optimised execution plan)
  - SQL-like DataFrame API
  - Handles datasets much larger than RAM via spill-to-disk
"""

from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType


# ── Spark session factory ──────────────────────────────────────────────────

def get_spark(app_name: str = "USTariffAnalytics") -> SparkSession:
    """
    Creates (or retrieves) a local SparkSession.
    'local[*]' means: use all available CPU cores.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")   # small data → fewer partitions
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.nanosAsLong", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Column normaliser ──────────────────────────────────────────────────────

def _find_column(df: DataFrame, candidates: list[str]) -> str | None:
    """Return the first matching column name (case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in cols_lower:
            return cols_lower[candidate.lower()]
    return None


# ── Core transformations ───────────────────────────────────────────────────

def clean_raw(df: DataFrame) -> DataFrame:
    """
    Stage 1 — Clean the raw tariff DataFrame:
    - Standardise column names
    - Cast tariff rate column to Double
    - Drop rows with null tariff rates
    - Trim string columns
    """
    logger.info("  [clean_raw] Starting …")

    # ── Detect key columns (dataset may use slightly different names) ──────
    rate_col    = _find_column(df, ["trump_response", "tariff_rate", "rate"])
    country_col = _find_column(df, ["country"])
    sector_col  = _find_column(df, ["trump_tariffs_alleged", "sector", "category"])
    hs_col      = _find_column(df, ["hs_code", "hscode"])
    desc_col    = _find_column(df, ["description", "us_2024_deficit"])

    logger.info(
        f"  Detected columns → rate={rate_col}, country={country_col}, "
        f"sector={sector_col}, hs={hs_col}, desc={desc_col}"
    )

    # ── Rename to canonical names ──────────────────────────────────────────
    rename_map = {}
    if rate_col    and rate_col    != "tariff_rate":        rename_map[rate_col]    = "tariff_rate"
    if country_col and country_col != "country":            rename_map[country_col] = "country"
    if sector_col  and sector_col  != "sector":             rename_map[sector_col]  = "sector"
    if hs_col      and hs_col      != "hs_code":            rename_map[hs_col]      = "hs_code"
    if desc_col    and desc_col    != "description":        rename_map[desc_col]    = "description"

    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    # ── Ensure required columns exist (add nulls if missing) ──────────────
    for col_name in ["tariff_rate", "country", "sector", "hs_code", "description"]:
        if col_name not in df.columns:
            logger.warning(f"  Column '{col_name}' not found — adding as null")
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    # ── Cast tariff_rate to numeric ────────────────────────────────────────
    df = df.withColumn(
        "tariff_rate",
        F.regexp_replace(F.col("tariff_rate").cast(StringType()), "%", "").cast(DoubleType())
    )

    # ── Drop rows without a tariff rate ───────────────────────────────────
    before = df.count()
    df = df.filter(F.col("tariff_rate").isNotNull())
    after = df.count()
    logger.info(f"  Dropped {before - after:,} rows with null tariff_rate ({before:,} → {after:,})")

    # ── Trim string columns ────────────────────────────────────────────────
    for c in ["country", "sector", "description"]:
        df = df.withColumn(c, F.trim(F.col(c)))

    # ── Replace empty strings with null ───────────────────────────────────
    for c in ["country", "sector"]:
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

    # ── Fill remaining nulls ───────────────────────────────────────────────
    df = df.fillna({"country": "Unknown", "sector": "Unclassified"})

    # ── Add a year column (all data is 2025) ───────────────────────────────
    df = df.withColumn("tariff_year", F.lit(2025))

    logger.info(f"  [clean_raw] Done — {df.count():,} clean rows")
    return df


def build_country_aggregates(df: DataFrame) -> DataFrame:
    """
    Stage 2a — Aggregate tariff rates by country.
    Output: one row per country with avg/min/max/count.
    """
    logger.info("  [build_country_aggregates] Starting …")
    agg = (
        df.groupBy("country")
        .agg(
            F.round(F.avg("tariff_rate"), 2).alias("avg_tariff_rate"),
            F.round(F.min("tariff_rate"), 2).alias("min_tariff_rate"),
            F.round(F.max("tariff_rate"), 2).alias("max_tariff_rate"),
            F.count("*").alias("product_line_count"),
            F.round(F.stddev("tariff_rate"), 2).alias("stddev_tariff_rate"),
        )
        .orderBy(F.desc("avg_tariff_rate"))
    )
    logger.info(f"  [build_country_aggregates] {agg.count()} countries")
    return agg


def build_sector_aggregates(df: DataFrame) -> DataFrame:
    """
    Stage 2b — Aggregate tariff rates by product sector.
    Output: one row per sector.
    """
    logger.info("  [build_sector_aggregates] Starting …")
    agg = (
        df.groupBy("sector")
        .agg(
            F.round(F.avg("tariff_rate"), 2).alias("avg_tariff_rate"),
            F.round(F.min("tariff_rate"), 2).alias("min_tariff_rate"),
            F.round(F.max("tariff_rate"), 2).alias("max_tariff_rate"),
            F.count("*").alias("product_line_count"),
            F.countDistinct("country").alias("country_count"),
        )
        .orderBy(F.desc("avg_tariff_rate"))
    )
    logger.info(f"  [build_sector_aggregates] {agg.count()} sectors")
    return agg


def build_country_sector_matrix(df: DataFrame) -> DataFrame:
    """
    Stage 2c — Cross-aggregate: country × sector matrix.
    Used for the heatmap tile in the dashboard.
    """
    logger.info("  [build_country_sector_matrix] Starting …")
    matrix = (
        df.groupBy("country", "sector")
        .agg(
            F.round(F.avg("tariff_rate"), 2).alias("avg_tariff_rate"),
            F.count("*").alias("product_line_count"),
        )
        .orderBy("country", "sector")
    )
    logger.info(f"  [build_country_sector_matrix] {matrix.count()} rows")
    return matrix


def build_rate_buckets(df: DataFrame) -> DataFrame:
    """
    Stage 2d — Bin tariff rates into brackets for histogram analysis.
    Brackets: 0-5%, 5-10%, 10-25%, 25-50%, 50-100%, 100%+
    """
    logger.info("  [build_rate_buckets] Starting …")
    bucketed = df.withColumn(
        "rate_bucket",
        F.when(F.col("tariff_rate") < 5,   "0–5%")
         .when(F.col("tariff_rate") < 10,  "5–10%")
         .when(F.col("tariff_rate") < 25,  "10–25%")
         .when(F.col("tariff_rate") < 50,  "25–50%")
         .when(F.col("tariff_rate") < 100, "50–100%")
         .otherwise("100%+")
    )
    summary = (
        bucketed.groupBy("rate_bucket")
        .agg(
            F.count("*").alias("product_line_count"),
            F.countDistinct("country").alias("country_count"),
        )
        .orderBy("rate_bucket")
    )
    logger.info(f"  [build_rate_buckets] {summary.count()} buckets")
    return summary


def build_summary_kpis(df: DataFrame) -> DataFrame:
    """
    Stage 2e — Top-level KPI summary for dashboard header tiles.
    """
    logger.info("  [build_summary_kpis] Starting …")
    summary = df.agg(
        F.count("*").alias("total_product_lines"),
        F.countDistinct("country").alias("total_countries"),
        F.countDistinct("sector").alias("total_sectors"),
        F.round(F.avg("tariff_rate"), 2).alias("global_avg_tariff"),
        F.round(F.max("tariff_rate"), 2).alias("max_tariff_rate"),
        F.round(F.min("tariff_rate"), 2).alias("min_tariff_rate"),
    )
    return summary


# ── Run all transforms ─────────────────────────────────────────────────────

def run_all_transforms(raw_parquet_dir: Path, processed_dir: Path) -> dict[str, Path]:
    """
    Orchestrates all Spark transformations.
    Reads from raw_parquet_dir, writes to processed_dir.
    Returns dict of {table_name: output_path}.
    """
    spark = get_spark()
    output_paths = {}

    # ── Find raw parquet files ─────────────────────────────────────────────
    raw_files = list(raw_parquet_dir.glob("*.parquet"))
    if not raw_files:
        raise FileNotFoundError(f"No parquet files found in {raw_parquet_dir}")

    logger.info(f"Reading {len(raw_files)} raw parquet file(s) …")
    raw_df = spark.read.parquet(*[str(f) for f in raw_files])
    logger.info(f"Raw schema: {raw_df.dtypes}")

    # ── Stage 1: Clean ─────────────────────────────────────────────────────
    clean_df = clean_raw(raw_df)
    clean_path = processed_dir / "clean_tariffs"
    clean_df.write.mode("overwrite").parquet(str(clean_path))
    output_paths["clean_tariffs"] = clean_path
    logger.info(f"Written: {clean_path}")

    # ── Stage 2a: Country aggregates ───────────────────────────────────────
    country_agg = build_country_aggregates(clean_df)
    country_path = processed_dir / "tariffs_by_country"
    country_agg.write.mode("overwrite").parquet(str(country_path))
    output_paths["tariffs_by_country"] = country_path
    logger.info(f"Written: {country_path}")

    # ── Stage 2b: Sector aggregates ────────────────────────────────────────
    sector_agg = build_sector_aggregates(clean_df)
    sector_path = processed_dir / "tariffs_by_sector"
    sector_agg.write.mode("overwrite").parquet(str(sector_path))
    output_paths["tariffs_by_sector"] = sector_path
    logger.info(f"Written: {sector_path}")

    # ── Stage 2c: Country × Sector matrix ─────────────────────────────────
    matrix = build_country_sector_matrix(clean_df)
    matrix_path = processed_dir / "country_sector_matrix"
    matrix.write.mode("overwrite").parquet(str(matrix_path))
    output_paths["country_sector_matrix"] = matrix_path
    logger.info(f"Written: {matrix_path}")

    # ── Stage 2d: Rate buckets ─────────────────────────────────────────────
    buckets = build_rate_buckets(clean_df)
    buckets_path = processed_dir / "rate_buckets"
    buckets.write.mode("overwrite").parquet(str(buckets_path))
    output_paths["rate_buckets"] = buckets_path
    logger.info(f"Written: {buckets_path}")

    # ── Stage 2e: Summary KPIs ─────────────────────────────────────────────
    summary = build_summary_kpis(clean_df)
    summary_path = processed_dir / "summary_kpis"
    summary.write.mode("overwrite").parquet(str(summary_path))
    output_paths["summary_kpis"] = summary_path
    logger.info(f"Written: {summary_path}")

    spark.stop()
    return output_paths
