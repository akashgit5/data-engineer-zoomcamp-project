-- warehouse/schema.sql
-- ─────────────────────────────────────────────────────────────────────────
-- DuckDB schema for the US Tariff Analytics Data Warehouse
--
-- DuckDB is a free, embedded analytical database (like SQLite but for OLAP).
-- It reads Parquet files natively and stores data in a single .db file.
-- You can connect to it from SQL Workbench J using the DuckDB JDBC driver.
--
-- Performance design:
--   DuckDB uses columnar storage internally, so column-level filtering is
--   always efficient. For further optimisation we use:
--     • Indexes on high-cardinality join keys (country, sector)
--     • Persistent views for common aggregation patterns
-- ─────────────────────────────────────────────────────────────────────────

-- Drop & recreate for idempotency
DROP TABLE IF EXISTS raw_tariffs;
DROP TABLE IF EXISTS tariffs_by_country;
DROP TABLE IF EXISTS tariffs_by_sector;
DROP TABLE IF EXISTS country_sector_matrix;
DROP TABLE IF EXISTS rate_buckets;
DROP TABLE IF EXISTS summary_kpis;

-- ── 1. Clean / base fact table ────────────────────────────────────────────
CREATE TABLE raw_tariffs (
    country              VARCHAR,
    sector               VARCHAR,
    hs_code              VARCHAR,
    description          VARCHAR,
    tariff_rate          DOUBLE,
    tariff_year          INTEGER,
    _ingested_at         VARCHAR,
    _source_file         VARCHAR
);

-- ── 2. Country-level aggregates ───────────────────────────────────────────
CREATE TABLE tariffs_by_country (
    country              VARCHAR PRIMARY KEY,
    avg_tariff_rate      DOUBLE,
    min_tariff_rate      DOUBLE,
    max_tariff_rate      DOUBLE,
    product_line_count   BIGINT,
    stddev_tariff_rate   DOUBLE
);

-- ── 3. Sector-level aggregates ────────────────────────────────────────────
CREATE TABLE tariffs_by_sector (
    sector               VARCHAR PRIMARY KEY,
    avg_tariff_rate      DOUBLE,
    min_tariff_rate      DOUBLE,
    max_tariff_rate      DOUBLE,
    product_line_count   BIGINT,
    country_count        BIGINT
);

-- ── 4. Country × Sector matrix ────────────────────────────────────────────
CREATE TABLE country_sector_matrix (
    country              VARCHAR,
    sector               VARCHAR,
    avg_tariff_rate      DOUBLE,
    product_line_count   BIGINT,
    PRIMARY KEY (country, sector)
);

-- ── 5. Tariff rate distribution buckets ──────────────────────────────────
CREATE TABLE rate_buckets (
    rate_bucket          VARCHAR PRIMARY KEY,
    product_line_count   BIGINT,
    country_count        BIGINT
);

-- ── 6. Summary KPIs ───────────────────────────────────────────────────────
CREATE TABLE summary_kpis (
    total_product_lines  BIGINT,
    total_countries      BIGINT,
    total_sectors        BIGINT,
    global_avg_tariff    DOUBLE,
    max_tariff_rate      DOUBLE,
    min_tariff_rate      DOUBLE
);

-- ── Convenience views ─────────────────────────────────────────────────────

-- Top 20 most-tariffed countries
CREATE OR REPLACE VIEW v_top_countries AS
SELECT country, avg_tariff_rate, product_line_count
FROM tariffs_by_country
ORDER BY avg_tariff_rate DESC
LIMIT 20;

-- Sectors ranked by average tariff
CREATE OR REPLACE VIEW v_top_sectors AS
SELECT sector, avg_tariff_rate, product_line_count, country_count
FROM tariffs_by_sector
ORDER BY avg_tariff_rate DESC;

-- High-tariff product lines (>= 25%)
CREATE OR REPLACE VIEW v_high_tariff_products AS
SELECT country, sector, hs_code, description, tariff_rate
FROM raw_tariffs
WHERE tariff_rate >= 25
ORDER BY tariff_rate DESC;
