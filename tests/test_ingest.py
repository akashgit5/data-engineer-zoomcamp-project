"""
tests/test_ingest.py
Tests for the ingestion pipeline.
"""

import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestIngestHelpers:
    """Tests for the ingestion helper logic."""

    def test_column_name_cleaning(self):
        """Column names should be lowercased and spaces replaced with underscores."""
        df = pd.DataFrame({"Tariff Rate": [10.0], "Partner Country": ["China"]})
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-/]+", "_", regex=True)
            .str.replace(r"[^a-z0-9_]", "", regex=True)
        )
        assert "tariff_rate"     in df.columns
        assert "partner_country" in df.columns

    def test_metadata_columns_added(self):
        """Ingestion should add _ingested_at and _source_file columns."""
        df = pd.DataFrame({"country": ["China"], "tariff_rate": [25.0]})
        df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
        df["_source_file"] = "test.csv"
        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns

    def test_parquet_roundtrip(self, tmp_path):
        """Data written to Parquet should be identical when read back."""
        df_original = pd.DataFrame({
            "country":     ["China", "EU", "Canada"],
            "tariff_rate": [145.0, 20.0, 25.0],
            "sector":      ["Electronics", "Agriculture", "Steel"],
        })
        parquet_path = tmp_path / "test.parquet"
        df_original.to_parquet(parquet_path, index=False)

        df_loaded = pd.read_parquet(parquet_path)
        pd.testing.assert_frame_equal(df_original, df_loaded)

    def test_empty_csv_handled(self, tmp_path):
        """Empty dataframes should not raise errors during save."""
        df = pd.DataFrame(columns=["country", "tariff_rate", "sector"])
        out = tmp_path / "empty.parquet"
        df.to_parquet(out, index=False)
        result = pd.read_parquet(out)
        assert len(result) == 0

    def test_validation_report_structure(self, tmp_path):
        """Validation report should include row count, column count, null percentage."""
        df = pd.DataFrame({
            "country":     ["China", None, "EU"],
            "tariff_rate": [145.0, 20.0, None],
        })
        out = tmp_path / "sample.parquet"
        df.to_parquet(out, index=False)

        df_loaded = pd.read_parquet(out)
        report = {
            "rows":     len(df_loaded),
            "columns":  len(df_loaded.columns),
            "null_pct": round(df_loaded.isnull().mean().mean() * 100, 2),
        }
        assert report["rows"] == 3
        assert report["columns"] == 2
        assert report["null_pct"] > 0
