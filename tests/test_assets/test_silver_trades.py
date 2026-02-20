"""
Unit tests for silver_trades asset.

Tests:
- Deduplication removes duplicate rows
- Rows with null price or quantity are rejected
- Rows with price <= 0 are rejected
- Type casting produces correct dtypes
- Derived timestamp is UTC-aware
"""

import pandas as pd
import pytest
from unittest.mock import patch

from dagster import build_asset_context


def _run_silver(bronze_df: pd.DataFrame, tmp_storage):
    from dagster_stock.assets.silver.trades import silver_trades

    with patch.object(tmp_storage, "read_parquet", return_value=bronze_df), \
         patch.object(tmp_storage, "write_parquet", return_value="/tmp/silver.parquet"):
        ctx = build_asset_context(resources={"storage": tmp_storage})
        return silver_trades(ctx, tmp_storage)


def test_silver_trades_dedup(mock_storage, sample_bronze_trades):
    """Duplicate (trade_id, symbol, timestamp) rows should be removed."""
    df_with_dupes = pd.concat([sample_bronze_trades, sample_bronze_trades.iloc[:2]], ignore_index=True)
    result = _run_silver(df_with_dupes, mock_storage)
    assert len(result) == len(sample_bronze_trades)


def test_silver_trades_rejects_null_price(mock_storage, sample_bronze_trades):
    """Rows with null price should be excluded from the output."""
    df = sample_bronze_trades.copy()
    df.loc[0, "price"] = None
    result = _run_silver(df, mock_storage)
    assert len(result) == len(sample_bronze_trades) - 1


def test_silver_trades_rejects_zero_price(mock_storage, sample_bronze_trades):
    """Rows with price <= 0 should be rejected."""
    df = sample_bronze_trades.copy()
    df.loc[1, "price"] = 0
    result = _run_silver(df, mock_storage)
    assert len(result) == len(sample_bronze_trades) - 1


def test_silver_trades_price_dtype(mock_storage, sample_bronze_trades):
    """price column should be float64 after cleaning."""
    df = sample_bronze_trades.copy()
    df["price"] = df["price"].astype(str)  # simulate string input
    result = _run_silver(df, mock_storage)
    assert result["price"].dtype == float


def test_silver_trades_timestamp_utc(mock_storage, sample_bronze_trades):
    """timestamp column should be UTC-aware datetime."""
    result = _run_silver(sample_bronze_trades, mock_storage)
    assert hasattr(result["timestamp"].dtype, "tz") or result["timestamp"].dt.tz is not None


def test_silver_trades_empty_input(mock_storage):
    """Empty bronze input should produce empty silver output."""
    result = _run_silver(pd.DataFrame(), mock_storage)
    assert result.empty
