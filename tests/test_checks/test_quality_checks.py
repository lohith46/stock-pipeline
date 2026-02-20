"""
Unit tests for @asset_check functions.

Tests verify that each check correctly passes on clean data
and correctly fails on deliberately corrupted data.
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from dagster import build_asset_context


# ── Silver checks ─────────────────────────────────────────────────────────────

class TestSilverTradesNoDuplicates:
    def test_passes_on_unique_data(self, mock_storage, sample_silver_trades):
        from dagster_stock.checks.silver_checks import silver_trades_no_duplicates
        with patch.object(mock_storage, "read_parquet", return_value=sample_silver_trades):
            result = silver_trades_no_duplicates(mock_storage)
        assert result.passed

    def test_fails_on_duplicate_data(self, mock_storage, sample_silver_trades):
        from dagster_stock.checks.silver_checks import silver_trades_no_duplicates
        duped = pd.concat([sample_silver_trades, sample_silver_trades.iloc[:3]], ignore_index=True)
        with patch.object(mock_storage, "read_parquet", return_value=duped):
            result = silver_trades_no_duplicates(mock_storage)
        assert not result.passed


class TestSilverTradesNoNulls:
    def test_passes_on_clean_data(self, mock_storage, sample_silver_trades):
        from dagster_stock.checks.silver_checks import silver_trades_no_nulls
        with patch.object(mock_storage, "read_parquet", return_value=sample_silver_trades):
            result = silver_trades_no_nulls(mock_storage)
        assert result.passed

    def test_fails_when_price_is_null(self, mock_storage, sample_silver_trades):
        from dagster_stock.checks.silver_checks import silver_trades_no_nulls
        df = sample_silver_trades.copy()
        df.loc[0, "price"] = None
        with patch.object(mock_storage, "read_parquet", return_value=df):
            result = silver_trades_no_nulls(mock_storage)
        assert not result.passed


class TestSilverTradesPriceBounds:
    def test_passes_with_normal_prices(self, mock_storage, sample_silver_trades):
        from dagster_stock.checks.silver_checks import silver_trades_price_bounds
        with patch.object(mock_storage, "read_parquet", return_value=sample_silver_trades):
            result = silver_trades_price_bounds(mock_storage)
        assert result.passed

    def test_fails_with_extreme_price(self, mock_storage, sample_silver_trades):
        from dagster_stock.checks.silver_checks import silver_trades_price_bounds
        df = sample_silver_trades.copy()
        df.loc[0, "price"] = 2_000_000.0
        with patch.object(mock_storage, "read_parquet", return_value=df):
            result = silver_trades_price_bounds(mock_storage)
        assert not result.passed


# ── Gold checks ───────────────────────────────────────────────────────────────

class TestGoldOhlcvPriceConsistency:
    def test_passes_on_valid_ohlcv(self, mock_storage, sample_gold_ohlcv):
        from dagster_stock.checks.gold_checks import gold_ohlcv_price_consistency
        with patch.object(mock_storage, "read_parquet", return_value=sample_gold_ohlcv):
            result = gold_ohlcv_price_consistency(mock_storage)
        assert result.passed

    def test_fails_when_high_lt_low(self, mock_storage, sample_gold_ohlcv):
        from dagster_stock.checks.gold_checks import gold_ohlcv_price_consistency
        df = sample_gold_ohlcv.copy()
        df.loc[0, "high"] = df.loc[0, "low"] - 1  # violate high >= low
        with patch.object(mock_storage, "read_parquet", return_value=df):
            result = gold_ohlcv_price_consistency(mock_storage)
        assert not result.passed


class TestGoldAgentPnlFillRate:
    def test_passes_with_valid_fill_rates(self, mock_storage):
        from dagster_stock.checks.gold_checks import gold_agent_pnl_fill_rate_range
        df = pd.DataFrame({"fill_rate_pct": [85.0, 72.5, 100.0, 0.0]})
        with patch.object(mock_storage, "read_parquet", return_value=df):
            result = gold_agent_pnl_fill_rate_range(mock_storage)
        assert result.passed

    def test_fails_with_out_of_range_fill_rate(self, mock_storage):
        from dagster_stock.checks.gold_checks import gold_agent_pnl_fill_rate_range
        df = pd.DataFrame({"fill_rate_pct": [85.0, 110.0]})  # 110 is invalid
        with patch.object(mock_storage, "read_parquet", return_value=df):
            result = gold_agent_pnl_fill_rate_range(mock_storage)
        assert not result.passed
