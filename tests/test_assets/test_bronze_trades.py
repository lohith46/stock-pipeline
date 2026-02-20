"""
Unit tests for bronze_trades asset.

Tests:
- Kafka poll produces a DataFrame with correct shape and column types
- Returns empty DataFrame when Kafka returns no messages
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from dagster import build_asset_context


def test_bronze_trades_returns_dataframe(mock_kafka, mock_storage, sample_bronze_trades):
    """bronze_trades should return a non-empty DataFrame when messages are available."""
    from dagster_stock.assets.bronze.trades import bronze_trades

    with patch.object(mock_storage, "write_parquet", return_value="/tmp/test.parquet"):
        ctx = build_asset_context(resources={"kafka": mock_kafka, "storage": mock_storage})
        result = bronze_trades(ctx, mock_kafka, mock_storage)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_bronze_trades)


def test_bronze_trades_columns(mock_kafka, mock_storage, sample_bronze_trades):
    """bronze_trades DataFrame should contain expected columns."""
    from dagster_stock.assets.bronze.trades import bronze_trades

    with patch.object(mock_storage, "write_parquet", return_value="/tmp/test.parquet"):
        ctx = build_asset_context(resources={"kafka": mock_kafka, "storage": mock_storage})
        result = bronze_trades(ctx, mock_kafka, mock_storage)

    expected_cols = {"trade_id", "symbol", "price", "quantity", "side", "timestamp"}
    assert expected_cols.issubset(set(result.columns))


def test_bronze_trades_empty_when_no_messages(mock_storage):
    """bronze_trades should return empty DataFrame when Kafka has no messages."""
    from dagster_stock.assets.bronze.trades import bronze_trades

    empty_kafka = MagicMock()
    empty_kafka.poll.return_value = []

    with patch.object(mock_storage, "write_parquet", return_value="/tmp/test.parquet"):
        ctx = build_asset_context(resources={"kafka": empty_kafka, "storage": mock_storage})
        result = bronze_trades(ctx, empty_kafka, mock_storage)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_bronze_trades_calls_kafka_with_correct_topic(mock_kafka, mock_storage):
    """bronze_trades should poll the stock.trades topic."""
    from dagster_stock.assets.bronze.trades import bronze_trades, TOPIC

    with patch.object(mock_storage, "write_parquet", return_value="/tmp/test.parquet"):
        ctx = build_asset_context(resources={"kafka": mock_kafka, "storage": mock_storage})
        bronze_trades(ctx, mock_kafka, mock_storage)

    mock_kafka.poll.assert_called_once()
    call_kwargs = mock_kafka.poll.call_args
    assert call_kwargs.kwargs.get("topic") == TOPIC or call_kwargs.args[0] == TOPIC
