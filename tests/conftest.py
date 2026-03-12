"""
Shared pytest fixtures: sample DataFrames matching mock-stock's event schemas
and mock resources.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


# ── Sample DataFrames ─────────────────────────────────────────────────────────

@pytest.fixture
def sample_bronze_trades() -> pd.DataFrame:
    """TradeExecuted events as stored in bronze (schema-on-read)."""
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    symbols   = ["AAPL", "MSFT", "NVDA", "TSLA", "JPM"]
    return pd.DataFrame([
        {
            "event_type":        "TradeExecuted",
            "event_id":          str(uuid.uuid4()),
            "trade_id":          f"T{i:05d}",
            "symbol":            sym,
            "price":             150.0 + i * 1.5,
            "quantity":          100 + i * 10,
            "is_aggressive_buy": i % 2 == 0,
            "buyer_agent_id":    f"agent-{i:04d}",
            "seller_agent_id":   f"agent-{(i + 1):04d}",
            "buy_order_id":      f"order-{i * 2:06d}",
            "sell_order_id":     f"order-{i * 2 + 1:06d}",
            "timestamp":         (base_time + timedelta(seconds=i * 5)).isoformat(),
        }
        for i, sym in enumerate(symbols)
    ])


@pytest.fixture
def sample_bronze_orders() -> pd.DataFrame:
    """OrderPlaced + OrderCancelled events as stored in bronze."""
    base_time   = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    agent_types = ["RetailTrader", "InstitutionalInvestor", "MarketMaker", "HighFrequencyTrader"]
    order_types = ["Market", "Limit", "StopLoss", "StopLimit"]

    placed = [
        {
            "event_type":  "OrderPlaced",
            "event_id":    str(uuid.uuid4()),
            "order_id":    f"O{i:05d}",
            "symbol":      "AAPL",
            "price":       150.0 + i if i % 4 != 0 else None,  # Market orders have no price
            "quantity":    50 + i * 5,
            "side":        "Buy" if i % 2 == 0 else "Sell",
            "order_type":  order_types[i % 4],
            "agent_id":    f"agent-{i:04d}",
            "agent_type":  agent_types[i % 4],
            "timestamp":   (base_time + timedelta(seconds=i * 10)).isoformat(),
        }
        for i in range(8)
    ]

    # Two of the orders are cancelled
    cancelled = [
        {
            "event_type": "OrderCancelled",
            "event_id":   str(uuid.uuid4()),
            "order_id":   "O00001",
            "symbol":     "AAPL",
            "reason":     "price_moved",
            "timestamp":  (base_time + timedelta(seconds=20)).isoformat(),
        },
        {
            "event_type": "OrderCancelled",
            "event_id":   str(uuid.uuid4()),
            "order_id":   "O00003",
            "symbol":     "AAPL",
            "reason":     "agent_decision",
            "timestamp":  (base_time + timedelta(seconds=45)).isoformat(),
        },
    ]

    return pd.DataFrame(placed + cancelled)


@pytest.fixture
def sample_silver_trades() -> pd.DataFrame:
    """Clean silver_trades rows matching the post-transformation schema."""
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    return pd.DataFrame([
        {
            "trade_id":         f"T{i:05d}",
            "symbol":           "AAPL",
            "price":            150.0 + i,
            "quantity":         100.0,
            "side":             "buy" if i % 2 == 0 else "sell",
            "is_aggressive_buy": i % 2 == 0,
            "buyer_agent_id":   f"agent-{i:04d}",
            "seller_agent_id":  f"agent-{(i + 1):04d}",
            "timestamp":        pd.Timestamp(base_time + timedelta(seconds=i * 30)),
        }
        for i in range(20)
    ])


@pytest.fixture
def sample_gold_ohlcv() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "symbol": "AAPL", "trade_date": "2024-01-15",
            "open": 148.0, "high": 155.0, "low": 147.5, "close": 152.0,
            "volume": 500_000.0, "vwap": 151.2, "trade_count": 1_200,
            "stats_vwap": 151.5, "stats_volume": 498_000.0, "vwap_deviation_pct": 0.2,
        },
        {
            "symbol": "MSFT", "trade_date": "2024-01-15",
            "open": 370.0, "high": 378.0, "low": 368.5, "close": 375.5,
            "volume": 320_000.0, "vwap": 373.8, "trade_count": 890,
            "stats_vwap": 374.0, "stats_volume": 319_500.0, "vwap_deviation_pct": 0.05,
        },
    ])


# ── Mock resources ────────────────────────────────────────────────────────────

@pytest.fixture
def mock_kafka(sample_bronze_trades) -> MagicMock:
    kafka = MagicMock(spec=KafkaConsumerResource)
    kafka.poll.return_value = sample_bronze_trades.to_dict(orient="records")
    kafka.get_lag.return_value = 0
    return kafka


@pytest.fixture
def mock_storage(tmp_path) -> StorageResource:
    return StorageResource(backend="local", local_data_dir=str(tmp_path))


@pytest.fixture
def mock_duckdb(tmp_path) -> DuckDBResource:
    return DuckDBResource(database_path=str(tmp_path / "test.duckdb"), threads=1)
