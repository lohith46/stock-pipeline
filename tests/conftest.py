"""
Shared pytest fixtures: sample DataFrames and mock resources.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dagster_stock.resources.kafka_resource import KafkaConsumerResource
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


# ── Sample DataFrames ────────────────────────────────────────────────────────

@pytest.fixture
def sample_bronze_trades() -> pd.DataFrame:
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    return pd.DataFrame([
        {"trade_id": f"T{i:05d}", "symbol": sym, "price": 150.0 + i * 0.5,
         "quantity": 100 + i * 10, "side": "buy" if i % 2 == 0 else "sell",
         "agent_type": "MM", "mid_price": 150.25 + i * 0.5,
         "timestamp": (base_time + timedelta(seconds=i * 5)).isoformat()}
        for i, sym in enumerate(["AAPL", "MSFT", "GOOG", "TSLA", "AMZN"])
    ])


@pytest.fixture
def sample_bronze_orders() -> pd.DataFrame:
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    return pd.DataFrame([
        {"order_id": f"O{i:05d}", "symbol": "AAPL", "price": 150.0 + i,
         "quantity": 50 + i * 5, "side": "buy", "order_type": "limit",
         "fill_status": ["F", "PF", "O", "C"][i % 4],
         "agent_code": ["MM", "ARB", "RET", "INST"][i % 4],
         "timestamp": (base_time + timedelta(seconds=i * 10)).isoformat()}
        for i in range(8)
    ])


@pytest.fixture
def sample_silver_trades() -> pd.DataFrame:
    base_time = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    return pd.DataFrame([
        {"trade_id": f"T{i:05d}", "symbol": "AAPL", "price": 150.0 + i,
         "quantity": 100.0, "side": "buy", "agent_type": "MM",
         "mid_price": 150.25 + i,
         "timestamp": pd.Timestamp(base_time + timedelta(seconds=i * 30))}
        for i in range(20)
    ])


@pytest.fixture
def sample_gold_ohlcv() -> pd.DataFrame:
    return pd.DataFrame([
        {"symbol": "AAPL", "trade_date": "2024-01-15",
         "open": 148.0, "high": 155.0, "low": 147.5, "close": 152.0,
         "volume": 500_000.0, "vwap": 151.2, "trade_count": 1_200},
        {"symbol": "MSFT", "trade_date": "2024-01-15",
         "open": 370.0, "high": 378.0, "low": 368.5, "close": 375.5,
         "volume": 320_000.0, "vwap": 373.8, "trade_count": 890},
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
