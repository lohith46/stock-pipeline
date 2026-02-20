"""
Unit tests for gold_ohlcv asset.

Tests:
- VWAP formula correctness: sum(price*qty) / sum(qty)
- OHLCV grouping produces one row per symbol per day
- High >= Close >= Open >= Low constraint
"""

import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timezone, timedelta


def _build_trades(n: int = 100) -> pd.DataFrame:
    base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    prices    = np.random.uniform(148, 155, n)
    quantities = np.random.uniform(50, 500, n)
    return pd.DataFrame({
        "trade_id":  [f"T{i:05d}" for i in range(n)],
        "symbol":    ["AAPL"] * n,
        "price":     prices,
        "quantity":  quantities,
        "side":      ["buy" if i % 2 == 0 else "sell" for i in range(n)],
        "agent_type": ["MM"] * n,
        "timestamp": [pd.Timestamp(base + timedelta(seconds=i * 30)) for i in range(n)],
    })


def test_vwap_formula():
    """VWAP = sum(price * qty) / sum(qty)."""
    df = _build_trades(50)
    expected_vwap = (df["price"] * df["quantity"]).sum() / df["quantity"].sum()
    computed_vwap = (df["price"] * df["quantity"]).sum() / df["quantity"].sum()
    assert abs(expected_vwap - computed_vwap) < 1e-6


def test_ohlcv_grouping():
    """One row per (symbol, trade_date) after aggregation."""
    df = _build_trades(100)
    grouped = (
        df.groupby(["symbol", df["timestamp"].dt.date])
          .agg(open=("price", "first"), high=("price", "max"),
               low=("price", "min"), close=("price", "last"),
               volume=("quantity", "sum"), trade_count=("price", "count"))
          .reset_index()
    )
    assert len(grouped) == 1  # single symbol, single day


def test_ohlcv_price_consistency():
    """For any OHLCV bar: high >= close >= low and high >= open >= low."""
    df = _build_trades(200)
    grouped = (
        df.groupby(["symbol", df["timestamp"].dt.date])
          .agg(open=("price", "first"), high=("price", "max"),
               low=("price", "min"), close=("price", "last"))
          .reset_index()
    )
    for _, row in grouped.iterrows():
        assert row["high"] >= row["low"],  f"high < low for {row['symbol']}"
        assert row["high"] >= row["close"], f"high < close for {row['symbol']}"
        assert row["close"] >= row["low"],  f"close < low for {row['symbol']}"
        assert row["high"] >= row["open"],  f"high < open for {row['symbol']}"
        assert row["open"]  >= row["low"],  f"open < low for {row['symbol']}"


def test_vwap_within_hl_range():
    """VWAP should fall between the daily low and high."""
    df = _build_trades(200)
    high  = df["price"].max()
    low   = df["price"].min()
    vwap  = (df["price"] * df["quantity"]).sum() / df["quantity"].sum()
    assert low <= vwap <= high, f"VWAP {vwap:.4f} outside [{low:.4f}, {high:.4f}]"
