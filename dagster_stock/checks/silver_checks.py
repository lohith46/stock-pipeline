"""
Silver layer @asset_check functions.

Checks:
- silver_trades_no_duplicates     — (trade_id, symbol, timestamp) must be unique
- silver_trades_no_nulls          — critical columns must have no nulls
- silver_trades_price_bounds      — price within [0.01, 1_000_000]
- silver_trades_timestamp_order   — timestamps must be non-decreasing per symbol
- silver_quotes_spread_non_negative — spread_bps >= 0
- silver_orders_fill_status_valid  — fill_status only contains known values
"""

import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.silver.quotes import silver_quotes
from dagster_stock.assets.silver.orders import silver_orders
from dagster_stock.resources.storage_resource import StorageResource

KNOWN_FILL_STATUSES = {"filled", "partial_fill", "cancelled", "open", "unknown"}


@asset_check(asset=silver_trades, description="No duplicate (trade_id, symbol, timestamp) rows.")
def silver_trades_no_duplicates(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    dupe_count = df.duplicated(subset=["trade_id", "symbol", "timestamp"]).sum()
    return AssetCheckResult(
        passed=dupe_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": int(dupe_count)},
    )


@asset_check(asset=silver_trades, description="Critical columns contain no nulls.")
def silver_trades_no_nulls(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    null_counts = df[["trade_id", "symbol", "price", "quantity", "timestamp"]].isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())
    return AssetCheckResult(
        passed=total_nulls == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={k: int(v) for k, v in null_counts.items()},
    )


@asset_check(asset=silver_trades, description="Price within [0.01, 1_000_000].")
def silver_trades_price_bounds(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    out_of_bounds = ((df["price"] < 0.01) | (df["price"] > 1_000_000)).sum()
    return AssetCheckResult(
        passed=out_of_bounds == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"out_of_bounds_count": int(out_of_bounds)},
    )


@asset_check(asset=silver_trades, description="Timestamps non-decreasing per symbol.")
def silver_trades_timestamp_order(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    violations = (
        df.sort_values(["symbol", "timestamp"])
          .groupby("symbol")["timestamp"]
          .apply(lambda s: (s.diff() < pd.Timedelta(0)).sum())
          .sum()
    )
    return AssetCheckResult(
        passed=violations == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"out_of_order_rows": int(violations)},
    )


@asset_check(asset=silver_quotes, description="spread_bps >= 0 for all quotes.")
def silver_quotes_spread_non_negative(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="quotes")
    neg_spread = (df["spread_bps"] < 0).sum()
    return AssetCheckResult(
        passed=neg_spread == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"negative_spread_count": int(neg_spread)},
    )


@asset_check(asset=silver_orders, description="fill_status only contains known values.")
def silver_orders_fill_status_valid(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="orders")
    unknown = (~df["fill_status"].isin(KNOWN_FILL_STATUSES)).sum()
    return AssetCheckResult(
        passed=unknown == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"unknown_fill_status_count": int(unknown)},
    )


silver_checks = [
    silver_trades_no_duplicates,
    silver_trades_no_nulls,
    silver_trades_price_bounds,
    silver_trades_timestamp_order,
    silver_quotes_spread_non_negative,
    silver_orders_fill_status_valid,
]
