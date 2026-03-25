"""
Silver layer @asset_check functions.

Checks:
- silver_trades_no_duplicates      — (trade_id, symbol, timestamp) must be unique
- silver_trades_no_nulls           — critical columns must have no nulls
- silver_trades_price_bounds       — price within [0.01, 1_000_000]
- silver_trades_timestamp_order    — timestamps non-decreasing per symbol
- silver_trades_side_valid         — side in {"buy", "sell"}
- silver_quotes_spread_non_negative — spread_bps >= 0
- silver_orders_fill_status_valid  — fill_status only contains known values
- silver_orders_agent_type_valid   — agent_type only contains known values
"""

import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.silver.quotes import silver_quotes
from dagster_stock.assets.silver.orders import silver_orders
from dagster_stock.resources.storage_resource import StorageResource

KNOWN_FILL_STATUSES = {"open", "cancelled"}
KNOWN_AGENT_TYPES   = {"retail", "institutional", "market_maker", "hft", "unknown"}
KNOWN_SIDES         = {"buy", "sell"}


# ── silver_trades checks ──────────────────────────────────────────────────────

@asset_check(asset=silver_trades, description="No duplicate (trade_id, symbol, timestamp) rows.")
def silver_trades_no_duplicates(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    dupe_count = df.duplicated(subset=["trade_id", "symbol", "timestamp"]).sum()
    return AssetCheckResult(
        passed=bool(dupe_count == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": int(dupe_count)},
    )


@asset_check(asset=silver_trades, description="Critical columns contain no nulls.")
def silver_trades_no_nulls(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    null_counts = df[["trade_id", "symbol", "price", "quantity", "timestamp"]].isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())
    return AssetCheckResult(
        passed=bool(total_nulls == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={k: int(v) for k, v in null_counts.items()},
    )


@asset_check(asset=silver_trades, description="Price within [0.01, 1_000_000].")
def silver_trades_price_bounds(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    out_of_bounds = ((df["price"] < 0.01) | (df["price"] > 1_000_000)).sum()
    return AssetCheckResult(
        passed=bool(out_of_bounds == 0),
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
        passed=bool(violations == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"out_of_order_rows": int(violations)},
    )


@asset_check(asset=silver_trades, description="side column only contains 'buy' or 'sell'.")
def silver_trades_side_valid(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    if "side" not in df.columns:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR,
                                metadata={"reason": "side column missing"})
    invalid = (~df["side"].isin(KNOWN_SIDES)).sum()
    return AssetCheckResult(
        passed=bool(invalid == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"invalid_side_count": int(invalid)},
    )


# ── silver_quotes checks ──────────────────────────────────────────────────────

@asset_check(asset=silver_quotes, description="spread_bps >= 0 for all quotes.")
def silver_quotes_spread_non_negative(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="quotes")
    neg_spread = (df["spread_bps"] < 0).sum()
    return AssetCheckResult(
        passed=bool(neg_spread == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"negative_spread_count": int(neg_spread)},
    )


# ── silver_orders checks ──────────────────────────────────────────────────────

@asset_check(asset=silver_orders, description="fill_status only contains known values.")
def silver_orders_fill_status_valid(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="orders")
    unknown = (~df["fill_status"].isin(KNOWN_FILL_STATUSES)).sum()
    return AssetCheckResult(
        passed=bool(unknown == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"unknown_fill_status_count": int(unknown)},
    )


@asset_check(asset=silver_orders, description="agent_type only contains known values.")
def silver_orders_agent_type_valid(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="orders")
    unknown = (~df["agent_type"].isin(KNOWN_AGENT_TYPES)).sum()
    return AssetCheckResult(
        passed=bool(unknown == 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"unknown_agent_type_count": int(unknown)},
    )


silver_checks = [
    silver_trades_no_duplicates,
    silver_trades_no_nulls,
    silver_trades_price_bounds,
    silver_trades_timestamp_order,
    silver_trades_side_valid,
    silver_quotes_spread_non_negative,
    silver_orders_fill_status_valid,
    silver_orders_agent_type_valid,
]
