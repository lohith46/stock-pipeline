"""
Gold layer @asset_check functions.

Checks:
- gold_ohlcv_vwap_deviation       — VWAP within ±20% of (high + low) / 2
- gold_ohlcv_price_consistency    — high >= close >= open >= low
- gold_market_quality_freshness   — data must be <= 25 hours old
- gold_agent_pnl_fill_rate_range  — fill_rate_pct in [0, 100]
"""

import pandas as pd
from datetime import datetime, timezone, timedelta
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

from dagster_stock.assets.gold.ohlcv import gold_ohlcv
from dagster_stock.assets.gold.market_quality import gold_market_quality
from dagster_stock.assets.gold.agent_pnl import gold_agent_pnl
from dagster_stock.resources.storage_resource import StorageResource

FRESHNESS_SLA_HOURS = 25


@asset_check(asset=gold_ohlcv, description="VWAP within ±20% of HL midpoint.")
def gold_ohlcv_vwap_deviation(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="ohlcv")
    df["hl_mid"] = (df["high"] + df["low"]) / 2
    df["vwap_deviation_pct"] = ((df["vwap"] - df["hl_mid"]) / df["hl_mid"] * 100).abs()
    violations = (df["vwap_deviation_pct"] > 20).sum()
    return AssetCheckResult(
        passed=violations == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "violations": int(violations),
            "max_deviation_pct": float(df["vwap_deviation_pct"].max()),
        },
    )


@asset_check(asset=gold_ohlcv, description="OHLC price consistency: high >= close >= open >= low.")
def gold_ohlcv_price_consistency(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="ohlcv")
    violations = (
        (df["high"] < df["low"])
        | (df["close"] > df["high"])
        | (df["close"] < df["low"])
        | (df["open"] > df["high"])
        | (df["open"] < df["low"])
    ).sum()
    return AssetCheckResult(
        passed=violations == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"price_inconsistency_rows": int(violations)},
    )


@asset_check(asset=gold_market_quality, description=f"Gold market quality data fresher than {FRESHNESS_SLA_HOURS}h.")
def gold_market_quality_freshness(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="market_quality")
    if df.empty:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR,
                                metadata={"reason": "no data"})
    max_date = pd.to_datetime(df["trade_date"]).max()
    age_hours = (datetime.now(tz=timezone.utc) - pd.Timestamp(max_date).tz_localize("UTC")).total_seconds() / 3600
    return AssetCheckResult(
        passed=age_hours <= FRESHNESS_SLA_HOURS,
        severity=AssetCheckSeverity.ERROR,
        metadata={"age_hours": round(age_hours, 2), "sla_hours": FRESHNESS_SLA_HOURS},
    )


@asset_check(asset=gold_agent_pnl, description="fill_rate_pct in [0, 100].")
def gold_agent_pnl_fill_rate_range(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="agent_pnl")
    out_of_range = ((df["fill_rate_pct"] < 0) | (df["fill_rate_pct"] > 100)).sum()
    return AssetCheckResult(
        passed=out_of_range == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"out_of_range_count": int(out_of_range)},
    )


gold_checks = [
    gold_ohlcv_vwap_deviation,
    gold_ohlcv_price_consistency,
    gold_market_quality_freshness,
    gold_agent_pnl_fill_rate_range,
]
