"""
Gold layer asset checks using Great Expectations (GX 1.x).

One GE suite per gold asset:
  gold_ohlcv_suite          — OHLC price consistency, VWAP deviation <= 20%
  gold_market_quality_suite — spread non-negative, trade_count >= 0, freshness SLA
  gold_agent_pnl_suite      — trade_count and volume non-negative
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_stock.assets.gold.agent_pnl import gold_agent_pnl
from dagster_stock.assets.gold.market_quality import gold_market_quality
from dagster_stock.assets.gold.ohlcv import gold_ohlcv
from dagster_stock.checks.gx_suites import run_suite
from dagster_stock.resources.storage_resource import StorageResource

FRESHNESS_SLA_HOURS = 25


@asset_check(
    asset=gold_ohlcv,
    description="GE suite: high >= low, open/close within [low, high], VWAP deviation <= 20%.",
)
def gold_ohlcv_suite(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="ohlcv")
    if df.empty:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no data"},
        )

    # Encode pair comparisons as boolean columns for GX
    df = df.copy()
    df["_high_gte_low"]    = df["high"] >= df["low"]
    df["_close_in_range"]  = (df["close"] >= df["low"]) & (df["close"] <= df["high"])
    df["_open_in_range"]   = (df["open"] >= df["low"])  & (df["open"]  <= df["high"])

    def build(v):
        v.expect_column_values_to_be_in_set("_high_gte_low",   [True])
        v.expect_column_values_to_be_in_set("_close_in_range", [True])
        v.expect_column_values_to_be_in_set("_open_in_range",  [True])
        v.expect_column_values_to_be_between("vwap_deviation_pct", min_value=0, max_value=20)

    return run_suite(df, "gold_ohlcv", build, severity=AssetCheckSeverity.ERROR)


@asset_check(
    asset=gold_market_quality,
    description=f"GE suite: spread >= 0, trade_count >= 0, data fresher than {FRESHNESS_SLA_HOURS}h.",
)
def gold_market_quality_suite(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="market_quality")
    if df.empty:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no data"},
        )

    max_date = pd.to_datetime(df["trade_date"]).max()
    age_hours = (
        datetime.now(tz=timezone.utc) - pd.Timestamp(max_date).tz_localize("UTC")
    ).total_seconds() / 3600
    is_fresh = age_hours <= FRESHNESS_SLA_HOURS

    def build(v):
        v.expect_column_values_to_be_between("avg_spread_bps", min_value=0)
        v.expect_column_values_to_be_between("trade_count", min_value=0)

    ge_result = run_suite(df, "gold_market_quality", build, severity=AssetCheckSeverity.ERROR)

    return AssetCheckResult(
        passed=bool(ge_result.passed and is_fresh),
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "expectations_evaluated": ge_result.metadata["expectations_evaluated"],
            "expectations_failed":    ge_result.metadata["expectations_failed"],
            "failures":               ge_result.metadata["failures"],
            "age_hours":              round(age_hours, 2),
            "sla_hours":              FRESHNESS_SLA_HOURS,
            "fresh":                  is_fresh,
        },
    )


@asset_check(
    asset=gold_agent_pnl,
    description="GE suite: trade_count and total_volume non-negative for all agent-day rows.",
)
def gold_agent_pnl_suite(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="gold", table="agent_pnl")
    if df.empty:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no data"},
        )

    def build(v):
        v.expect_column_values_to_be_between("trade_count", min_value=0)
        v.expect_column_values_to_be_between("total_volume", min_value=0)

    return run_suite(df, "gold_agent_pnl", build, severity=AssetCheckSeverity.ERROR)


gold_checks = [
    gold_ohlcv_suite,
    gold_market_quality_suite,
    gold_agent_pnl_suite,
]
