"""
Silver layer asset checks using Great Expectations (GX 1.x).

One GE suite per silver asset:
  silver_trades_suite  — uniqueness, no nulls, price bounds, side validity
  silver_quotes_suite  — spread non-negative, bid/ask positive and ordered
  silver_orders_suite  — fill_status and agent_type within known value sets
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_stock.assets.silver.orders import silver_orders
from dagster_stock.assets.silver.quotes import silver_quotes
from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.checks.gx_suites import run_suite
from dagster_stock.resources.storage_resource import StorageResource

KNOWN_SIDES         = ["buy", "sell"]
KNOWN_FILL_STATUSES = ["open", "cancelled"]
KNOWN_AGENT_TYPES   = ["retail", "institutional", "market_maker", "hft", "unknown"]


@asset_check(
    asset=silver_trades,
    description="GE suite: no duplicates, no nulls on critical columns, price bounds, side validity.",
)
def silver_trades_suite(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="trades")
    if df.empty:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no data"},
        )

    # Compound uniqueness: encode as a single key column for GX
    df = df.copy()
    df["_trade_key"] = (
        df["trade_id"].astype(str) + "|" + df["symbol"] + "|" + df["timestamp"].astype(str)
    )

    def build(v):
        v.expect_column_values_to_be_unique("_trade_key")
        for col in ["trade_id", "symbol", "price", "quantity", "timestamp"]:
            v.expect_column_values_to_not_be_null(col)
        v.expect_column_values_to_be_between("price", min_value=0.01, max_value=1_000_000)
        v.expect_column_values_to_be_in_set("side", KNOWN_SIDES)

    return run_suite(df, "silver_trades", build, severity=AssetCheckSeverity.ERROR)


@asset_check(
    asset=silver_quotes,
    description="GE suite: spread_bps non-negative, bid/ask positive, ask > bid.",
)
def silver_quotes_suite(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="quotes")
    if df.empty:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no data"},
        )

    # Encode ask > bid as a boolean column for GX
    df = df.copy()
    df["_ask_gt_bid"] = df["ask"] > df["bid"]

    def build(v):
        v.expect_column_values_to_be_between("spread_bps", min_value=0)
        v.expect_column_values_to_be_between("bid", min_value=0.01)
        v.expect_column_values_to_be_between("ask", min_value=0.01)
        v.expect_column_values_to_be_in_set("_ask_gt_bid", [True])

    return run_suite(df, "silver_quotes", build, severity=AssetCheckSeverity.ERROR)


@asset_check(
    asset=silver_orders,
    description="GE suite: fill_status and agent_type within known value sets.",
)
def silver_orders_suite(storage: StorageResource) -> AssetCheckResult:
    df = storage.read_parquet(layer="silver", table="orders")
    if df.empty:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no data"},
        )

    def build(v):
        v.expect_column_values_to_be_in_set("fill_status", KNOWN_FILL_STATUSES)
        v.expect_column_values_to_be_in_set("agent_type", KNOWN_AGENT_TYPES)

    return run_suite(df, "silver_orders", build, severity=AssetCheckSeverity.WARN)


silver_checks = [
    silver_trades_suite,
    silver_quotes_suite,
    silver_orders_suite,
]
