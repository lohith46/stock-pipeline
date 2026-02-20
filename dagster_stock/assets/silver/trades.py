"""
silver_trades — cleaned, typed, and validated trade records.

Transformations applied:
- Deduplicate on (trade_id, symbol, timestamp)
- Cast price / quantity to float64
- Drop rows with null price, quantity, or symbol
- Reject rows where price <= 0 or quantity <= 0
- Parse timestamp to UTC datetime
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.trades import bronze_trades
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="silver_trades",
    description="Cleaned, typed, and deduplicated trade events.",
    deps=[bronze_trades],
    required_resource_keys={"storage"},
    metadata={"layer": "silver"},
)
def silver_trades(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="trades")

    if df.empty:
        context.log.warning("No bronze trades to process.")
        return pd.DataFrame()

    # 1. Deduplication
    before = len(df)
    df = df.drop_duplicates(subset=["trade_id", "symbol", "timestamp"])
    context.log.info("Dedup removed %d rows", before - len(df))

    # 2. Type casting
    df["price"]     = pd.to_numeric(df["price"],    errors="coerce")
    df["quantity"]  = pd.to_numeric(df["quantity"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # 3. Reject invalid rows
    invalid_mask = df["price"].isna() | df["quantity"].isna() | df["symbol"].isna() | (df["price"] <= 0) | (df["quantity"] <= 0)
    rejected = df[invalid_mask]
    df = df[~invalid_mask].reset_index(drop=True)

    if not rejected.empty:
        context.log.warning("Rejected %d invalid trade rows", len(rejected))
        storage.write_parquet(rejected, layer="rejected", table="trades", context=context)

    context.log.info("Silver trades: %d clean records", len(df))
    storage.write_parquet(df, layer="silver", table="trades", context=context)
    return df
