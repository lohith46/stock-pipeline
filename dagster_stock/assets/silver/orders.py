"""
silver_orders — cleaned orders with agent_type joined and fill_status enriched.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.orders import bronze_orders
from dagster_stock.resources.storage_resource import StorageResource

FILL_STATUS_MAP = {
    "F":  "filled",
    "PF": "partial_fill",
    "C":  "cancelled",
    "O":  "open",
}

AGENT_TYPE_MAP = {
    "MM":   "market_maker",
    "ARB":  "arbitrageur",
    "RET":  "retail",
    "INST": "institutional",
}


@asset(
    name="silver_orders",
    description="Cleaned orders with agent_type label and normalised fill_status.",
    deps=[bronze_orders],
    required_resource_keys={"storage"},
    metadata={"layer": "silver"},
)
def silver_orders(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="orders")

    if df.empty:
        context.log.warning("No bronze orders to process.")
        return pd.DataFrame()

    df["price"]    = pd.to_numeric(df.get("price"), errors="coerce")
    df["quantity"] = pd.to_numeric(df.get("quantity"), errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    df = df.dropna(subset=["order_id", "symbol", "price", "quantity", "timestamp"])

    df["fill_status"] = df["fill_status"].map(FILL_STATUS_MAP).fillna("unknown")
    df["agent_type"]  = df["agent_code"].map(AGENT_TYPE_MAP).fillna("unknown")

    context.log.info("Silver orders: %d clean records", len(df))
    storage.write_parquet(df, layer="silver", table="orders", context=context)
    return df
