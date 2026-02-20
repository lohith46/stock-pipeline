"""
silver_quotes — cleaned quote data with derived market microstructure fields.

Derived fields:
- spread_bps:       (ask - bid) / mid_price * 10_000
- mid_price:        (bid + ask) / 2
- depth_imbalance:  (bid_size - ask_size) / (bid_size + ask_size)
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.quotes import bronze_quotes
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="silver_quotes",
    description="Cleaned quotes with spread_bps, mid_price, and depth_imbalance.",
    deps=[bronze_quotes],
    required_resource_keys={"storage"},
    metadata={"layer": "silver"},
)
def silver_quotes(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="quotes")

    if df.empty:
        context.log.warning("No bronze quotes to process.")
        return pd.DataFrame()

    for col in ["bid", "ask", "bid_size", "ask_size"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["bid", "ask", "bid_size", "ask_size", "symbol", "timestamp"])
    df = df[(df["bid"] > 0) & (df["ask"] > df["bid"])]

    df["mid_price"]       = (df["bid"] + df["ask"]) / 2
    df["spread_bps"]      = (df["ask"] - df["bid"]) / df["mid_price"] * 10_000
    df["depth_imbalance"] = (df["bid_size"] - df["ask_size"]) / (df["bid_size"] + df["ask_size"])

    context.log.info("Silver quotes: %d clean records", len(df))
    storage.write_parquet(df, layer="silver", table="quotes", context=context)
    return df
