"""
silver_quotes — cleaned QuoteUpdate records with derived microstructure fields.

Source fields from mock-stock's QuoteUpdate event:
    symbol, best_bid, best_bid_size, best_ask, best_ask_size, spread, timestamp

Transformations:
    - Rename best_bid → bid, best_ask → ask, etc. (pipeline-standard naming)
    - Drop rows where bid or ask is null (no market on one side)
    - Drop rows where ask <= bid (crossed / locked market)
    - Derive:
        mid_price        = (bid + ask) / 2
        spread_bps       = (ask - bid) / mid_price * 10_000
        depth_imbalance  = (bid_size - ask_size) / (bid_size + ask_size)
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.quotes import bronze_quotes
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="silver_quotes",
    description="Cleaned QuoteUpdate events with bid/ask renamed and microstructure metrics derived.",
    deps=[bronze_quotes],
    metadata={"layer": "silver"},
)
def silver_quotes(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="quotes")

    if df.empty:
        context.log.warning("No bronze quotes to process.")
        return pd.DataFrame()

    # Rename to pipeline-standard column names
    df = df.rename(columns={
        "best_bid":      "bid",
        "best_ask":      "ask",
        "best_bid_size": "bid_size",
        "best_ask_size": "ask_size",
    })

    for col in ["bid", "ask", "bid_size", "ask_size"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # Drop rows without a two-sided market
    df = df.dropna(subset=["bid", "ask", "bid_size", "ask_size", "symbol", "timestamp"])
    df = df[(df["bid"] > 0) & (df["ask"] > df["bid"])].reset_index(drop=True)

    # Derived microstructure fields
    df["mid_price"]       = (df["bid"] + df["ask"]) / 2
    df["spread_bps"]      = (df["ask"] - df["bid"]) / df["mid_price"] * 10_000
    df["depth_imbalance"] = (df["bid_size"] - df["ask_size"]) / (df["bid_size"] + df["ask_size"])

    context.log.info("Silver quotes: %d clean records", len(df))
    storage.write_parquet(df, layer="silver", table="quotes", context=context)
    return df
