"""
gold_ohlcv — daily Open/High/Low/Close/Volume + VWAP per symbol.

Sources:
- silver_trades  (primary price/volume data)
- bronze_market_stats (cross-validation of daily volume)
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.bronze.market_stats import bronze_market_stats
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_ohlcv",
    description="Daily OHLCV bars with VWAP per symbol, cross-validated against market stats.",
    deps=[silver_trades, bronze_market_stats],
    required_resource_keys={"storage", "duckdb"},
    metadata={"layer": "gold"},
)
def gold_ohlcv(context: AssetExecutionContext, storage: StorageResource, duckdb: DuckDBResource) -> pd.DataFrame:
    trades_path = storage.get_parquet_path(layer="silver", table="trades")

    query = f"""
        SELECT
            symbol,
            DATE_TRUNC('day', timestamp)::DATE                  AS trade_date,
            FIRST(price ORDER BY timestamp)                     AS open,
            MAX(price)                                          AS high,
            MIN(price)                                          AS low,
            LAST(price ORDER BY timestamp)                      AS close,
            SUM(quantity)                                       AS volume,
            SUM(price * quantity) / NULLIF(SUM(quantity), 0)   AS vwap,
            COUNT(*)                                            AS trade_count
        FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true)
        GROUP BY symbol, DATE_TRUNC('day', timestamp)
        ORDER BY trade_date, symbol
    """

    df = duckdb.query(query)
    context.log.info("Gold OHLCV: %d symbol-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="ohlcv", context=context)
    return df
