"""
gold_ohlcv — daily Open/High/Low/Close/Volume + VWAP per symbol.

Primary source:  silver_trades  (computed from actual trade executions)
Cross-validation: bronze_market_stats (mock-stock's own OHLCV snapshot)

Silver_trades fields used: symbol, price, quantity, timestamp
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.bronze.market_stats import bronze_market_stats
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_ohlcv",
    description=(
        "Daily OHLCV bars with VWAP per symbol, computed from silver_trades "
        "and cross-validated against bronze_market_stats."
    ),
    deps=[silver_trades, bronze_market_stats],
    metadata={"layer": "gold"},
)
def gold_ohlcv(
    context: AssetExecutionContext,
    storage: StorageResource,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    trades_path = storage.get_parquet_path(layer="silver", table="trades")
    stats_path  = storage.get_parquet_path(layer="bronze", table="market_stats")

    query = f"""
        WITH trade_bars AS (
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
        ),
        stats_bars AS (
            SELECT
                symbol,
                DATE_TRUNC('day', timestamp)::DATE  AS trade_date,
                vwap                                AS stats_vwap,
                volume                              AS stats_volume
            FROM read_parquet('{stats_path}/**/*.parquet', hive_partitioning=true)
            WHERE event_type = 'MarketStats'
              AND vwap IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol, DATE_TRUNC('day', timestamp)
                ORDER BY timestamp DESC
            ) = 1   -- latest stats snapshot per symbol-day
        )
        SELECT
            t.symbol,
            t.trade_date,
            t.open,
            t.high,
            t.low,
            t.close,
            t.volume,
            t.vwap,
            t.trade_count,
            s.stats_vwap,
            s.stats_volume,
            -- VWAP deviation between computed and reported
            ROUND(ABS(t.vwap - COALESCE(s.stats_vwap, t.vwap))
                  / NULLIF(t.vwap, 0) * 100, 4)     AS vwap_deviation_pct
        FROM trade_bars t
        LEFT JOIN stats_bars s USING (symbol, trade_date)
        ORDER BY trade_date, symbol
    """

    df = duckdb.query(query)
    context.log.info("Gold OHLCV: %d symbol-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="ohlcv", context=context)
    return df
