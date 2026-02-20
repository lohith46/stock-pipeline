"""
gold_market_quality — average spread, depth, and composite quality score per symbol.

Sources:
- silver_trades
- silver_quotes
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.silver.quotes import silver_quotes
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_market_quality",
    description="Per-symbol market quality: avg spread (bps), depth imbalance, composite score.",
    deps=[silver_trades, silver_quotes],
    required_resource_keys={"storage", "duckdb"},
    metadata={"layer": "gold"},
)
def gold_market_quality(context: AssetExecutionContext, storage: StorageResource, duckdb: DuckDBResource) -> pd.DataFrame:
    quotes_path = storage.get_parquet_path(layer="silver", table="quotes")
    trades_path = storage.get_parquet_path(layer="silver", table="trades")

    query = f"""
        WITH quote_stats AS (
            SELECT
                symbol,
                DATE_TRUNC('day', timestamp)::DATE  AS trade_date,
                AVG(spread_bps)                     AS avg_spread_bps,
                AVG(ABS(depth_imbalance))           AS avg_depth_imbalance,
                COUNT(*)                            AS quote_count
            FROM read_parquet('{quotes_path}/**/*.parquet', hive_partitioning=true)
            GROUP BY symbol, DATE_TRUNC('day', timestamp)
        ),
        trade_stats AS (
            SELECT
                symbol,
                DATE_TRUNC('day', timestamp)::DATE  AS trade_date,
                COUNT(*)                            AS trade_count,
                SUM(quantity)                       AS total_volume
            FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true)
            GROUP BY symbol, DATE_TRUNC('day', timestamp)
        )
        SELECT
            q.symbol,
            q.trade_date,
            q.avg_spread_bps,
            q.avg_depth_imbalance,
            q.quote_count,
            t.trade_count,
            t.total_volume,
            -- Composite quality score: lower spread + lower imbalance = higher quality
            100.0 - LEAST(q.avg_spread_bps, 100) - (q.avg_depth_imbalance * 50) AS quality_score
        FROM quote_stats q
        LEFT JOIN trade_stats t USING (symbol, trade_date)
        ORDER BY q.trade_date, q.symbol
    """

    df = duckdb.query(query)
    context.log.info("Gold market quality: %d symbol-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="market_quality", context=context)
    return df
