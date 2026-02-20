"""
gold_circuit_breaker — halt frequency, recovery time, and pre/post price analysis.

Source:
- bronze_trading_halts
- silver_trades (for pre/post price context)
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.trading_halts import bronze_trading_halts
from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_circuit_breaker",
    description="Circuit breaker analysis: halt frequency, avg recovery time, pre/post price impact.",
    deps=[bronze_trading_halts, silver_trades],
    required_resource_keys={"storage", "duckdb"},
    metadata={"layer": "gold"},
)
def gold_circuit_breaker(context: AssetExecutionContext, storage: StorageResource, duckdb: DuckDBResource) -> pd.DataFrame:
    halts_path = storage.get_parquet_path(layer="bronze", table="trading_halts")
    trades_path = storage.get_parquet_path(layer="silver", table="trades")

    query = f"""
        WITH halts AS (
            SELECT
                symbol,
                halt_start::TIMESTAMPTZ                             AS halt_start,
                halt_end::TIMESTAMPTZ                               AS halt_end,
                DATEDIFF('minute', halt_start::TIMESTAMPTZ,
                                   halt_end::TIMESTAMPTZ)           AS duration_minutes,
                reason
            FROM read_parquet('{halts_path}/**/*.parquet', hive_partitioning=true)
            WHERE halt_end IS NOT NULL
        ),
        pre_price AS (
            SELECT t.symbol, h.halt_start,
                   LAST(t.price ORDER BY t.timestamp)               AS price_before_halt
            FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true) t
            JOIN halts h ON t.symbol = h.symbol
              AND t.timestamp BETWEEN h.halt_start - INTERVAL '5 minutes' AND h.halt_start
            GROUP BY t.symbol, h.halt_start
        ),
        post_price AS (
            SELECT t.symbol, h.halt_start,
                   FIRST(t.price ORDER BY t.timestamp)              AS price_after_halt
            FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true) t
            JOIN halts h ON t.symbol = h.symbol
              AND t.timestamp BETWEEN h.halt_end AND h.halt_end + INTERVAL '5 minutes'
            GROUP BY t.symbol, h.halt_start
        )
        SELECT
            h.symbol,
            DATE_TRUNC('day', h.halt_start)::DATE                   AS halt_date,
            COUNT(*)                                                 AS halt_count,
            AVG(h.duration_minutes)                                  AS avg_duration_minutes,
            MODE(h.reason)                                           AS most_common_reason,
            AVG(pre.price_before_halt)                               AS avg_pre_halt_price,
            AVG(post.price_after_halt)                               AS avg_post_halt_price,
            AVG((post.price_after_halt - pre.price_before_halt)
                / NULLIF(pre.price_before_halt, 0) * 100)            AS avg_price_impact_pct
        FROM halts h
        LEFT JOIN pre_price  pre  USING (symbol, halt_start)
        LEFT JOIN post_price post USING (symbol, halt_start)
        GROUP BY h.symbol, DATE_TRUNC('day', h.halt_start)
        ORDER BY halt_date, h.symbol
    """

    df = duckdb.query(query)
    context.log.info("Gold circuit breaker: %d symbol-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="circuit_breaker", context=context)
    return df
