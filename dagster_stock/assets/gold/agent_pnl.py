"""
gold_agent_pnl — P&L, fill rate, and slippage by agent type.

Sources:
- silver_trades
- silver_orders
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.silver.orders import silver_orders
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_agent_pnl",
    description="Daily P&L, fill rate, and avg slippage per agent type.",
    deps=[silver_trades, silver_orders],
    required_resource_keys={"storage", "duckdb"},
    metadata={"layer": "gold"},
)
def gold_agent_pnl(context: AssetExecutionContext, storage: StorageResource, duckdb: DuckDBResource) -> pd.DataFrame:
    trades_path = storage.get_parquet_path(layer="silver", table="trades")
    orders_path = storage.get_parquet_path(layer="silver", table="orders")

    query = f"""
        WITH order_fills AS (
            SELECT
                o.agent_type,
                DATE_TRUNC('day', o.timestamp)::DATE            AS trade_date,
                COUNT(*)                                         AS total_orders,
                SUM(CASE WHEN o.fill_status = 'filled' THEN 1 ELSE 0 END) AS filled_orders,
                AVG(o.price)                                     AS avg_order_price
            FROM read_parquet('{orders_path}/**/*.parquet', hive_partitioning=true) o
            GROUP BY o.agent_type, DATE_TRUNC('day', o.timestamp)
        ),
        trade_pnl AS (
            SELECT
                agent_type,
                DATE_TRUNC('day', timestamp)::DATE              AS trade_date,
                SUM(CASE WHEN side = 'buy'  THEN -price * quantity
                         WHEN side = 'sell' THEN  price * quantity
                         ELSE 0 END)                             AS realized_pnl,
                AVG(ABS(price - mid_price)) / NULLIF(mid_price, 0) * 10000 AS avg_slippage_bps
            FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true)
            GROUP BY agent_type, DATE_TRUNC('day', timestamp)
        )
        SELECT
            COALESCE(f.agent_type, p.agent_type)        AS agent_type,
            COALESCE(f.trade_date, p.trade_date)        AS trade_date,
            f.total_orders,
            f.filled_orders,
            ROUND(f.filled_orders * 100.0 / NULLIF(f.total_orders, 0), 2) AS fill_rate_pct,
            p.realized_pnl,
            p.avg_slippage_bps
        FROM order_fills f
        FULL OUTER JOIN trade_pnl p USING (agent_type, trade_date)
        ORDER BY trade_date, agent_type
    """

    df = duckdb.query(query)
    context.log.info("Gold agent P&L: %d agent-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="agent_pnl", context=context)
    return df
