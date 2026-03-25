"""
gold_agent_pnl — daily P&L, trade count, and volume by agent type.

Sources:
    silver_trades  — TradeExecuted events with buyer_agent_id / seller_agent_id
    silver_orders  — OrderPlaced events with agent_id → agent_type mapping

Methodology:
    1. Build an agent_id → agent_type registry from silver_orders.
    2. For each trade, attribute:
         buyer  gets: pnl = -(price × quantity)   [cash outflow]
         seller gets: pnl = +(price × quantity)   [cash inflow]
    3. Sum net PnL per agent_type per day.
       (Positive net PnL = collected more from sells than spent on buys.)

silver_trades fields used:
    trade_id, symbol, price, quantity, buyer_agent_id, seller_agent_id, timestamp

silver_orders fields used:
    agent_id, agent_type
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.silver.trades import silver_trades
from dagster_stock.assets.silver.orders import silver_orders
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_agent_pnl",
    description="Daily net P&L, trade count, and volume by agent type (retail/institutional/hft/market_maker).",
    deps=[silver_trades, silver_orders],
    metadata={"layer": "gold"},
)
def gold_agent_pnl(
    context: AssetExecutionContext,
    storage: StorageResource,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    trades_path = storage.get_parquet_path(layer="silver", table="trades")
    orders_path = storage.get_parquet_path(layer="silver", table="orders")

    query = f"""
        -- Step 1: agent_id → agent_type registry (distinct, prefer non-unknown)
        WITH agent_map AS (
            SELECT DISTINCT
                agent_id,
                agent_type
            FROM read_parquet('{orders_path}/**/*.parquet', hive_partitioning=true)
            WHERE agent_id IS NOT NULL
              AND agent_type <> 'unknown'
        ),

        -- Step 2: buyer-side contributions (cash outflow = negative PnL)
        buy_side AS (
            SELECT
                t.buyer_agent_id                          AS agent_id,
                COALESCE(am.agent_type, 'unknown')        AS agent_type,
                DATE_TRUNC('day', t.timestamp::TIMESTAMPTZ)::DATE      AS trade_date,
                t.symbol,
                -(t.price * t.quantity)                   AS pnl,
                t.quantity                                 AS volume,
                1                                         AS trade_cnt
            FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true) t
            LEFT JOIN agent_map am ON t.buyer_agent_id = am.agent_id
        ),

        -- Step 3: seller-side contributions (cash inflow = positive PnL)
        sell_side AS (
            SELECT
                t.seller_agent_id                         AS agent_id,
                COALESCE(am.agent_type, 'unknown')        AS agent_type,
                DATE_TRUNC('day', t.timestamp::TIMESTAMPTZ)::DATE      AS trade_date,
                t.symbol,
                (t.price * t.quantity)                    AS pnl,
                t.quantity                                 AS volume,
                1                                         AS trade_cnt
            FROM read_parquet('{trades_path}/**/*.parquet', hive_partitioning=true) t
            LEFT JOIN agent_map am ON t.seller_agent_id = am.agent_id
        ),

        all_sides AS (
            SELECT * FROM buy_side
            UNION ALL
            SELECT * FROM sell_side
        ),

        -- Step 4: aggregate per agent_type / symbol / day
        per_agent_symbol AS (
            SELECT
                agent_type,
                symbol,
                trade_date,
                SUM(pnl)       AS realized_pnl,
                SUM(volume)    AS total_volume,
                SUM(trade_cnt) AS trade_count
            FROM all_sides
            GROUP BY agent_type, symbol, trade_date
        )

        -- Step 5: roll up to agent_type / day (across all symbols)
        SELECT
            agent_type,
            trade_date,
            SUM(realized_pnl)                           AS realized_pnl,
            SUM(total_volume)                           AS total_volume,
            SUM(trade_count)                            AS trade_count,
            ROUND(AVG(realized_pnl), 4)                 AS avg_pnl_per_symbol,
            COUNT(DISTINCT symbol)                      AS symbols_traded
        FROM per_agent_symbol
        GROUP BY agent_type, trade_date
        ORDER BY trade_date, agent_type
    """

    df = duckdb.query(query)
    context.log.info("Gold agent P&L: %d agent-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="agent_pnl", context=context)
    return df
