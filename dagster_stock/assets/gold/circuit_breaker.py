"""
gold_circuit_breaker — halt analysis per symbol per day.

Source: bronze_trading_halts
    Both TradingHalt and TradingResume events land on stock.halts.
    The bridge injects event_type to distinguish them.

TradingHalt fields:   symbol, timestamp, reason, circuit_breaker_level,
                      reference_price, current_price, price_change_percent
TradingResume fields: symbol, timestamp, halt_duration_ms

Methodology:
    - Pair the nth halt with the nth resume per symbol (by timestamp order).
    - Compute duration from halt_duration_ms (TradingResume).
    - Aggregate per symbol per day: count, avg duration, dominant reason/level,
      avg price impact at halt trigger.
"""

import duckdb as duckdb_lib
import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.trading_halts import bronze_trading_halts
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.resources.duckdb_resource import DuckDBResource


@asset(
    name="gold_circuit_breaker",
    description=(
        "Circuit breaker analysis: halt frequency, avg recovery time, "
        "avg price-change trigger, and most common halt reason per symbol per day."
    ),
    deps=[bronze_trading_halts],
    metadata={"layer": "gold"},
)
def gold_circuit_breaker(
    context: AssetExecutionContext,
    storage: StorageResource,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    halts_path = storage.get_parquet_path(layer="bronze", table="trading_halts")

    query = f"""
        WITH halts AS (
            SELECT
                symbol,
                timestamp::TIMESTAMPTZ                       AS halt_ts,
                reason,
                circuit_breaker_level,
                reference_price,
                current_price,
                price_change_percent,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol ORDER BY timestamp
                )                                            AS halt_seq
            FROM read_parquet('{halts_path}/**/*.parquet', hive_partitioning=true)
            WHERE event_type = 'TradingHalt'
        ),
        resumes AS (
            SELECT
                symbol,
                timestamp::TIMESTAMPTZ                       AS resume_ts,
                halt_duration_ms / 60000.0                   AS duration_minutes,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol ORDER BY timestamp
                )                                            AS resume_seq
            FROM read_parquet('{halts_path}/**/*.parquet', hive_partitioning=true)
            WHERE event_type = 'TradingResume'
        ),
        paired AS (
            SELECT
                h.symbol,
                DATE_TRUNC('day', h.halt_ts)::DATE           AS halt_date,
                h.halt_ts,
                r.resume_ts,
                COALESCE(r.duration_minutes, 0)              AS duration_minutes,
                h.reason,
                h.circuit_breaker_level,
                h.price_change_percent,
                h.reference_price,
                h.current_price
            FROM halts h
            LEFT JOIN resumes r
                ON  h.symbol    = r.symbol
                AND h.halt_seq  = r.resume_seq
        )
        SELECT
            symbol,
            halt_date,
            COUNT(*)                                         AS halt_count,
            AVG(duration_minutes)                            AS avg_duration_minutes,
            MAX(duration_minutes)                            AS max_duration_minutes,
            MODE(reason)                                     AS most_common_reason,
            MODE(circuit_breaker_level)                      AS most_common_level,
            AVG(ABS(price_change_percent))                   AS avg_trigger_move_pct,
            MAX(ABS(price_change_percent))                   AS max_trigger_move_pct,
            AVG(reference_price)                             AS avg_reference_price,
            AVG(current_price)                               AS avg_halt_price
        FROM paired
        GROUP BY symbol, halt_date
        ORDER BY halt_date, symbol
    """

    try:
        df = duckdb.query(query)
    except duckdb_lib.IOException:
        context.log.warning(
            "No bronze trading_halts data found at %s — returning empty DataFrame", halts_path
        )
        return pd.DataFrame()
    context.log.info("Gold circuit breaker: %d symbol-day rows", len(df))
    storage.write_parquet(df, layer="gold", table="circuit_breaker", context=context)
    return df
