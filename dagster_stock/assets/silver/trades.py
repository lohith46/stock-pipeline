"""
silver_trades — validated TradeExecuted records promoted from the staging layer.

Source: stg_trades (staging/trades Parquet)

Only rows where _validation_status == "valid" AND _is_duplicate == False are
included.  All original bronze columns are carried forward alongside the
normalized / cast columns added in staging, so the transformation chain remains
visible for debugging.

Key columns present in the output
----------------------------------
Original bronze  : trade_id, symbol, price, quantity, is_aggressive_buy,
                   buyer_agent_id, seller_agent_id, buy_order_id, sell_order_id,
                   timestamp, event_type
From staging     : price_cast, quantity_cast, timestamp_utc, side_normalized,
                   _is_duplicate, _validation_status, _invalid_reasons
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.trades import stg_trades
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="silver_trades",
    description=(
        "Validated TradeExecuted records with original bronze columns preserved "
        "alongside normalized / cast columns from the staging layer."
    ),
    deps=[stg_trades],
    metadata={"layer": "silver"},
)
def silver_trades(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="staging", table="trades")

    if df.empty:
        context.log.warning("No staged trades to promote.")
        return pd.DataFrame()

    is_valid = df["_validation_status"] == "valid"
    is_not_dup = ~df["_is_duplicate"].astype(bool) if "_is_duplicate" in df.columns else pd.Series(True, index=df.index)

    silver = df[is_valid & is_not_dup].reset_index(drop=True)

    context.log.info(
        "silver_trades: %d promoted from %d staged  (%d invalid  %d duplicate skipped)",
        len(silver),
        len(df),
        (~is_valid).sum(),
        (~is_not_dup).sum(),
    )

    storage.write_parquet(silver, layer="silver", table="trades", context=context)
    return silver
