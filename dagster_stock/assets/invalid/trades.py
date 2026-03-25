"""
invalid_trades — TradeExecuted records rejected from the silver layer.

Reads stg_trades and materialises only the rows that are either:
    • _validation_status == "invalid"  (failed one or more data-quality checks), or
    • _is_duplicate == True            (key repeated after the first occurrence)

All original bronze columns are preserved alongside every staging diagnostic
column (_invalid_reasons, _is_duplicate, _validation_status) so analysts can
investigate exactly why each record was rejected without re-running the pipeline.

Stored under ./data/invalid/trades/ with the standard daily partitioning.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.trades import stg_trades
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="invalid_trades",
    description=(
        "Invalid or duplicate TradeExecuted records rejected from the silver layer, "
        "with full reason metadata preserved for investigation."
    ),
    deps=[stg_trades],
    metadata={"layer": "invalid"},
)
def invalid_trades(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="staging", table="trades")

    if df.empty:
        context.log.info("No staged trades — nothing to scan for invalids.")
        return pd.DataFrame()

    is_invalid = df["_validation_status"] == "invalid"
    is_dup     = df["_is_duplicate"].astype(bool) if "_is_duplicate" in df.columns else pd.Series(False, index=df.index)

    flagged = df[is_invalid | is_dup].copy()

    context.log.info(
        "invalid_trades: %d flagged  (invalid=%d  duplicate=%d)",
        len(flagged),
        is_invalid[is_invalid | is_dup].sum(),
        is_dup[is_invalid | is_dup].sum(),
    )

    if not flagged.empty:
        storage.write_parquet(flagged, layer="invalid", table="trades", context=context)

    return flagged
