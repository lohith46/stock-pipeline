"""
invalid_orders — order records rejected from the silver layer.

Reads stg_orders and materialises only the rows that are either:
    • _validation_status == "invalid"  (failed one or more data-quality checks), or
    • _is_duplicate == True            (same order_id placed more than once)

OrderCancelled rows that fail validation (e.g. missing order_id) are also
captured here — they are never promoted to silver regardless.

All original bronze columns are preserved alongside every staging diagnostic
column so analysts can trace the exact failure reason without re-running.

Stored under ./data/invalid/orders/ with the standard daily partitioning.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.orders import stg_orders
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="invalid_orders",
    description=(
        "Invalid or duplicate order records rejected from the silver layer, "
        "with full reason metadata preserved for investigation."
    ),
    deps=[stg_orders],
    metadata={"layer": "invalid"},
)
def invalid_orders(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="staging", table="orders")

    if df.empty:
        context.log.info("No staged orders — nothing to scan for invalids.")
        return pd.DataFrame()

    is_invalid = df["_validation_status"] == "invalid"
    is_dup     = df["_is_duplicate"].astype(bool) if "_is_duplicate" in df.columns else pd.Series(False, index=df.index)

    flagged = df[is_invalid | is_dup].copy()

    context.log.info(
        "invalid_orders: %d flagged  (invalid=%d  duplicate=%d)",
        len(flagged),
        is_invalid[is_invalid | is_dup].sum(),
        is_dup[is_invalid | is_dup].sum(),
    )

    if not flagged.empty:
        storage.write_parquet(flagged, layer="invalid", table="orders", context=context)

    return flagged
