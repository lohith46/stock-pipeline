"""
silver_orders — validated order records promoted from the staging layer.

Source: stg_orders (staging/orders Parquet)

Only rows where _validation_status == "valid" AND _is_duplicate == False are
included.  OrderCancelled rows are excluded from silver (they are not orders;
they are state-change events whose effect is already captured in fill_status).
Original bronze columns are preserved alongside all normalized columns from
staging so the transformation chain stays visible.

Key columns present in the output
----------------------------------
Original bronze  : order_id, symbol, side, order_type, price, quantity,
                   agent_id, agent_type, timestamp, event_type, reason
From staging     : price_cast, quantity_cast, timestamp_utc,
                   side_normalized, order_type_normalized, agent_type_normalized,
                   fill_status, _is_duplicate, _validation_status, _invalid_reasons
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.staging.orders import stg_orders
from dagster_stock.resources.storage_resource import StorageResource


@asset(
    name="silver_orders",
    description=(
        "Validated OrderPlaced records with original bronze columns preserved "
        "alongside normalized columns from the staging layer.  "
        "OrderCancelled rows are excluded; cancellation state is carried as fill_status."
    ),
    deps=[stg_orders],
    metadata={"layer": "silver"},
)
def silver_orders(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="staging", table="orders")

    if df.empty:
        context.log.warning("No staged orders to promote.")
        return pd.DataFrame()

    # Exclude OrderCancelled rows — they are not order records
    if "event_type" in df.columns:
        placed_only = df["event_type"] != "OrderCancelled"
    else:
        placed_only = pd.Series(True, index=df.index)

    is_valid   = df["_validation_status"] == "valid"
    is_not_dup = ~df["_is_duplicate"].astype(bool) if "_is_duplicate" in df.columns else pd.Series(True, index=df.index)

    silver = df[placed_only & is_valid & is_not_dup].reset_index(drop=True)

    context.log.info(
        "silver_orders: %d promoted from %d staged  "
        "(%d cancellation_events excluded  %d invalid  %d duplicate skipped)",
        len(silver),
        len(df),
        (~placed_only).sum(),
        (~is_valid & placed_only).sum(),
        (~is_not_dup & placed_only & is_valid).sum(),
    )

    storage.write_parquet(silver, layer="silver", table="orders", context=context)
    return silver
