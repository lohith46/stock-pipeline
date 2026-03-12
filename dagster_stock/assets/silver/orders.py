"""
silver_orders — cleaned and enriched order records.

Source fields from mock-stock's OrderPlaced / OrderCancelled events
(both land on stock.orders; distinguished by the `event_type` column
injected by the ws_to_kafka bridge):

    OrderPlaced:   event_type, order_id, symbol, side, order_type,
                   price (nullable for Market orders), quantity,
                   agent_id, agent_type, timestamp
    OrderCancelled: event_type, order_id, symbol, reason, timestamp

Transformations:
    - Base on OrderPlaced rows; mark cancelled orders from OrderCancelled
    - Normalise side:       "Buy" → "buy",  "Sell" → "sell"
    - Normalise order_type: "Market" → "market", "Limit" → "limit", etc.
    - Normalise agent_type: "RetailTrader" → "retail", etc.
    - Derive fill_status:   "open" | "cancelled"
    - Cast price / quantity to float64; timestamp to UTC datetime
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.orders import bronze_orders
from dagster_stock.resources.storage_resource import StorageResource


AGENT_TYPE_MAP = {
    "RetailTrader":          "retail",
    "InstitutionalInvestor": "institutional",
    "MarketMaker":           "market_maker",
    "HighFrequencyTrader":   "hft",
}

ORDER_TYPE_MAP = {
    "Market":    "market",
    "Limit":     "limit",
    "StopLoss":  "stop_loss",
    "StopLimit": "stop_limit",
}


@asset(
    name="silver_orders",
    description=(
        "Cleaned orders: OrderPlaced events enriched with cancellation status, "
        "normalised side/order_type/agent_type, and derived fill_status."
    ),
    deps=[bronze_orders],
    required_resource_keys={"storage"},
    metadata={"layer": "silver"},
)
def silver_orders(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="orders")

    if df.empty:
        context.log.warning("No bronze orders to process.")
        return pd.DataFrame()

    # Split by event sub-type
    if "event_type" in df.columns:
        placed    = df[df["event_type"] == "OrderPlaced"].copy()
        cancelled = df[df["event_type"] == "OrderCancelled"].copy()
    else:
        placed    = df.copy()
        cancelled = pd.DataFrame()

    if placed.empty:
        context.log.warning("No OrderPlaced events in bronze orders.")
        return pd.DataFrame()

    # Mark orders that have a corresponding cancellation
    cancelled_ids: set[str] = set(
        cancelled["order_id"].dropna().astype(str)
    ) if not cancelled.empty else set()

    # 1. Type casting
    placed["price"]     = pd.to_numeric(placed.get("price"),    errors="coerce")
    placed["quantity"]  = pd.to_numeric(placed.get("quantity"), errors="coerce")
    placed["timestamp"] = pd.to_datetime(placed["timestamp"], utc=True, errors="coerce")

    placed = placed.dropna(subset=["order_id", "symbol", "quantity", "timestamp"])

    # 2. Normalise categorical fields
    placed["side"]       = placed["side"].str.lower()
    placed["order_type"] = placed["order_type"].map(ORDER_TYPE_MAP).fillna("unknown")
    placed["agent_type"] = placed["agent_type"].map(AGENT_TYPE_MAP).fillna("unknown")

    # 3. Derive fill_status
    placed["fill_status"] = placed["order_id"].astype(str).apply(
        lambda oid: "cancelled" if oid in cancelled_ids else "open"
    )

    context.log.info(
        "Silver orders: %d placed  (%d marked cancelled)",
        len(placed),
        (placed["fill_status"] == "cancelled").sum(),
    )
    storage.write_parquet(placed, layer="silver", table="orders", context=context)
    return placed
