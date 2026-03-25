"""
stg_orders — labeled and normalized order records.

Reads bronze_orders (OrderPlaced + OrderCancelled rows on the same topic)
and keeps ALL original columns intact.  No rows are dropped; every record
is annotated with diagnostic and normalized columns:

    price_cast            : float64 price (NaN for Market orders is expected)
    quantity_cast         : float64 quantity
    timestamp_utc         : timezone-aware UTC datetime
    side_normalized       : "buy" | "sell"  (lowercased from "Buy" / "Sell")
    order_type_normalized : "market" | "limit" | "stop_loss" | "stop_limit" | "unknown"
    agent_type_normalized : "retail" | "institutional" | "market_maker" | "hft" | "unknown"
    fill_status           : "open" | "cancelled" — derived for OrderPlaced rows by
                            cross-referencing with OrderCancelled rows on the same topic;
                            OrderCancelled rows themselves get "cancellation_event"
    _is_duplicate         : True for any OrderPlaced row whose order_id appears more
                            than once (first occurrence → False); always False for
                            OrderCancelled rows (multiple cancellations are unusual but
                            kept for investigation)
    _validation_status    : "valid" | "invalid"
    _invalid_reasons      : pipe-separated list of triggered checks
"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_stock.assets.bronze.orders import bronze_orders
from dagster_stock.resources.storage_resource import StorageResource
from dagster_stock.utils.validation import build_invalid_reasons, label_duplicates


AGENT_TYPE_MAP: dict[str, str] = {
    "RetailTrader":          "retail",
    "InstitutionalInvestor": "institutional",
    "MarketMaker":           "market_maker",
    "HighFrequencyTrader":   "hft",
}

ORDER_TYPE_MAP: dict[str, str] = {
    "Market":    "market",
    "Limit":     "limit",
    "StopLoss":  "stop_loss",
    "StopLimit": "stop_limit",
}


@asset(
    name="stg_orders",
    description=(
        "Labeled and normalized order records. All rows retained; "
        "invalids and duplicates are flagged, not dropped."
    ),
    deps=[bronze_orders],
    metadata={"layer": "staging"},
)
def stg_orders(context: AssetExecutionContext, storage: StorageResource) -> pd.DataFrame:
    df: pd.DataFrame = storage.read_parquet(layer="bronze", table="orders")

    if df.empty:
        context.log.warning("No bronze orders to process.")
        return pd.DataFrame()

    df = df.copy()

    # ── 1. Collect cancelled order IDs to derive fill_status ───────────────
    if "event_type" in df.columns:
        cancelled_ids: set[str] = set(
            df.loc[df["event_type"] == "OrderCancelled", "order_id"].dropna().astype(str)
        )
        placed_mask = df["event_type"] == "OrderPlaced"
    else:
        cancelled_ids = set()
        placed_mask = pd.Series(True, index=df.index)

    context.log.info(
        "bronze_orders: %d OrderPlaced  |  %d OrderCancelled  |  %d cancellation IDs",
        placed_mask.sum(),
        (~placed_mask).sum(),
        len(cancelled_ids),
    )

    # ── 2. Cast columns (originals unchanged) ──────────────────────────────
    df["price_cast"]    = pd.to_numeric(df.get("price"),    errors="coerce")
    df["quantity_cast"] = pd.to_numeric(df.get("quantity"), errors="coerce")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # ── 3. Normalized categoricals (new columns, originals kept) ───────────
    df["side_normalized"] = (
        df["side"].str.lower()
        if "side" in df.columns
        else pd.Series("unknown", index=df.index, dtype=str)
    )
    df["order_type_normalized"] = (
        df["order_type"].map(ORDER_TYPE_MAP).fillna("unknown")
        if "order_type" in df.columns
        else pd.Series("unknown", index=df.index, dtype=str)
    )
    df["agent_type_normalized"] = (
        df["agent_type"].map(AGENT_TYPE_MAP).fillna("unknown")
        if "agent_type" in df.columns
        else pd.Series("unknown", index=df.index, dtype=str)
    )

    # ── 4. Derive fill_status ──────────────────────────────────────────────
    #  OrderCancelled rows  → "cancellation_event"  (they are not orders themselves)
    #  OrderPlaced rows     → "cancelled" | "open"
    def _fill_status(row: pd.Series) -> str:
        if row.get("event_type") == "OrderCancelled":
            return "cancellation_event"
        return "cancelled" if str(row.get("order_id", "")) in cancelled_ids else "open"

    df["fill_status"] = df.apply(_fill_status, axis=1)

    # ── 5. Duplicate flag (OrderPlaced only — same order_id placed twice) ──
    df["_is_duplicate"] = False
    if placed_mask.any():
        dup_in_placed = label_duplicates(df.loc[placed_mask], subset=["order_id"])
        df.loc[dup_in_placed[dup_in_placed].index, "_is_duplicate"] = True

    # ── 6. Validation reasons ───────────────────────────────────────────────
    null_order_id = df.get("order_id", pd.Series(dtype=object)).isna()
    null_symbol   = df["symbol"].isna() if "symbol" in df.columns else pd.Series(True, index=df.index)
    null_ts       = df["timestamp_utc"].isna()
    # quantity is only required for OrderPlaced rows; Market orders may have null price — that is valid
    null_quantity_placed = placed_mask & df["quantity_cast"].isna()

    df["_invalid_reasons"] = build_invalid_reasons(df, {
        "null_order_id":       null_order_id,
        "null_symbol":         null_symbol,
        "null_timestamp":      null_ts,
        "null_quantity":       null_quantity_placed,
    })
    df["_validation_status"] = df["_invalid_reasons"].apply(
        lambda r: "invalid" if r else "valid"
    )

    context.log.info(
        "stg_orders: %d total | %d valid | %d invalid | %d duplicate",
        len(df),
        (df["_validation_status"] == "valid").sum(),
        (df["_validation_status"] == "invalid").sum(),
        df["_is_duplicate"].sum(),
    )

    storage.write_parquet(df, layer="staging", table="orders", context=context)
    return df
