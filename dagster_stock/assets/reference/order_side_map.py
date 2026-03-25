"""
ref_order_side_map — canonical mapping of raw order side values.

Static reference derived from the mock-stock OrderPlaced specification.

Columns
-------
raw_value        : exact string from mock-stock ("Buy" / "Sell")
normalized_value : lowercase pipeline code ("buy" / "sell")
aggressor_flag   : True when this side is typically the price-taking / aggressive side
                   in a TradeExecuted event (Buy = aggressive buyer)
"""

import pandas as pd
from dagster import asset


_ROWS = [
    ("Buy",  "buy",  True),
    ("Sell", "sell", False),
]


@asset(
    name="ref_order_side_map",
    description=(
        "Static reference table mapping raw mock-stock order side strings to "
        "normalized pipeline codes."
    ),
    metadata={"layer": "reference"},
)
def ref_order_side_map() -> pd.DataFrame:
    return pd.DataFrame(_ROWS, columns=["raw_value", "normalized_value", "aggressor_flag"])
