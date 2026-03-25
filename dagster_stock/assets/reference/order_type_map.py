"""
ref_order_type_map — canonical mapping of raw order_type values.

Static reference derived from the mock-stock OrderPlaced specification.

Columns
-------
raw_value        : exact string from mock-stock (e.g. "StopLoss")
normalized_value : snake_case pipeline code (e.g. "stop_loss")
requires_price   : True when the order type mandates a non-null price field
"""

import pandas as pd
from dagster import asset


_ROWS = [
    ("Market",    "market",     False),
    ("Limit",     "limit",      True),
    ("StopLoss",  "stop_loss",  True),
    ("StopLimit", "stop_limit", True),
]


@asset(
    name="ref_order_type_map",
    description=(
        "Static reference table mapping raw mock-stock order_type strings to "
        "normalized pipeline codes, with price-requirement metadata."
    ),
    metadata={"layer": "reference"},
)
def ref_order_type_map() -> pd.DataFrame:
    return pd.DataFrame(_ROWS, columns=["raw_value", "normalized_value", "requires_price"])
