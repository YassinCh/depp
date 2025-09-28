"""Example using type narrowing helper for better IDE support."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...src.dbt.adapters.depp.typing import GeoPandasDbt, SessionObject


def model(dbt: "GeoPandasDbt", session: "SessionObject"):
    products_df = dbt.source("raw_data", "result_table")
    return products_df
