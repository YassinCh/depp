"""Example using type narrowing helper for better IDE support."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...src.dbt.adapters.depp import PolarsDbt, SessionObject


def model(dbt: "PolarsDbt", session: "SessionObject"):
    products_df = dbt.ref("base_sql_model")
    return products_df
