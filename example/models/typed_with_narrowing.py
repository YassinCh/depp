"""Example using type narrowing helper for better IDE support."""

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from ...src.dbt.adapters.depp import PandasDbtObject, SessionObject


def model(dbt: "PandasDbtObject", session: "SessionObject"):
    products_df = dbt.ref("base_sql_model")
    print(isinstance(products_df, pl.DataFrame))

    return products_df
