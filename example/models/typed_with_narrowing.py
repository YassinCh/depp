"""Example using type narrowing helper for better IDE support."""

from typing import TYPE_CHECKING

import geopandas as gpd

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import GeoPandasDbt, SessionObject


def model(dbt: "GeoPandasDbt", session: "SessionObject") -> gpd.GeoDataFrame:
    dbt.config(constraints=[{"columns": ["id"], "type": "primary_key"}])

    df = dbt.source("raw_data", "result_table")
    df["test_column"] = [[1, 2, 3]] * len(df)
    return df
