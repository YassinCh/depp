"""Example using type narrowing helper for better IDE support."""

from typing import TYPE_CHECKING

import geopandas as gpd

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import GeoPandasDbt, SessionObject


def model(dbt: "GeoPandasDbt", session: "SessionObject") -> gpd.GeoDataFrame:
    df = dbt.ref("typed_with_narrowing")
    df["test_column"] = [[1, 2, 3]] * len(df)
    return df
