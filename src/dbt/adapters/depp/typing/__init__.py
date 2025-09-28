import geopandas as gpd  # type: ignore
import pandas as pd

from .base import DbtConfig, DbtThis, SessionObject
from .dbt_objects import DbtObject, PolarsDataFrame

PandasDbt = DbtObject[pd.DataFrame]
"""Type hint for pandas-based dbt models."""

GeoPandasDbt = DbtObject[gpd.GeoDataFrame]
"""Type hint for GeoPandas-based dbt models."""

PolarsDbt = DbtObject[PolarsDataFrame]
"""Type hint for Polars-based dbt models (DataFrame or LazyFrame)."""


__all__ = [
    "PolarsDataFrame",
    "DbtThis",
    "DbtConfig",
    "DbtObject",
    "PandasDbt",
    "PolarsDbt",
    "GeoPandasDbt",
    "SessionObject",
]
