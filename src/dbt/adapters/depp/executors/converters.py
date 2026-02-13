from typing import Any, cast

import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
import shapely

from dbt.adapters.depp.db import DatabaseOps

from .protocols import GeoArrowResult


def is_array_like(x: object) -> bool:
    """Check if a value is a list or numpy array."""
    return isinstance(x, (list, np.ndarray))


def detect_pandas_array_columns(
    df: pd.DataFrame, db_ops: DatabaseOps
) -> tuple[pd.DataFrame, dict[str, bool]]:
    """Detect object-dtype columns containing lists and format them."""
    array_cols: dict[str, bool] = {}
    for col in df.select_dtypes("object").columns:
        if not df[col].apply(is_array_like).any():
            continue
        sample = df[col].dropna().iloc[0] if df[col].notna().any() else None
        is_int = sample is not None and all(isinstance(v, int) for v in sample)
        df[col] = df[col].apply(
            lambda x, ops=db_ops: (
                ops.format_array(list(x)) if is_array_like(x) else None
            )
        )
        array_cols[str(col)] = is_int
    return df, array_cols


class PolarsConverter:
    """Convert between Arrow and Polars DataFrames."""

    __slots__ = ()

    def from_arrow(self, arrow: Any) -> pl.DataFrame:
        """Convert an Arrow table to a Polars DataFrame."""
        return cast(pl.DataFrame, pl.from_arrow(arrow))

    def prepare_array_columns(
        self, df: pl.DataFrame, db_ops: DatabaseOps
    ) -> tuple[pl.DataFrame, dict[str, bool]]:
        """Detect and format list columns for database insertion."""
        array_cols: dict[str, bool] = {}
        for col in df.columns:
            if not isinstance(df[col].dtype, pl.List):
                continue
            values = df[col].to_list()
            sample = next((v for v in values if v is not None), None)
            df = df.with_columns(
                pl.Series(
                    col,
                    [db_ops.format_array(v) if v is not None else None for v in values],
                )
            )
            array_cols[col] = sample is not None and all(
                isinstance(x, int) for x in sample
            )
        return df, array_cols

    def to_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        """Return the DataFrame as-is (already Polars)."""
        return df

    def to_pandas(self, df: pl.DataFrame) -> pd.DataFrame:
        """Convert a Polars DataFrame to Pandas."""
        return df.to_pandas()


class PandasConverter:
    """Convert between Arrow and Pandas DataFrames."""

    __slots__ = ()

    def from_arrow(self, arrow: Any) -> pd.DataFrame:
        """Convert an Arrow table to a Pandas DataFrame."""
        return cast(pl.DataFrame, pl.from_arrow(arrow)).to_pandas()

    def prepare_array_columns(
        self, df: pd.DataFrame, db_ops: DatabaseOps
    ) -> tuple[pd.DataFrame, dict[str, bool]]:
        """Detect and format list columns for database insertion."""
        return detect_pandas_array_columns(df, db_ops)

    def to_polars(self, df: pd.DataFrame) -> pl.DataFrame:
        """Convert a Pandas DataFrame to Polars."""
        return pl.from_pandas(df)

    def to_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return the DataFrame as-is (already Pandas)."""
        return df


class GeoPandasConverter:
    """Convert between Arrow/GeoArrowResult and GeoDataFrames."""

    __slots__ = ()

    def from_arrow(self, arrow: Any) -> Any:
        """Convert a GeoArrowResult to a GeoDataFrame."""
        result = cast(GeoArrowResult, arrow)
        df: pd.DataFrame = result.table.to_pandas()
        if not result.geometry_columns:
            return gpd.GeoDataFrame(df)
        if result.format == "wkb":
            for col in result.geometry_columns:
                df[col] = gpd.GeoSeries.from_wkb(df[f"{col}_wkb"], crs=result.srid)
            df = df.drop(columns=[f"{col}_wkb" for col in result.geometry_columns])
        elif result.format == "geojson":
            for col in result.geometry_columns:
                df[col] = df[col].apply(
                    lambda x: (shapely.from_geojson(x) if x else None)
                )
                df[col] = gpd.GeoSeries(df[col], crs=result.srid)
        return gpd.GeoDataFrame(df, geometry=result.geometry_columns[0])

    def prepare_array_columns(
        self, df: Any, db_ops: DatabaseOps
    ) -> tuple[Any, dict[str, bool]]:
        """Detect and format list columns for database insertion."""
        return detect_pandas_array_columns(df, db_ops)

    def to_polars(self, df: Any) -> pl.DataFrame:
        """Convert a GeoDataFrame to Polars via Pandas."""
        return pl.from_pandas(pd.DataFrame(df))

    def to_pandas(self, df: Any) -> pd.DataFrame:
        """Convert a GeoDataFrame to a plain Pandas DataFrame."""
        return pd.DataFrame(df)
