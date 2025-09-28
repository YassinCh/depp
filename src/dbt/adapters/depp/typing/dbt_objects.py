"""Type hints for dbt Python models to improve developer experience."""

from typing import Generic, Literal, Optional, Protocol, TypeVar, Union

import geopandas as gpd
import pandas as pd
import polars as pl

from .base import DbtConfig, DbtThis

PolarsDataFrame = Union[pl.DataFrame, pl.LazyFrame]
DataFrame = Union[pd.DataFrame, PolarsDataFrame, gpd.GeoDataFrame]

DataFrameT = TypeVar("DataFrameT", bound=DataFrame, covariant=True)


class DbtObject(Protocol, Generic[DataFrameT]):
    """
    Dbt Object type providing method to reference other models and sources
    """

    def ref(
        self,
        model_name: str,
        *additional_names: str,
        version: Optional[Union[str, int]] = None,
        v: Optional[Union[str, int]] = None,
    ) -> DataFrameT:
        """Reference another model in the dbt project.

        Args:
            model_name: Name of the model to reference
            *additional_names: Additional parts of the model name (for two-part names)
            version: Model version (alternative to 'v')
            v: Model version (short form)

        Returns:
            A dataFrame containing the referenced model's data.
        """
        ...

    def source(self, source_name: str, table_name: str) -> DataFrameT:
        """Reference a source table.

        Args:
            source_name: Name of the source
            table_name: Name of the table within the source

        Returns:
            A DataFrame containing the source table's data.
        """
        ...

    def config(
        self, library: Literal["polars"] | Literal["pandas"] | Literal["geopandas"]
    ) -> DbtConfig:
        """Access model configuration."""
        ...

    @property
    def this(self) -> DbtThis:
        """Reference to the current model."""
        ...

    def is_incremental(self) -> bool:
        """Check if this is an incremental model run."""
        ...
