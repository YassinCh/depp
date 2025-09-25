"""Type hints for dbt Python models to improve developer experience."""

from typing import Any, Callable, Literal, Optional, Protocol, TypeVar, Union, overload

import pandas as pd
import polars as pl

PandasDataFrame = pd.DataFrame
PolarsDataFrame = Union[pl.DataFrame, pl.LazyFrame]
DataFrame = Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]

DataFrameT = TypeVar("DataFrameT", pd.DataFrame, pl.DataFrame, pl.LazyFrame)


class DbtThis:
    """Represents the current model in dbt."""

    database: str
    schema: str
    identifier: str

    def __repr__(self) -> str:
        """Return the fully qualified table name."""
        return f'"{self.database}"."{self.schema}"."{self.identifier}"'


class DbtConfig:
    """Configuration object for dbt models.

    After calling config(library="pandas"), ref() will return pd.DataFrame.
    After calling config(library="polars"), ref() will return pl.DataFrame or pl.LazyFrame.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize configuration."""
        ...

    @staticmethod
    def get(key: str, default: Optional[Any] = None) -> Any:
        """Get configuration value by key."""
        ...

    def __call__(self, **kwargs: Any) -> None:
        """Set configuration options.

        Args:
            library: DataFrame library to use ("pandas" or "polars")
            **kwargs: Additional configuration options
        """
        ...


class DbtObject(Protocol):
    """Type hints for the dbt object passed to Python models.

    The DataFrame type returned by ref() and source() depends on the library
    configured via dbt.config(library="pandas" or "polars").

    For the best type hints experience:
    1. Call dbt.config(library="pandas" or "polars") at the start of your model
    2. Use type annotations when assigning:
       - customers: pd.DataFrame = dbt.ref("customers")  # for pandas
       - customers: pl.DataFrame = dbt.ref("customers")  # for polars
    """

    def ref(
        self,
        model_name: str,
        *additional_names: str,
        version: Optional[Union[str, int]] = None,
        v: Optional[Union[str, int]] = None,
    ) -> DataFrame:
        """Reference another model in the dbt project.

        Args:
            model_name: Name of the model to reference
            *additional_names: Additional parts of the model name (for two-part names)
            version: Model version (alternative to 'v')
            v: Model version (short form)

        Returns:
            DataFrame containing the referenced model's data.
            - Returns pd.DataFrame when dbt.config(library="pandas")
            - Returns pl.DataFrame or pl.LazyFrame when dbt.config(library="polars")
        """
        ...

    def source(self, source_name: str, table_name: str) -> DataFrame:
        """Reference a source table.

        Args:
            source_name: Name of the source
            table_name: Name of the table within the source

        Returns:
            DataFrame containing the source table's data.
            - Returns pd.DataFrame when dbt.config(library="pandas")
            - Returns pl.DataFrame or pl.LazyFrame when dbt.config(library="polars")
        """
        ...

    def config(self, library: Literal["polars"] | Literal["pandas"]) -> DbtConfig:
        """Access model configuration."""
        ...

    @property
    def this(self) -> DbtThis:
        """Reference to the current model."""
        ...

    def is_incremental(self) -> bool:
        """Check if this is an incremental model run."""
        ...


class PandasDbtObject(DbtObject, Protocol):
    """DbtObject specifically for pandas models.

    When you use this type hint, the library is automatically configured to "pandas":
    def model(dbt: PandasDbtObject, session: SessionObject) -> pd.DataFrame:
        # No need to call dbt.config(library="pandas") - it's automatic!
        customers = dbt.ref("customers")  # IDE knows this is pd.DataFrame
    """

    def ref(
        self,
        model_name: str,
        *additional_names: str,
        version: Optional[Union[str, int]] = None,
        v: Optional[Union[str, int]] = None,
    ) -> pd.DataFrame:
        """Reference returns pd.DataFrame when using pandas."""
        ...

    def source(self, source_name: str, table_name: str) -> pd.DataFrame:
        """Source returns pd.DataFrame when using pandas."""
        ...


class PolarsDbtObject(DbtObject, Protocol):
    """DbtObject specifically for polars models.

    When you use this type hint, the library is automatically configured to "polars":
    def model(dbt: PolarsDbtObject, session: SessionObject) -> pl.DataFrame:
        # No need to call dbt.config(library="polars") - it's automatic!
        customers = dbt.ref("customers")  # IDE knows this is pl.DataFrame/LazyFrame
    """

    def ref(
        self,
        model_name: str,
        *additional_names: str,
        version: Optional[Union[str, int]] = None,
        v: Optional[Union[str, int]] = None,
    ) -> pl.DataFrame:
        """Reference returns pl.DataFrame or pl.LazyFrame when using polars."""
        ...

    def source(
        self, source_name: str, table_name: str
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Source returns pl.DataFrame or pl.LazyFrame when using polars."""
        ...


class SessionObject(Protocol):
    """Type hints for the session object passed to Python models.

    The session object provides database connectivity for Python models.
    In most cases with this adapter, it represents a SQLAlchemy session
    or similar database connection object.
    """

    def execute(self, query: str) -> Any:
        """Execute a SQL query.

        Args:
            query: SQL query string to execute

        Returns:
            Query result
        """
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    def close(self) -> None:
        """Close the session."""
        ...


# Model function overloads for different library configurations
@overload
def model(dbt: PandasDbtObject, session: SessionObject) -> pd.DataFrame:
    """Model function when configured for pandas."""
    ...


@overload
def model(
    dbt: PolarsDbtObject, session: SessionObject
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Model function when configured for polars."""
    ...


@overload
def model(dbt: DbtObject, session: SessionObject) -> DataFrame:
    """Generic model function signature."""
    ...


def model(dbt: DbtObject, session: SessionObject) -> DataFrame:
    """Type stub for dbt Python model function.

    This is the standard signature for a dbt Python model.

    Args:
        dbt: The dbt object providing access to ref(), source(), config, etc.
        session: Database session object for direct database operations

    Returns:
        DataFrame to be materialized as the model result
    """
    raise NotImplementedError("This is a type stub - implement in your model file")


type ModelFunction = Callable[[DbtObject, SessionObject], DataFrame]

__all__ = [
    "DataFrame",
    "PandasDataFrame",
    "PolarsDataFrame",
    "DataFrameT",
    "DbtThis",
    "DbtConfig",
    "DbtObject",
    "PandasDbtObject",
    "PolarsDbtObject",
    "SessionObject",
    "ModelFunction",
    "model",
]
