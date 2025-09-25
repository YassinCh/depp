from typing import TYPE_CHECKING

from dbt.adapters.base.plugin import AdapterPlugin

from .typing import (
    DataFrame,
    DataFrameT,
    DbtConfig,
    DbtObject,
    DbtThis,
    ModelFunction,
    PandasDataFrame,
    PandasDbt,
    PolarsDataFrame,
    PolarsDbt,
    SessionObject,
)

if TYPE_CHECKING:
    from .adapter import PythonAdapter
    from .config import DeppCredentials

__all__ = [
    "PythonAdapter",
    "DeppCredentials",
    "DataFrame",
    "PandasDataFrame",
    "PolarsDataFrame",
    "DataFrameT",
    "DbtConfig",
    "DbtObject",
    "PandasDbt",
    "PolarsDbt",
    "DbtThis",
    "ModelFunction",
    "SessionObject",
]


def __getattr__(name: str):
    # Lazy imports to avoid initialization at import time
    from ...include import depp  # type: ignore
    from .adapter import PythonAdapter
    from .config import DeppCredentials

    return AdapterPlugin(
        adapter=PythonAdapter,  # type: ignore
        credentials=DeppCredentials,
        include_path=depp.PACKAGE_PATH,
    )
