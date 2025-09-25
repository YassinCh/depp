from abc import ABC, abstractmethod
from typing import Any, Generic, Literal, TypeVar, overload

import pandas as pd
import polars as pl
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.postgres.connections import PostgresCredentials
from sqlalchemy import create_engine

DataFrameType = TypeVar("DataFrameType", pd.DataFrame, pl.DataFrame)


class AbstractPythonExecutor(ABC, Generic[DataFrameType]):
    @overload
    def __init__(
        self: "AbstractPythonExecutor[pl.DataFrame]",
        parsed_model: dict[str, Any],
        credential: Credentials,
        library: Literal["polars"],
    ) -> None: ...

    @overload
    def __init__(
        self: "AbstractPythonExecutor[pd.DataFrame]",
        parsed_model: dict[str, Any],
        credential: Credentials,
        library: Literal["pandas"],
    ) -> None: ...

    def __init__(
        self,
        parsed_model: dict[str, Any],
        credential: Credentials,
        library: Literal["pandas", "polars"] = "polars",
    ):
        if not isinstance(credential, PostgresCredentials):
            raise ValueError("Currently just postgres is supported")
        if library not in ("pandas", "polars"):
            raise ValueError("Only 'pandas' or 'polars' supported")

        self.parsed_model = parsed_model
        self.db_creds = credential
        self.library = library
        conn_string = f"postgresql://{self.db_creds.user}:{self.db_creds.password}@{self.db_creds.host}:{self.db_creds.port}/{self.db_creds.database}"
        self.engine = create_engine(conn_string)

    @abstractmethod
    def read_df(self, table_name: str) -> DataFrameType:
        raise NotImplementedError

    @abstractmethod
    def write_df(self, table_name: str, dataframe: DataFrameType) -> None:
        raise NotImplementedError

    def submit(self, compiled_code: str) -> str:
        local_vars: dict[str, Any] = {}
        exec(compiled_code, local_vars)

        if "main" in local_vars:
            result = local_vars["main"](self.read_df, self.write_df)
            return result
        else:
            raise RuntimeError("No main function found in compiled code")
