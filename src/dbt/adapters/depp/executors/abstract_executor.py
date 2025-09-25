import asyncio
import io
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import asyncpg
import connectorx as cx
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.postgres.connections import PostgresCredentials

DataFrameType = TypeVar("DataFrameType")


class AbstractPythonExecutor(ABC, Generic[DataFrameType]):
    registry: dict[str, type["AbstractPythonExecutor[Any]"]] = {}
    library_name: str | None = None

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        if cls.library_name is not None:
            AbstractPythonExecutor.registry[cls.library_name] = cls

    def __init__(
        self,
        parsed_model: dict[str, Any],
        credential: Credentials,
        library: str = "polars",
    ):
        if not isinstance(credential, PostgresCredentials):
            raise ValueError("Currently just postgres is supported")
        if library not in self.registry:
            raise ValueError(f"Only {', '.join(self.registry)} supported")

        self.parsed_model = parsed_model
        self.db_creds = credential
        self.library = library
        self.conn_string = f"postgresql://{self.db_creds.user}:{self.db_creds.password}@{self.db_creds.host}:{self.db_creds.port}/{self.db_creds.database}"

    @staticmethod
    def get_select(table_name: str):
        _, schema, table = table_name.replace('"', "").split(".")
        return f'SELECT * FROM "{schema}"."{table}"'

    def read_df(self, table_name: str) -> DataFrameType:
        return cx.read_sql(  # type: ignore
            self.conn_string,
            self.get_select(table_name),
            return_type=self.library,
            protocol="binary",
        )  # type: ignore

    def write_df(self, table_name: str, dataframe: DataFrameType):
        asyncio.run(self.async_write_df(table_name, dataframe))

    @abstractmethod
    def get_csv(self, df: DataFrameType) -> str:
        raise NotImplementedError

    async def async_write_df(self, table_name: str, df: DataFrameType):
        parts = table_name.replace('"', "").split(".")
        schema, table = parts[-2], parts[-1]
        conn = await asyncpg.connect(dsn=self.conn_string)

        csv_buffer = io.BytesIO()
        csv_string = self.get_csv(df)
        csv_buffer.write(csv_string.encode("utf-8"))
        csv_buffer.seek(0)

        await conn.copy_to_table(
            table_name=table,
            schema_name=schema,
            source=csv_buffer,
            format="csv",
            delimiter="\t",
            null="\\N",
        )

    def submit(self, compiled_code: str) -> str:
        local_vars: dict[str, Any] = {}
        exec(compiled_code, local_vars)

        if "main" not in local_vars:
            raise RuntimeError("No main function found in compiled code")

        result = local_vars["main"](self.read_df, self.write_df)
        return result
