from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import connectorx as cx
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.postgres.connections import PostgresCredentials

from .result import ExecutionResult


@dataclass
class SourceInfo:
    full_name: str
    schema: str
    table: str


class AbstractPythonExecutor[DataFrameType](ABC):
    """
    Base executor for running Python models in dbt with different DataFrame libraries.
    Subclasses must set `library_name` and implement `write_dataframe`.
    Auto-registers implementations to a central registry.
    """

    registry: dict[str, type["AbstractPythonExecutor[Any]"]] = {}
    type_mapping: dict[str, str] = {}
    library_name: str | None = None
    handled_types: list[str] = []

    def __init_subclass__(cls, **kwargs: Any):
        """Automatically registers the executor to a registry"""
        super().__init_subclass__(**kwargs)
        if cls.library_name is not None:
            AbstractPythonExecutor.registry[cls.library_name] = cls
            for type_hint in getattr(cls, "handled_types", []):
                AbstractPythonExecutor.type_mapping[type_hint] = cls.library_name

    def __init__(self, parsed: dict[str, Any], db: Credentials, lib: str = "polars"):
        if not isinstance(db, PostgresCredentials):
            raise ValueError("Currently just postgres is supported")
        if lib not in self.registry:
            raise ValueError(f"Only {', '.join(self.registry)} supported")

        self.parsed_model = parsed
        self.library = lib
        self.conn_string = self.get_connection_string(db)

    def read_df(self, table_name: str) -> DataFrameType:
        # TODO: Support filtering
        """Reads all data from the table using connectorx"""
        source = self.get_source_info(table_name)
        query = f'SELECT * FROM "{source.schema}"."{source.table}"'

        result = cx.read_sql(
            self.conn_string, query, return_type=self.library, protocol="binary"
        )
        return result  # type: ignore

    def write_df(self, table_name: str, dataframe: DataFrameType) -> ExecutionResult:
        """Write DataFrame to database table using async bulk copy."""
        source = self.get_source_info(table_name)
        return self.write_dataframe(dataframe, source.table, source.schema)

    @abstractmethod
    def write_dataframe(
        self, df: DataFrameType, table: str, schema: str
    ) -> ExecutionResult:
        """Write DataFrame to PostgreSQL."""
        raise NotImplementedError

    def submit(self, compiled_code: str) -> ExecutionResult:
        """Execute compiled dbt Python model code."""
        local_vars: dict[str, Any] = {}
        exec(compiled_code, local_vars)
        if "main" not in local_vars:
            raise RuntimeError("No main function found in compiled code")
        result = local_vars["main"](self.read_df, self.write_df)
        return ExecutionResult(**result) if isinstance(result, dict) else result

    @staticmethod
    def get_source_info(table_name: str) -> SourceInfo:
        clean_name = table_name.replace('"', "")
        _, schema, table = clean_name.split(".")
        return SourceInfo(f"{schema}.{table}", schema, table)

    @staticmethod
    def get_connection_string(db_creds: PostgresCredentials) -> str:
        # TODO: support more database types
        """Build PostgreSQL connection string from credentials."""
        return f"postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}"

    @classmethod
    def get_library_for_type(cls, type_hint: str) -> str | None:
        """Get the library name for a given type hint."""
        return cls.type_mapping.get(type_hint)
