from typing import Any, cast

from dbt.adapters.snowflake.connections import SnowflakeCredentials

from dbt.adapters.depp.db.snowflake import SnowflakeOps
from dbt.adapters.depp.executors.protocols import (
    Converter,
    DbContext,
    SourceInfo,
    geometry_columns,
)
from dbt.adapters.depp.executors.result import ExecutionResult


def cast_geography_columns(
    ops: SnowflakeOps,
    creds: SnowflakeCredentials,
    schema: str,
    table: str,
    geom_cols: list[str],
) -> None:
    """CTAS to convert VARCHAR geometry columns to GEOGRAPHY."""
    s = ops.format_identifier(schema)
    t = ops.format_identifier(table)
    with ops.get_connection(creds, schema) as conn:
        all_cols = ops.get_all_columns(conn, schema, table)
        geom_set = {c.lower() for c in geom_cols}
        select = ", ".join(
            f"TRY_TO_GEOGRAPHY({c.upper()}) AS {c.upper()}"
            if c in geom_set
            else c.upper()
            for c in all_cols
        )
        sql = f"CREATE OR REPLACE TABLE {s}.{t} AS SELECT {select} FROM {s}.{t}"
        conn.cursor().execute(sql)


class SnowflakeWriter:
    """Write DataFrames to Snowflake via write_pandas."""

    __slots__ = ("converter",)

    def __init__(self, converter: Converter[Any]) -> None:
        self.converter = converter

    def write(
        self, ctx: DbContext, df: Any, source: SourceInfo
    ) -> ExecutionResult:
        ops = cast(SnowflakeOps, ctx.ops)
        creds = cast(SnowflakeCredentials, ctx.creds)
        df = self.converter.prepare_array_columns(df, ctx.ops)[0]
        pdf = self.converter.to_pandas(df)
        rows = ops.write_from_pandas(creds, pdf, source.table, source.schema)
        return ExecutionResult(
            rows_affected=rows, schema=source.schema, table=source.table
        )


class SnowflakeGeoWriter:
    """Write GeoDataFrames to Snowflake with GEOGRAPHY casting."""

    __slots__ = ("converter",)

    def __init__(self, converter: Converter[Any]) -> None:
        self.converter = converter

    def write(
        self, ctx: DbContext, df: Any, source: SourceInfo
    ) -> ExecutionResult:
        ops = cast(SnowflakeOps, ctx.ops)
        creds = cast(SnowflakeCredentials, ctx.creds)
        geom_cols = geometry_columns(df)
        for col in geom_cols:
            df[col] = df[col].apply(
                lambda g: ops.geometry_to_db(g) if g else None
            )
        df = self.converter.prepare_array_columns(df, ctx.ops)[0]
        pdf = self.converter.to_pandas(df)
        rows = ops.write_from_pandas(creds, pdf, source.table, source.schema)
        if geom_cols:
            cast_geography_columns(
                ops,
                creds,
                source.schema,
                source.table,
                geom_cols,
            )
        return ExecutionResult(
            rows_affected=rows,
            schema=source.schema,
            table=source.table,
        )
