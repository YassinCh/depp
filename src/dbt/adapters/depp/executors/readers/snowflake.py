from typing import Any, cast

from dbt.adapters.snowflake.connections import SnowflakeCredentials

from dbt.adapters.depp.db.base import DEFAULT_SRID
from dbt.adapters.depp.db.snowflake import SnowflakeOps
from dbt.adapters.depp.executors.protocols import (
    DbContext,
    GeoArrowResult,
    SourceInfo,
)


class SnowflakeReader:
    """Read tables from Snowflake into Arrow via cursor."""

    __slots__ = ()

    def read_arrow(self, ctx: DbContext, source: SourceInfo) -> Any:
        ops = cast(SnowflakeOps, ctx.ops)
        creds = cast(SnowflakeCredentials, ctx.creds)
        query = ops.build_select_query(source.schema, source.table)
        return ops.read_arrow(creds, source.schema, query)


class SnowflakeGeoReader:
    """Read geo tables from Snowflake, returning GeoJSON geometry."""

    __slots__ = ()

    def read_arrow(self, ctx: DbContext, source: SourceInfo) -> GeoArrowResult:
        ops = cast(SnowflakeOps, ctx.ops)
        creds = cast(SnowflakeCredentials, ctx.creds)
        query = ops.build_select_query(source.schema, source.table)
        arrow = ops.read_arrow(creds, source.schema, query)
        with ops.get_connection(creds, source.schema) as conn:
            geom_cols = ops.get_geometry_columns(conn, source.schema, source.table)
        srid = next(iter(geom_cols.values()), DEFAULT_SRID)
        return GeoArrowResult(
            table=arrow, geometry_columns=list(geom_cols), srid=srid, format="geojson"
        )
