from typing import Any, cast

import connectorx as cx
from dbt.adapters.postgres.connections import PostgresCredentials

from dbt.adapters.depp.db.base import DEFAULT_SRID
from dbt.adapters.depp.db.postgres import PostgresOps
from dbt.adapters.depp.executors.protocols import (
    DbContext,
    GeoArrowResult,
    SourceInfo,
)


class PostgresReader:
    """Read tables from PostgreSQL into Arrow via connectorx."""

    __slots__ = ()

    def read_arrow(self, ctx: DbContext, source: SourceInfo) -> Any:
        query = ctx.ops.build_select_query(source.schema, source.table)
        conn = ctx.ops.connection_string(ctx.creds)
        return cx.read_sql(conn, query, return_type="arrow", protocol="binary")


class PostgresGeoReader:
    """Read geo tables from PostgreSQL, extracting geometry as WKB."""

    __slots__ = ()

    def read_arrow(self, ctx: DbContext, source: SourceInfo) -> GeoArrowResult:
        ops = cast(PostgresOps, ctx.ops)
        creds = cast(PostgresCredentials, ctx.creds)
        with ops.get_connection(creds) as conn:
            geom_cols = ops.get_geometry_columns(conn, source.schema, source.table)
            all_cols = ops.get_all_columns(conn, source.schema, source.table)
        srid = DEFAULT_SRID
        if geom_cols:
            srid = next(iter(geom_cols.values()))
        regular = [c for c in all_cols if c not in geom_cols]
        geom_names = list(geom_cols.keys())
        parts = regular + [f"ST_AsBinary({c}) as {c}_wkb" for c in geom_names]
        query = f'SELECT {", ".join(parts)} FROM "{source.schema}"."{source.table}"'
        conn_str = ctx.ops.connection_string(ctx.creds)
        arrow = cx.read_sql(conn_str, query, return_type="arrow", protocol="binary")
        return GeoArrowResult(
            table=arrow, geometry_columns=geom_names, srid=srid, format="wkb"
        )
