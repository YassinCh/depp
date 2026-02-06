import contextlib
import time
from typing import Any

import connectorx as cx
import geopandas as gpd
import pandas as pd
from geoalchemy2 import Geometry
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.dialects.postgresql import ARRAY

from .abstract_executor import AbstractPythonExecutor, SourceInfo
from .result import ExecutionResult


class GeoPandasLocalExecutor(AbstractPythonExecutor[gpd.GeoDataFrame]):
    library_name = "geopandas"
    handled_types = ["GeoPandasDbt"]
    srid: int = 28992

    # --- WRITE ---

    def write_dataframe(
        self, df: gpd.GeoDataFrame, table: str, schema: str
    ) -> ExecutionResult:
        if self.db_type == "snowflake":
            return self._write_snowflake(df, table, schema)
        return self._write_postgres(df, table, schema)

    def _write_postgres(
        self, df: gpd.GeoDataFrame, table: str, schema: str
    ) -> ExecutionResult:
        engine = create_engine(self.conn_string)
        df, dtype = self._prepare_pg_array_columns(df)
        dtype_geometry = {
            col: Geometry("GEOMETRY", srid=self.srid)
            for col, d in df.dtypes.items()
            if d == "geometry"
        } | dtype

        df.to_postgis(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            dtype=dtype_geometry,
        )
        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def _write_snowflake(
        self, df: gpd.GeoDataFrame, table: str, schema: str
    ) -> ExecutionResult:
        """Write GeoDataFrame to Snowflake: geometry as WKT, then cast to GEOGRAPHY."""
        from snowflake.connector.pandas_tools import write_pandas  # type: ignore[import-untyped]

        geom_cols = [col for col, d in df.dtypes.items() if d == "geometry"]
        write_df = pd.DataFrame(df.drop(columns=geom_cols))
        for col in geom_cols:
            write_df[col] = df[col].to_wkt()

        conn = self._get_snowflake_connection()
        try:
            write_pandas(
                conn,
                write_df,
                table_name=table.upper(),
                schema=schema.upper(),
                auto_create_table=True,
                overwrite=True,
            )
            self._cast_snowflake_geometry_columns(conn, schema, table, geom_cols)
        finally:
            conn.close()
        return ExecutionResult(rows_affected=len(df), schema=schema, table=table)

    def _cast_snowflake_geometry_columns(
        self, conn: Any, schema: str, table: str, geom_cols: list[str]
    ) -> None:
        """Cast WKT string columns to Snowflake GEOGRAPHY type."""
        cursor = conn.cursor()
        fqn = f'"{schema.upper()}"."{table.upper()}"'
        for col in geom_cols:
            col_upper = col.upper()
            tmp = f"{col_upper}_GEOG_TMP"
            cursor.execute(
                f"ALTER TABLE {fqn} ADD COLUMN {tmp} GEOGRAPHY"
            )
            cursor.execute(
                f"UPDATE {fqn} SET {tmp} = TRY_TO_GEOGRAPHY({col_upper})"
            )
            cursor.execute(f"ALTER TABLE {fqn} DROP COLUMN {col_upper}")
            cursor.execute(
                f"ALTER TABLE {fqn} RENAME COLUMN {tmp} TO {col_upper}"
            )

    # --- READ ---

    def read_df(self, table_name: str) -> gpd.GeoDataFrame:
        """Read geospatial table from Postgres (PostGIS) or Snowflake."""
        if self.db_type == "snowflake":
            return self._read_snowflake(table_name)
        return self._read_postgres(table_name)

    def _read_postgres(self, table_name: str) -> gpd.GeoDataFrame:
        """Read PostGIS table via connectorx + WKB conversion."""
        start = time.perf_counter()
        source = self.get_source_info(table_name)
        geom_cols = self._get_pg_geometry_columns(source.schema, source.table)
        all_cols = self._get_all_columns_pg(source)
        regular_cols = [c for c in all_cols if c not in geom_cols]
        select_parts = regular_cols + [
            f"ST_AsBinary({c}) as {c}_wkb" for c in geom_cols
        ]

        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        df = cx.read_sql(self.conn_string, query, protocol="binary")

        wkb_cols = [f"{col}_wkb" for col in geom_cols]
        for col in geom_cols:
            df[col] = gpd.GeoSeries.from_wkb(df[f"{col}_wkb"], crs=self.srid)
        df = df.drop(columns=wkb_cols)
        self._read_time += time.perf_counter() - start
        return gpd.GeoDataFrame(df, geometry=list(geom_cols)[0])

    def _read_snowflake(self, table_name: str) -> gpd.GeoDataFrame:
        """Read Snowflake GEOGRAPHY columns via ST_ASWKB."""
        start = time.perf_counter()
        source = self.get_source_info(table_name)
        geom_cols = self._get_sf_geometry_columns(source.schema, source.table)

        if not geom_cols:
            conn = self._get_snowflake_connection()
            try:
                df = conn.cursor().execute(
                    f'SELECT * FROM "{source.schema}"."{source.table}"'
                ).fetch_pandas_all()
            finally:
                conn.close()
            self._read_time += time.perf_counter() - start
            return gpd.GeoDataFrame(df)

        all_cols = self._get_all_columns_sf(source)
        regular_cols = [c for c in all_cols if c not in geom_cols]
        select_parts = regular_cols + [
            f"ST_ASWKB({c}) as {c}_wkb" for c in geom_cols
        ]

        conn = self._get_snowflake_connection()
        try:
            query = f"SELECT {', '.join(select_parts)} FROM \"{source.schema}\".\"{source.table}\""
            df = conn.cursor().execute(query).fetch_pandas_all()
        finally:
            conn.close()

        for col in geom_cols:
            df[col] = gpd.GeoSeries.from_wkb(df[f"{col}_wkb"], crs=4326)
        df = df.drop(columns=[f"{col}_wkb" for col in geom_cols])
        self._read_time += time.perf_counter() - start
        return gpd.GeoDataFrame(df, geometry=list(geom_cols)[0])

    # --- GEOMETRY COLUMN DISCOVERY ---

    def _get_pg_geometry_columns(self, schema: str, table: str) -> dict[str, str]:
        """Discover geometry columns from PostGIS metadata."""
        query = f"""
            SELECT f_geometry_column as col_name, type as geom_type, srid
            FROM geometry_columns
            WHERE f_table_schema = '{schema}' AND f_table_name = '{table}'
        """
        geom_df = cx.read_sql(self.conn_string, query)
        srid = 28992
        with contextlib.suppress(Exception):
            srid = int(geom_df["srid"].iloc[0])
        self.srid = srid if srid != 0 else 28992
        return dict(zip(geom_df["col_name"], geom_df["geom_type"]))

    def _get_sf_geometry_columns(self, schema: str, table: str) -> dict[str, str]:
        """Discover GEOGRAPHY/GEOMETRY columns from Snowflake information_schema."""
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema.upper()}'
              AND TABLE_NAME = '{table.upper()}'
              AND DATA_TYPE IN ('GEOGRAPHY', 'GEOMETRY')
            ORDER BY ORDINAL_POSITION
        """
        conn = self._get_snowflake_connection()
        try:
            result = conn.cursor().execute(query).fetchall()
        finally:
            conn.close()
        return {row[0]: row[1] for row in result}

    # --- COLUMN LISTING ---

    def _get_all_columns_pg(self, source: SourceInfo) -> list[str]:
        cols_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE (table_schema || '.' || table_name = '{source.full_name}'
                   OR table_name = '{source.full_name}')
            ORDER BY ordinal_position
        """
        return cx.read_sql(self.conn_string, cols_query)["column_name"].tolist()

    def _get_all_columns_sf(self, source: SourceInfo) -> list[str]:
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{source.schema.upper()}'
              AND TABLE_NAME = '{source.table.upper()}'
            ORDER BY ORDINAL_POSITION
        """
        conn = self._get_snowflake_connection()
        try:
            result = conn.cursor().execute(query).fetchall()
        finally:
            conn.close()
        return [row[0] for row in result]

    # --- HELPERS ---

    def _get_snowflake_connection(self) -> Any:
        """Create a Snowflake connection from stored credentials."""
        import snowflake.connector  # type: ignore[import-untyped]

        return snowflake.connector.connect(
            user=getattr(self.creds, "user", ""),
            password=getattr(self.creds, "password", ""),
            account=getattr(self.creds, "account", ""),
            database=self.creds.database,
            schema=self.creds.schema,
            warehouse=getattr(self.creds, "warehouse", ""),
        )

    def _prepare_pg_array_columns(
        self, df: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, dict[str, ARRAY[Any]]]:
        """Convert list columns to PostgreSQL ARRAY format and return dtype mapping."""
        array_dtype: dict[str, ARRAY[Any]] = {}

        for col in df.select_dtypes("object").columns:
            if not df[col].apply(isinstance, args=(list,)).any():
                continue

            sample = df[col].dropna().iloc[0] if df[col].notna().any() else None
            df[col] = df[col].apply(
                lambda x: f"{{{','.join(map(str, x))}}}" if x else None
            )
            array_dtype[col] = (
                ARRAY(Integer)
                if sample and all(isinstance(v, int) for v in sample)
                else ARRAY(String)  # type: ignore
            )

        return df, array_dtype
