from typing import Any

import connectorx as cx
import geopandas as gpd
from geoalchemy2 import Geometry
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.dialects.postgresql import ARRAY

from .abstract_executor import AbstractPythonExecutor, SourceInfo
from .result import ExecutionResult


class GeoPandasLocalExecutor(AbstractPythonExecutor[gpd.GeoDataFrame]):
    library_name = "geopandas"
    handled_types = ["GeoPandasDbt"]
    srid: int = 28992

    def write_dataframe(
        self, df: gpd.GeoDataFrame, table: str, schema: str
    ) -> ExecutionResult:
        engine = create_engine(self.conn_string)
        df, dtype = self.prepare_array_columns(df)
        dtype_geometry = {
            col: Geometry("GEOMETRY", srid=self.srid)
            for col, dtype in df.dtypes.items()
            if dtype == "geometry"
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

    def read_df(self, table_name: str) -> gpd.GeoDataFrame:
        """Read PostGIS table."""
        # TODO: add support for providing geometry to reduce queries

        source = self.get_source_info(table_name)
        geom_cols = self._get_geometry_columns(source.schema, source.table)
        all_cols = self._get_all_columns(source)
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
        return gpd.GeoDataFrame(df, geometry=list(geom_cols)[0])

    def _get_geometry_columns(self, schema: str, table: str) -> dict[str, str]:
        query = f"""
            SELECT f_geometry_column as col_name, type as geom_type, srid
            FROM geometry_columns 
            WHERE f_table_schema = '{schema}' AND f_table_name = '{table}'
        """
        geom_df = cx.read_sql(self.conn_string, query)
        srid = int(geom_df["srid"].iloc[0])
        self.srid = srid if srid != 0 else 28992
        return dict(zip(geom_df["col_name"], geom_df["geom_type"]))

    def _get_all_columns(self, source: SourceInfo) -> list[str]:
        # TODO: Find out if this can cause deadlocks on information_schema
        cols_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE (table_schema || '.' || table_name = '{source.full_name}'
                   OR table_name = '{source.full_name}')
            ORDER BY ordinal_position
        """
        return cx.read_sql(self.conn_string, cols_query)["column_name"].tolist()

    def prepare_array_columns(
        self, df: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, dict[str, ARRAY[Any]]]:
        """Convert list columns to PostgreSQL format and return dtype mapping."""
        # TODO: Duplicate code and should be conslidated
        array_dtype = {}

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
