import io
import re
import time

import connectorx as cx
import geopandas as gpd
import numpy as np
from geoalchemy2 import Geometry
from sqlalchemy import create_engine

from .abstract_executor import AbstractPythonExecutor, SourceInfo


class GeoPandasLocalExecutor(AbstractPythonExecutor[gpd.GeoDataFrame]):
    library_name = "geopandas"

    @staticmethod
    def detect_geometry_column(value: str) -> bool:
        """Check if a string value looks like WKT geometry."""
        if not value or not value.strip():
            return False
        return bool(
            re.match(
                r"^(POINT|LINESTRING|POLYGON|MULTI|GEOMETRY)", value.strip().upper()
            )
        )

    def prepare_bulk_write(self, df: gpd.GeoDataFrame, table: str, schema: str) -> str:
        start = time.perf_counter()

        engine = create_engine(self.conn_string)
        dtype_mapping = {}
        for col, dtype in df.dtypes.items():
            if dtype == "geometry":
                dtype_mapping[col] = Geometry("GEOMETRY", srid=4326)

        df.head(1).to_postgis(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            dtype=dtype_mapping,
        )

        # Convert geometry columns to WKT format
        df_copy = df.copy()
        for col in df_copy.columns:
            if df_copy[col].dtype == "geometry":
                df_copy[col] = df_copy[col].apply(
                    lambda geom: geom.wkt if geom else "\\N"
                )

        output = io.StringIO()
        np_array = df_copy.to_numpy()
        np.savetxt(output, np_array, delimiter="\t", fmt="%s", newline="\n")

        end = time.perf_counter()
        print(f"Numpy write took {end - start} seconds")

        return output.getvalue()

    def _get_geometry_columns(
        self, schema: str, table: str
    ) -> tuple[dict[str, str], int]:
        query = f"""
            SELECT f_geometry_column as col_name, type as geom_type, srid
            FROM geometry_columns 
            WHERE f_table_schema = '{schema}' AND f_table_name = '{table}'
        """
        geom_df = cx.read_sql(self.conn_string, query)  # type: ignore
        srid = int(geom_df["srid"].iloc[0])
        return dict(zip(geom_df["col_name"], geom_df["geom_type"])), srid

    def _get_all_columns(self, source: SourceInfo):
        # TODO: Find out if this can cause deadlocks on information_schema
        print(source.full_name)
        cols_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE (table_schema || '.' || table_name = '{source.full_name}'
                   OR table_name = '{source.full_name}')
            ORDER BY ordinal_position
        """
        return cx.read_sql(self.conn_string, cols_query)["column_name"]  # type: ignore

    def read_df(self, table_name: str) -> gpd.GeoDataFrame:
        """Read PostGIS table."""
        # TODO: add support for providing geometry to reduce queries
        source = self.get_source_info(table_name)
        geom_cols, srid = self._get_geometry_columns(source.schema, source.table)
        all_cols = self._get_all_columns(source)
        regular_cols = [c for c in all_cols if c not in geom_cols]
        select_parts = regular_cols + [
            f"ST_AsBinary({c}) as {c}_wkb" for c in geom_cols
        ]
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        df = cx.read_sql(self.conn_string, query, protocol="binary")  # type: ignore
        wkb_cols = [f"{col}_wkb" for col in geom_cols]
        for col in geom_cols:
            df[col] = gpd.GeoSeries.from_wkb(df[f"{col}_wkb"])
        df = df.drop(columns=wkb_cols)
        crs = f"EPSG:{srid}" if srid else None
        return gpd.GeoDataFrame(df, geometry=list(geom_cols)[0], crs=crs)
