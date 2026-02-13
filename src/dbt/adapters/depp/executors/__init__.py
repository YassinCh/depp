import contextlib
from importlib import import_module

from .converters import GeoPandasConverter, PandasConverter, PolarsConverter
from .executor import Executor, ExecutorConfig
from .readers.postgres import PostgresGeoReader, PostgresReader
from .result import ExecutionResult
from .writers.postgres import PostgresGeoWriter, PostgresWriter

polars_conv = PolarsConverter()
pandas_conv = PandasConverter()
geo_conv = GeoPandasConverter()

pg_reader = PostgresReader()
pg_geo_reader = PostgresGeoReader()
pg_geo_writer = PostgresGeoWriter()

Executor.register(
    "polars", "postgres", ["PolarsDbt"],
    ExecutorConfig(pg_reader, PostgresWriter(polars_conv), polars_conv),
)
Executor.register(
    "pandas", "postgres", ["PandasDbt"],
    ExecutorConfig(pg_reader, PostgresWriter(pandas_conv), pandas_conv),
)
Executor.register(
    "geopandas", "postgres", ["GeoPandasDbt"],
    ExecutorConfig(pg_geo_reader, pg_geo_writer, geo_conv),
)


def _register_snowflake() -> None:
    sf_readers = import_module(".readers.snowflake", __package__)
    sf_writers = import_module(".writers.snowflake", __package__)

    sf_reader = sf_readers.SnowflakeReader()
    sf_geo_reader = sf_readers.SnowflakeGeoReader()

    Executor.register(
        "polars", "snowflake", [],
        ExecutorConfig(sf_reader, sf_writers.SnowflakeWriter(polars_conv), polars_conv),
    )
    Executor.register(
        "pandas", "snowflake", [],
        ExecutorConfig(sf_reader, sf_writers.SnowflakeWriter(pandas_conv), pandas_conv),
    )
    Executor.register(
        "geopandas", "snowflake", [],
        ExecutorConfig(
            sf_geo_reader, sf_writers.SnowflakeGeoWriter(geo_conv), geo_conv
        ),
    )


with contextlib.suppress(ImportError):
    _register_snowflake()

__all__ = ["ExecutionResult", "Executor"]
