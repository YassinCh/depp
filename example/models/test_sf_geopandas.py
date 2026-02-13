"""Stress test Snowflake GeoPandas executor - 1M rows with geometry."""

from typing import TYPE_CHECKING

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

if TYPE_CHECKING:
    from src.dbt.adapters.depp.typing import GeoPandasDbt, SessionObject


def model(dbt: "GeoPandasDbt", session: "SessionObject") -> gpd.GeoDataFrame:
    n = 1_000_000
    rng = np.random.default_rng(42)
    lons = rng.uniform(3.5, 7.0, n)
    lats = rng.uniform(50.8, 53.5, n)
    df = pd.DataFrame(
        {
            "id": range(n),
            "city": [f"place_{i}" for i in range(n)],
        }
    )
    geometry = [Point(lon, lat) for lon, lat in zip(lons, lats)]
    return gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
