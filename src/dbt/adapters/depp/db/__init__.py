from typing import Any

from dbt.adapters.contracts.connection import Credentials

from .base import DEFAULT_SRID, DatabaseOps
from .postgres import PostgresOps

ops_registry: dict[str, type[Any]] = {"postgres": PostgresOps}

try:
    from .snowflake import SnowflakeOps

    ops_registry["snowflake"] = SnowflakeOps
except ImportError:
    pass


def get_db_ops(creds: Credentials) -> DatabaseOps:
    """Get database operations for the given credentials type."""
    if creds.type not in ops_registry:
        raise ValueError(
            f"Unsupported database: {creds.type}. Supported: {list(ops_registry)}"
        )
    return ops_registry[creds.type]()


__all__ = ["DEFAULT_SRID", "DatabaseOps", "PostgresOps", "get_db_ops"]
