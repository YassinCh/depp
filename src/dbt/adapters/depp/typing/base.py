from typing import Any, Protocol


class DbtThis:
    """Represents the current model in dbt."""

    database: str
    schema: str
    identifier: str

    def __repr__(self) -> str:
        """Return the fully qualified table name."""
        return f'"{self.database}"."{self.schema}"."{self.identifier}"'


class SessionObject(Protocol):
    """Type hints for the session object passed to Python models.

    The session object provides database connectivity for Python models.
    In most cases with this adapter, it represents a SQLAlchemy session
    or similar database connection object.
    """

    def execute(self, query: str) -> Any:
        """Execute a SQL query.

        Args:
            query: SQL query string to execute

        Returns:
            Query result
        """
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    def close(self) -> None:
        """Close the session."""
        ...


class DbtConfig:
    """Configuration object for dbt models."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize configuration."""
        ...

    @staticmethod
    def get(key: str, default: Any | None = None) -> Any:
        """Get configuration value by key."""
        ...

    def __call__(self, **kwargs: Any) -> None:
        """Set configuration options.

        Args:
            library: DataFrame library to use ("pandas" or "polars")
            **kwargs: Additional configuration options
        """
        ...
