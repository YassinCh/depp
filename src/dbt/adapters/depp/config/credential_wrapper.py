from typing import Any

from dbt.adapters.contracts.connection import Credentials

from dbt.adapters.depp.utils import find_funcs_in_stack

from .adapter_type import ADAPTER_NAME


class DeppCredentialsWrapper:
    """Wrap DB credentials, proxying type based on call context."""

    _db_creds: Credentials | None = None

    def __init__(self, db_creds: Credentials) -> None:
        """Initialize with underlying database credentials."""
        self._db_creds = db_creds

    @property
    def type(self) -> str:
        """Return the adapter type string."""
        if find_funcs_in_stack({"to_target_dict", "db_materialization"}):
            return self.db_creds.type
        return ADAPTER_NAME

    @property
    def db_creds(self) -> Credentials:
        """Return the underlying DB credentials."""
        if self._db_creds is None:
            raise ValueError("No valid DB Credentials")
        return self._db_creds

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the DB adapter."""
        return getattr(self._db_creds, name)
