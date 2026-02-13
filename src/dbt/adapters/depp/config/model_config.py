from dataclasses import dataclass
from typing import Any

from dbt.adapters.depp.executors import Executor
from dbt.adapters.depp.utils import get_library_from_typehint


@dataclass(frozen=True)
class ModelConfig:
    """Resolve and hold the execution library for a dbt Python model."""

    library: str
    parsed_model: dict[str, Any]
    compiled_code: str

    @classmethod
    def from_model(
        cls,
        parsed_model: dict[str, dict[str, Any]],
        compiled_code: str,
        default_library: str = "polars",
    ) -> "ModelConfig":
        """Create a ModelConfig by resolving the library from model metadata."""
        library = (
            parsed_model.get("config", {}).get("library")
            or get_library_from_typehint(compiled_code, Executor.type_mapping)
            or default_library
        )
        return cls(library, parsed_model, compiled_code)
