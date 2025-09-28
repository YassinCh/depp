from dataclasses import dataclass
from typing import Any

from ..executors import AbstractPythonExecutor
from ..utils import get_library_from_typehint


@dataclass(frozen=True)
class ModelConfig:
    library: str
    parsed_model: dict[str, Any]
    compiled_code: str

    @classmethod
    def from_model(
        cls,
        parsed_model: dict[str, Any],
        compiled_code: str,
        default_library: str = "polars",
    ) -> "ModelConfig":
        # Resolves library from config
        library = (
            parsed_model.get("config", {}).get("library")
            or get_library_from_typehint(
                compiled_code, AbstractPythonExecutor.type_mapping
            )
            or default_library
        )
        return cls(library, parsed_model, compiled_code)
