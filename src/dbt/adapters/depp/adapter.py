import inspect
from functools import lru_cache
from multiprocessing.context import SpawnContext
from pathlib import Path
from typing import Any, FrozenSet, Iterable, Type

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.meta import AdapterMeta, available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.factory import (
    FACTORY,
    get_adapter_by_type,  # type: ignore
)
from dbt.adapters.protocol import AdapterConfig
from dbt.artifacts.resources.types import ModelLanguage
from dbt.clients.jinja import MacroGenerator
from dbt.compilation import Compiler
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.parser.manifest import ManifestLoader

from .config import (
    AdapterTypeDescriptor,
    DeppCredentials,
    DeppCredentialsWrapper,
    ModelConfig,
    RelationDescriptor,
    get_db_profile_info,
)
from .executors import *  # noqa
from .executors import AbstractPythonExecutor
from .utils.ast_utils import extract_python_docstring
from .utils.general import logs, release_plugin_lock


class DeppAdapter(metaclass=AdapterMeta):
    # TODO: fix type ignores where possible
    """DBT adapter for executing Python models with database backends."""

    Relation = RelationDescriptor()
    AdapterSpecificConfigs = AdapterConfig
    type = AdapterTypeDescriptor()
    _db_adapter_class: Type[BaseAdapter]
    _db_adapter: BaseAdapter
    db_creds: Credentials

    def __new__(cls, config: RuntimeConfig, mp_context: SpawnContext):
        """Create adapter instance and configure underlying database adapter."""
        instance = super().__new__(cls)
        db_creds = cls.get_db_credentials(config)

        db_info = get_db_profile_info()
        if db_info.override_properties:
            for key in db_info.override_properties:
                if db_info.override_properties[key] is not None:
                    setattr(config, key, db_info.override_properties[key])

        with release_plugin_lock():
            db_adapter: Type[BaseAdapter] = FACTORY.get_adapter_class_by_name(  # type: ignore
                db_creds.type
            )
            original_plugin = FACTORY.get_plugin_by_name(config.credentials.type)
            original_plugin.dependencies = [db_creds.type]
            config.credentials = db_creds
            FACTORY.register_adapter(config, mp_context)
            config.credentials = DeppCredentialsWrapper(db_creds)  # type: ignore

        instance._db_adapter_class = db_adapter
        instance.db_creds = db_creds
        return instance

    def __init__(self, config: RuntimeConfig, mp_context: SpawnContext):
        """Initialize adapter with database connection and configuration."""
        self.config = config
        self.mp_context = mp_context

        # Type ignores are needed as we are overwriting some of dbt's behavior
        self._db_adapter = get_adapter_by_type(self._db_adapter_class.type())  # type: ignore
        self.connections = self._db_adapter.connections
        self._available_ = self._db_adapter._available_.union(self._available_)  # type: ignore
        self._parse_replacements_.update(self._db_adapter._parse_replacements_)  # type: ignore

    @logs
    def submit_python_job(self, parsed_model: dict[str, Any], compiled_code: str):
        # TODO: Add remote executors
        """Execute Python model code selecting the requested executor."""
        config = ModelConfig.from_model(parsed_model, compiled_code)
        executor = self.get_executor(parsed_model, config.library)
        result = executor.submit(compiled_code)
        return AdapterResponse(_message=f"PYTHON | {result}")

    def get_executor(
        self, parsed_model: dict[str, Any], library: str
    ) -> AbstractPythonExecutor[Any]:
        """Get Python executor based on model's configured library"""
        executor_class = AbstractPythonExecutor.registry.get(library)
        if executor_class is None:
            raise ValueError(
                f"No library '{library}'. Available: {list(AbstractPythonExecutor.registry.keys())}"
            )
        return executor_class(parsed_model, self.db_creds, library)  # type: ignore

    @available
    def db_materialization(self, context: dict[str, Any], materialization: str):
        """Execute database materialization macro."""
        materialization_macro = self.manifest.find_materialization_macro_by_name(
            self.config.project_name, materialization, self._db_adapter.type()
        )
        if materialization_macro is None:
            raise ValueError("Invalid Macro")
        return MacroGenerator(
            materialization_macro, context, stack=context["context_macro_stack"]
        )()

    @classmethod
    def get_db_credentials(cls, config: RuntimeConfig) -> Credentials:
        """Extract database credentials from adapter configuration."""
        dep_credentials: DeppCredentials | DeppCredentialsWrapper = config.credentials  # type: ignore
        if isinstance(dep_credentials, DeppCredentials):
            db_info = get_db_profile_info()
            return db_info.profile.credentials
        with release_plugin_lock():
            FACTORY.load_plugin(dep_credentials.db_creds.type)
        return dep_credentials.db_creds

    def get_compiler(self):
        """Get DBT compiler instance for this adapter."""
        return Compiler(self.config)

    def __getattr__(self, name: str):
        """Directly proxy to the DB adapter"""
        if hasattr(self._db_adapter, name):
            return getattr(self._db_adapter, name)
        else:
            getattr(super(), name)

    @classmethod
    def is_cancelable(cls) -> bool:
        """Python jobs cannot be cancelled once started."""
        return False

    @property
    def db_adapter(self):
        """Access underlying database adapter."""
        return self._db_adapter

    @property
    @lru_cache(maxsize=None)
    def manifest(self) -> Manifest:
        """Get cached DBT manifest for the project."""
        return ManifestLoader.get_full_manifest(self.config)

    def get_filtered_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[tuple[str, str]],
        relations: set[BaseRelation] | None = None,
    ) -> tuple[Any, list[Exception]]:
        """Override to enrich Python models with docstrings"""
        for manifest in [self.manifest, self._find_parent_manifest()]:  # type: ignore
            if manifest:
                self.inject_docstring(manifest)  # type: ignore

        return self._db_adapter.get_filtered_catalog(  # type: ignore
            relation_configs, used_schemas, relations
        )

    def _find_parent_manifest(self) -> Manifest | None:
        """Find manifest from parent GenerateTask if it exists"""
        try:
            frame = inspect.currentframe()
            while frame := frame.f_back:  # type: ignore
                if (
                    (obj := frame.f_locals.get("self"))
                    and type(obj).__name__ == "GenerateTask"
                    and hasattr(obj, "manifest")
                    and obj.manifest
                ):
                    return obj.manifest
        except Exception:
            pass
        return None

    def inject_docstring(self, manifest: Manifest):
        """Extract Python model docstrings as descriptions (YAML takes precedence)"""
        project_root = Path(self.config.project_root)
        for node in manifest.nodes.values():
            if (
                getattr(node, "language", None) == ModelLanguage.python
                and node.resource_type.value == "model"
                and not (node.description or "").strip()
            ):
                if docstring := extract_python_docstring(
                    str(project_root / node.original_file_path)
                ):
                    node.description = docstring
