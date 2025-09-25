from functools import lru_cache
from multiprocessing.context import SpawnContext
from typing import Any, Type

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.meta import AdapterMeta, available
from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.factory import (
    FACTORY,
    get_adapter_by_type,  # type: ignore
)
from dbt.adapters.protocol import AdapterConfig
from dbt.clients.jinja import MacroGenerator
from dbt.compilation import Compiler
from dbt.config.runtime import RuntimeConfig
from dbt.parser.manifest import ManifestLoader

from .config import (
    AdapterTypeDescriptor,
    DeppCredentials,
    DeppCredentialsWrapper,
    load_profile_info,
)
from .executors import AbstractPythonExecutor, PandasPythonExecutor, PolarsLocalExecutor
from .utils import logs, release_plugin_lock

DB_PROFILE, OVERRIDE_PROPERTIES = load_profile_info()
DB_RELATION = FACTORY.get_relation_class_by_name(DB_PROFILE.credentials.type)


class PythonAdapter(metaclass=AdapterMeta):
    """The PythonAdapter ...."""

    # TODO: documentation and docstrings

    Relation = DB_RELATION
    AdapterSpecificConfigs = AdapterConfig
    type = AdapterTypeDescriptor()
    _db_adapter_class: Type[BaseAdapter]
    db_creds: Credentials

    def __new__(cls, config: RuntimeConfig, mp_context: SpawnContext):
        instance = super().__new__(cls)
        db_creds = cls.get_db_credentials(config)

        with release_plugin_lock():
            # TODO: fix typehint here
            db_adapter: Type[BaseAdapter] = FACTORY.get_adapter_class_by_name(  # type: ignore
                db_creds.type
            )
            original_plugin = FACTORY.get_plugin_by_name(config.credentials.type)
            original_plugin.dependencies = [db_creds.type]

        for key in OVERRIDE_PROPERTIES:
            if OVERRIDE_PROPERTIES[key] is not None:
                setattr(config, key, OVERRIDE_PROPERTIES[key])

        with release_plugin_lock():
            config.credentials = db_creds
            FACTORY.register_adapter(config, mp_context)
            config.credentials = DeppCredentialsWrapper(db_creds)  # type: ignore
        instance._db_adapter_class = db_adapter
        instance.db_creds = db_creds
        return instance

    def __init__(self, config: RuntimeConfig, mp_context: SpawnContext):
        self.config = config
        self.mp_context = mp_context

        # TODO: look at this and improve
        self._db_adapter: BaseAdapter = get_adapter_by_type(
            self._db_adapter_class.type()
        )  # type: ignore
        self.connections = self._db_adapter.connections
        self._available_ = self._db_adapter._available_.union(self._available_)  # type: ignore
        self._parse_replacements_.update(self._db_adapter._parse_replacements_)  # type: ignore

    @logs
    def submit_python_job(self, parsed_model: dict[str, Any], compiled_code: str):
        # TODO: Add remote executor
        executor = self.get_executor(parsed_model)
        result = executor.submit(compiled_code)
        return self.generate_python_submission_response(result)

    def generate_python_submission_response(self, submission_result):
        # TODO: Add more Response items
        return AdapterResponse(
            _message=f"Successfully executed Python model, result shape: {getattr(submission_result, 'shape', 'unknown')}"
        )

    def get_executor(
        self, parsed_model: dict[str, Any]
    ) -> PolarsLocalExecutor | PandasPythonExecutor:
        # TODO: I dont wanna change this everywhere so let's find a way to fix it
        library_mapping = dict(polars=PolarsLocalExecutor, pandas=PandasPythonExecutor)
        library = parsed_model.get("config", {}).get("library", "pandas")
        return library_mapping[library](parsed_model, self.db_creds, library)

    @available
    def db_materialization(self, context: dict[str, Any], materialization: str):
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
        dep_credentials: DeppCredentials | DeppCredentialsWrapper = config.credentials  # type: ignore
        if isinstance(dep_credentials, DeppCredentials):
            return DB_PROFILE.credentials
        with release_plugin_lock():
            FACTORY.load_plugin(dep_credentials.db_creds.type)
        return dep_credentials.db_creds

    def get_compiler(self):
        return Compiler(self.config)

    def __getattr__(self, name: str):
        """Directly proxy to the DB adapter"""
        if hasattr(self._db_adapter, name):
            return getattr(self._db_adapter, name)
        else:
            getattr(super(), name)

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @property
    def db_adapter(self):
        return self._db_adapter

    @property
    @lru_cache(maxsize=None)
    def manifest(self):
        return ManifestLoader.get_full_manifest(self.config)
