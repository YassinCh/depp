"""CLI commands for dbt-depp."""

from .experiment import experiment
from .init import init
from .inspect import inspect
from .new_model import new_model
from .validate import validate

__all__ = ["experiment", "init", "inspect", "new_model", "validate"]
