"""Version information for depp package."""

from importlib.metadata import version as _get_version

version = ".".join(_get_version("dbt-depp").split(".")[:3])
