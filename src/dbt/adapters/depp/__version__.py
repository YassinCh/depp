"""Version information for depp package."""

from importlib.metadata import version

__version__ = ".".join(version("dbt-depp").split(".")[:3])

version = __version__
