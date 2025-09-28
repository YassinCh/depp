"""Version information for depp package."""

from importlib.metadata import version

__version__ = ".".join(version("dbt-depp").split(".")[:3])
# TODO: fix
version = __version__
