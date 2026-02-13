"""Template rendering utilities for the dbt-depp CLI."""

from pathlib import Path
from typing import Any

from jinja2 import Environment, PackageLoader


def render_template(template_name: str, output_path: Path, **context: Any) -> None:
    """Load a Jinja template and write rendered output."""
    env = Environment(
        loader=PackageLoader("dbt.adapters.depp.cli", "templates"),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    output_path.write_text(env.get_template(template_name).render(**context))
