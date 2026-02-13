"""Generate marimo notebooks for model experimentation."""

from pathlib import Path

from dbt.adapters.depp.cli.utils.templates import render_template


def generate_notebook(
    output_path: Path, deps: list[tuple[str, str]], library: str
) -> None:
    """Generate marimo notebook with data loading cells."""
    render_template(
        "marimo_notebook.py.jinja",
        output_path,
        library=library,
        dep_names=[name for name, _ in deps],
    )
