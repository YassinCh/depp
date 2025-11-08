"""Generate marimo notebooks for model experimentation."""

from pathlib import Path

from jinja2 import Environment, PackageLoader


def generate_notebook(
    output_path: Path, deps: list[tuple[str, str]], library: str
) -> None:
    """Generate marimo notebook with data loading cells."""
    env = Environment(loader=PackageLoader("dbt.adapters.depp.cli", "templates"))
    template = env.get_template("marimo_notebook.py.jinja")
    code = template.render(library=library, dep_names=[name for name, _ in deps])
    output_path.write_text(code)
