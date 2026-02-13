"""Create a new Python model from template."""

from pathlib import Path
from typing import Annotated, Literal

from cyclopts import Parameter
from rich.console import Console
from rich.prompt import Prompt

from dbt.adapters.depp.cli.main import app
from dbt.adapters.depp.cli.utils import confirm_overwrite, render_template

console = Console()

LibraryType = Literal["polars", "pandas", "geopandas"]


@app.command
def new_model(
    name: Annotated[str, Parameter(help="Model name (without .py extension)")],
    library: Annotated[
        LibraryType | None,
        Parameter(help="DataFrame library to use (polars, pandas, or geopandas)"),
    ] = None,
    description: Annotated[
        str, Parameter(help="Model description")
    ] = "TODO: Add model description",
    output_dir: Annotated[Path, Parameter(help="Output directory")] = Path("models"),
) -> None:
    """Create a new Python model from template."""
    if library is None:
        library = Prompt.ask(
            "[bold blue]Select DataFrame library[/bold blue]",
            choices=["polars", "pandas", "geopandas"],
            default="polars",
        )  # type: ignore[assignment]

    model_name = name.removesuffix(".py")
    output_path = output_dir / f"{model_name}.py"

    if not confirm_overwrite(output_path, console):
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    render_template(
        "python_model.py.jinja", output_path, library=library, description=description
    )

    console.print(f"[green]Created {library} model at: {output_path}")
