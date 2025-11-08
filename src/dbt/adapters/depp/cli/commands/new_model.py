"""Create a new Python model from template."""

from pathlib import Path
from typing import Annotated, Literal

from cyclopts import Parameter
from jinja2 import Environment, PackageLoader
from rich.console import Console
from rich.prompt import Prompt

from ..main import app

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

    if output_path.exists():
        overwrite = Prompt.ask(
            f"[yellow]File {output_path} already exists. Overwrite?[/yellow]",
            choices=["y", "n"],
            default="n",
        )
        if overwrite.lower() != "y":
            return console.print("[red]Cancelled")

    env = Environment(
        loader=PackageLoader("dbt.adapters.depp.cli", "templates"),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("python_model.py.jinja")
    rendered = template.render(library=library, description=description)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)

    console.print(f"[green]Created {library} model at: {output_path}")
