"""Interactive setup wizard for DEPP adapter."""

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from rich.console import Console
from rich.prompt import Prompt

from dbt.adapters.depp.cli.main import app
from dbt.adapters.depp.cli.utils import confirm_overwrite, render_template

console = Console()


@app.command
def init(
    profile_name: Annotated[str | None, Parameter(help="Profile name")] = None,
) -> None:
    """Interactive setup wizard for profiles.yml."""
    console.print("[bold blue]DEPP Adapter Setup Wizard[/bold blue]\n")

    profile_name = profile_name or Prompt.ask("Profile name", default="depp_project")

    console.print("\n[yellow]Database Credentials (PostgreSQL)[/yellow]")
    creds = {
        "host": Prompt.ask("Host", default="localhost"),
        "port": Prompt.ask("Port", default="5432"),
        "user": Prompt.ask("User", default="postgres"),
        "password": Prompt.ask("Password", password=True),
        "database": Prompt.ask("Database"),
        "schema": Prompt.ask("Schema", default="public"),
    }

    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    profiles_path.parent.mkdir(exist_ok=True)

    if not confirm_overwrite(profiles_path, console):
        return

    render_template(
        "profiles.yml.jinja", profiles_path, profile_name=profile_name, **creds
    )
    console.print(f"\n[green]✓ Created {profiles_path}")

    example_path = Path("models") / "example_model.py"
    example_path.parent.mkdir(exist_ok=True)

    if not example_path.exists():
        render_template("example_model.py.jinja", example_path)
        console.print(f"[green]✓ Created {example_path}")

    console.print(
        "\n[bold green]Setup complete![/bold green]\n"
        "Next steps:\n  1. dbt-depp validate\n  2. dbt run"
    )
