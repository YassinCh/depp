"""Interactive setup wizard for DEPP adapter."""

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from jinja2 import Environment, PackageLoader
from rich.console import Console
from rich.prompt import Prompt

from ..main import app

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

    if (
        profiles_path.exists()
        and Prompt.ask(
            "\n[yellow]profiles.yml exists. Overwrite?[/yellow]",
            choices=["y", "n"],
            default="n",
        )
        != "y"
    ):
        return console.print("[red]Cancelled")

    env = Environment(loader=PackageLoader("dbt.adapters.depp.cli", "templates"))
    profiles_path.write_text(
        env.get_template("profiles.yml.jinja").render(
            profile_name=profile_name, **creds
        )
    )
    console.print(f"\n[green]✓ Created {profiles_path}")

    example_path = Path("models") / "example_model.py"
    example_path.parent.mkdir(exist_ok=True)

    if not example_path.exists():
        example_path.write_text(env.get_template("example_model.py.jinja").render())
        console.print(f"[green]✓ Created {example_path}")

    console.print(
        "\n[bold green]Setup complete![/bold green]\nNext steps:\n  1. dbt-depp validate\n  2. dbt run"
    )
