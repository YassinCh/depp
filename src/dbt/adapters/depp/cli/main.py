"""Main CLI module for dbt-depp using cyclopts."""

import cyclopts
from typing import Annotated, Optional

app = cyclopts.App(
    name="dbt-depp",
    help="DBT Enexis Python Postgres adapter - Run python scripts from any dbt project.",
)


@app.command
def run(
    model: Annotated[Optional[str], cyclopts.Parameter(help="Model to run")] = None,
    profile: Annotated[str, cyclopts.Parameter(help="Profile to use")] = "default",
    verbose: Annotated[bool, cyclopts.Parameter(help="Enable verbose output")] = False,
):
    """Run a dbt model with the depp adapter."""
    if verbose:
        print(f"Running model: {model} with profile: {profile}")
    else:
        print(f"Running {model}")


@app.command
def compile(
    model: Annotated[Optional[str], cyclopts.Parameter(help="Model to compile")] = None,
    profile: Annotated[str, cyclopts.Parameter(help="Profile to use")] = "default",
):
    """Compile a dbt model."""
    print(f"Compiling model: {model} with profile: {profile}")


@app.command
def test(
    model: Annotated[Optional[str], cyclopts.Parameter(help="Model to test")] = None,
    profile: Annotated[str, cyclopts.Parameter(help="Profile to use")] = "default",
):
    """Run tests for a dbt model."""
    print(f"Testing model: {model} with profile: {profile}")


def main():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()