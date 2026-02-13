"""Main CLI module for dbt-depp using cyclopts."""

import cyclopts

app = cyclopts.App(
    name="dbt-depp",
    help="DBT Python Adapter with some commands to make your developer life easier.",
)


def main() -> None:
    """Run the dbt-depp CLI application."""
    app()


if __name__ == "__main__":
    main()
