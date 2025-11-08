"""Main CLI module for dbt-depp using cyclopts."""

import cyclopts

app = cyclopts.App(
    name="dbt-depp",
    help="DBT Python Adapter with some commands to make your developer life easier.",
)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
