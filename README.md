# DEPP - DBT Python Postgres Adapter
This package support for running python models in dbt for postgres directly within your dbt project
Inspired on dbt-fal but made to be both extremely high performance and fully typed
Also supports polars dataframe besides pandas and more are coming soon

## Features
- **Run Python scripts as dbt models** - Write Python logic directly in your dbt project
- **Fully typed Python models** - Complete type safety with IDE support ([see typing docs](docs/typing.md))
- **Multiple DataFrame libraries** - Support for both pandas and Polars dataframes (more comming soon)
- **Auto-generated documentation** - Python docstrings automatically become model descriptions in dbt docs
- **High performance** - Blazing fast execution using connectorx and asyncpg
- **PostgreSQL integration** - Seamless integration with PostgreSQL databases

## Installation

Install using [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv add dbt-debb
```

Or using pip:

```bash
pip install dbt-depp
```

## Quick Start

1. Add to your `profiles.yml`:
Make sure to both add a db_profile with all your details and add your database and schema

```yaml
your_project:
  target: dev
  outputs:
    dev:
      type: depp
      db_profile: dev_postgres
      database: example_db
      schema: test
      
    dev_postgres:
      type: postgres
      host: localhost
      user: postgres
      password: postgres
      port: 5432
      database: example_db
      schema: test
      threads: 1
```

2. Create Python models in your dbt project:

```python
# models/customer_analysis.py
"""Analyze customer purchase patterns using Polars."""
import polars as pl
from dbt.adapters.depp.typing import PolarsDbt, SessionObject

def model(dbt: PolarsDbt, session: SessionObject) -> pl.DataFrame:
    # Reference existing models with full type safety
    customers_df = dbt.ref("customers")
    orders_df = dbt.ref("orders")

    # Perform analysis using Polars
    result = (
        customers_df
        .join(orders_df, on="customer_id", how="inner")
        .group_by("customer_region")
        .agg([
            pl.col("order_amount").sum().alias("total_revenue"),
            pl.col("customer_id").n_unique().alias("unique_customers"),
            pl.col("order_amount").mean().alias("avg_order_value")
        ])
        .sort("total_revenue", descending=True)
    )

    return result
```
3. `dbt run`!

## Development
This project uses [uv](https://docs.astral.sh/uv/) for dependency management:

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Build package
uv build
```

## Documentation

- **[Getting Started Guide](docs/getting-started.md)** - Complete setup guide including profiles.yml configuration and first Python models
- **[Type Safety Guide](docs/typing.md)** - Complete guide to using DEPP's type system for better IDE support and code safety

## Requirements

- Python >= 3.12
- dbt-core >= 1.10.0
- PostgreSQL database

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.