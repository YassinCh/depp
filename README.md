# DEPP - DBT Python Postgres Adapter
This package support for running python models in dbt for postgres directly within your dbt project
Inspired on dbt-fal but is both extremely fast and high performance and fully typed

## Features

- Run Python scripts as dbt models
- Support for both pandas and polars dataframes
- Extremely High-performance using connectorx, asyncpg
- Seamless integration with PostgreSQL databases (more comming soon)

## Installation

Install using [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv add depp
```

Or using pip:

```bash
pip install depp
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
# models/my_python_model.py
import polars as pl

def model(dbt, session):
    dbt.config(library="polars")
    # Your Python logic here
    df = pl.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
    return df
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

## Requirements

- Python >= 3.12
- dbt-core >= 1.10.0
- PostgreSQL database

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.