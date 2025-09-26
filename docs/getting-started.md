# Getting Started with DEPP

DEPP (dbt Python Postgres) allows you to run Python scripts as dbt models directly within your dbt project. This guide will walk you through setting up DEPP from scratch.

## Prerequisites

Before you begin, make sure you have:

- **Python >= 3.12** - DEPP requires modern Python features
- **dbt-core >= 1.10.0** - Latest dbt features for Python model support
- **PostgreSQL database** - DEPP is optimized for PostgreSQL
- **uv** (recommended) or pip for package management

## Installation

### Option 1: Using uv (Recommended)

[uv](https://docs.astral.sh/uv/) is the fastest Python package manager and is highly recommended:

```bash
# Install DEPP
uv add dbt-depp

# Or if you're starting a new project
uv init my-dbt-project
cd my-dbt-project
uv add dbt-depp
```

### Option 2: Using pip

```bash
pip install dbt-depp
```

## Database Setup

DEPP requires a PostgreSQL database. Make sure you have:

1. A running PostgreSQL instance
2. A database created for your dbt project
3. A user with appropriate permissions

### Example PostgreSQL Setup

```sql
-- Connect to PostgreSQL as a superuser
CREATE DATABASE my_analytics_db;
CREATE USER dbt_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE my_analytics_db TO dbt_user;

-- Connect to your new database
\c my_analytics_db

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO dbt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dbt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dbt_user;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO dbt_user;
```

## dbt Project Setup

### 1. Initialize Your dbt Project

If you don't have a dbt project yet:

```bash
# Create new dbt project
dbt init my_project

# Navigate to your project
cd my_project
```

### 2. Configure profiles.yml

DEPP uses a dual-profile setup in your `~/.dbt/profiles.yml`:

```yaml
# ~/.dbt/profiles.yml
my_project:  # This should match your dbt_project.yml name
  target: dev
  outputs:
    dev:
      # DEPP adapter configuration
      type: depp
      db_profile: dev_postgres  # References the PostgreSQL profile below
      database: my_analytics_db
      schema: analytics

    prod:
      type: depp
      db_profile: prod_postgres
      database: my_analytics_db
      schema: production

# PostgreSQL connection profiles referenced by DEPP
dev_postgres:
  outputs:
    default:
      type: postgres
      host: localhost
      port: 5432
      user: dbt_user
      password: secure_password
      database: my_analytics_db
      schema: analytics
      threads: 4
      keepalives_idle: 0
      search_path: "analytics"

prod_postgres:
  outputs:
    default:
      type: postgres
      host: prod-postgres.company.com
      port: 5432
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      database: my_analytics_db
      schema: production
      threads: 8
      keepalives_idle: 0
      search_path: "production"
```

### 3. Verify Your Connection

Test that everything is working:

```bash
# Test connection
dbt debug

# You should see output like:
# Connection test: [OK connection ok]
```

## Creating Your First Python Model

### 1. Basic Python Model

Create your first Python model in `models/my_first_python_model.py`:

```python
# models/my_first_python_model.py
"""Generate sample customer data for testing."""

import pandas as pd
from dbt.adapters.depp.typing import PandasDbt, SessionObject

def model(dbt: PandasDbt, session: SessionObject) -> pd.DataFrame:
    # Create sample data
    customers_data = {
        "customer_id": [1, 2, 3, 4, 5],
        "customer_name": ["Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Eve Davis"],
        "email": ["alice@email.com", "bob@email.com", "carol@email.com", "david@email.com", "eve@email.com"],
        "registration_date": pd.to_datetime([
            "2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05", "2023-05-12"
        ]),
        "total_orders": [12, 8, 25, 3, 18],
        "lifetime_value": [1250.00, 890.50, 2340.75, 156.30, 1875.25]
    }

    df = pd.DataFrame(customers_data)

    # Add calculated columns
    df['avg_order_value'] = df['lifetime_value'] / df['total_orders']
    df['customer_segment'] = df['lifetime_value'].apply(
        lambda x: 'High Value' if x > 1500 else 'Medium Value' if x > 800 else 'Low Value'
    )

    return df
```

### 2. Run Your Model

```bash
# Run your Python model
dbt run --models my_first_python_model

# Check the results
dbt show --select my_first_python_model
```

## Working with Existing Data

### 1. Reference SQL Models

Create a SQL model first (`models/orders.sql`):

```sql
-- models/orders.sql
{{ config(materialized='table') }}

SELECT
    1 as order_id,
    1 as customer_id,
    '2023-01-20'::date as order_date,
    150.00 as order_amount,
    'completed' as order_status

UNION ALL

SELECT 2, 1, '2023-02-15'::date, 89.50, 'completed'
UNION ALL
SELECT 3, 2, '2023-02-18'::date, 245.00, 'completed'
UNION ALL
SELECT 4, 3, '2023-03-01'::date, 67.25, 'pending'
```

### 2. Reference It In Python

Create a Python model that uses the SQL model:

```python
# models/customer_order_analysis.py
"""Analyze customer ordering patterns using existing data."""

import polars as pl
from dbt.adapters.depp.typing import PolarsDbt, SessionObject

def model(dbt: PolarsDbt, session: SessionObject) -> pl.DataFrame:
    # Reference existing models
    customers_df = dbt.ref("my_first_python_model")
    orders_df = dbt.ref("orders")

    # Perform analysis
    analysis = (
        customers_df
        .join(orders_df, on="customer_id", how="left")
        .group_by(["customer_id", "customer_name", "customer_segment"])
        .agg([
            pl.col("order_amount").sum().alias("total_order_value"),
            pl.col("order_id").count().alias("order_count"),
            pl.col("order_amount").mean().alias("avg_order_amount"),
            pl.col("order_date").max().alias("last_order_date")
        ])
        .with_columns([
            (pl.col("total_order_value") / pl.col("order_count")).alias("calculated_avg_order"),
            pl.when(pl.col("order_count") > 5)
                .then(pl.lit("frequent"))
                .otherwise(pl.lit("occasional"))
                .alias("order_frequency_segment")
        ])
        .sort("total_order_value", descending=True)
    )

    return analysis
```

## Common Issues and Solutions

### Issue: "Could not find profile"

**Problem**: dbt can't find your profile configuration.

**Solution**:
1. Check that your `dbt_project.yml` profile name matches your `profiles.yml` profile name
2. Ensure `profiles.yml` is in the correct location (`~/.dbt/profiles.yml`)
3. Check YAML indentation - it must be exact

## Next Steps

Once you have DEPP working:

1. **Read the [Type Safety Guide](typing.md)** - Learn how to use DEPP's advanced typing features
2. **Explore the examples** - Check out more complex models in the `example/` directory
3. **Set up CI/CD** - Configure automated testing and deployment
4. **Add data tests** - Use dbt's testing framework to validate your Python models
5. **Documentation** - Use dbt's documentation features with your Python models

## Getting Help

If you run into issues:
1. Check the [troubleshooting section](#common-issues-and-solutions) above
2. Review dbt logs: `dbt run --debug`
3. Verify your setup: `dbt debug`
4. Check the [DEPP GitHub repository](https://github.com/YassinCh/depp) for examples and issues

## Example Project Structure

Here's what a typical DEPP project looks like:

```
my_dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   └── stg_orders.py          # Python staging model
│   ├── marts/
│   │   ├── dim_customers.py       # Python dimension model
│   │   └── fct_order_metrics.py   # Python fact model
│   └── analysis/
│       └── customer_cohorts.py    # Python analysis model
├── tests/
│   └── test_customer_metrics.yml
├── macros/
│   └── custom_macros.sql
└── seeds/
    └── lookup_tables.csv
```

This structure combines SQL and Python models seamlessly, giving you the best of both worlds!