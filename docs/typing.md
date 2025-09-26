# Type Safety in DEPP

DEPP provides comprehensive type safety for your dbt Python models, making development faster and more reliable with full IDE support.

## Overview

DEPP supports fully typed Python models with intelligent type inference based on your chosen DataFrame library. The typing system provides:

- **Type-safe model references**: `dbt.ref()` and `dbt.source()` returns properly typed DataFrames
- **Library-specific types**: Automatic type for pandas vs Polars
- **IDE integration**: Full autocomplete and type checking support
- **Runtime safety**: Type hints that match actual runtime behavior
- **Automatic Configuration based on type**: The configured dataframe backend is inferred from your type

## Quick Start with Types

### Option 1: Automatic Type Configuration (Recommended)

Use library-specific type hints that automatically configure your model:

```python
# models/polars_model.py
"""Polars model with automatic configuration."""

import polars as pl
from dbt.adapters.depp.typing import PolarsDbt, SessionObject

def model(dbt: PolarsDbt, session: SessionObject) -> pl.DataFrame:
    # No need to call dbt.config() - automatically set to "polars"
    customers = dbt.ref("customers")  # Type: pl.DataFrame | pl.LazyFrame
    orders = dbt.ref("orders")

    return customers.join(orders, on="customer_id")
```

```python
# models/pandas_model.py
"""Pandas model with automatic configuration."""

import pandas as pd
from dbt.adapters.depp.typing import PandasDbt, SessionObject

def model(dbt: "PandasDbt", session: "SessionObject") -> pd.DataFrame:
    # No need to call dbt.config() - automatically set to "pandas"
    customers = dbt.ref("customers")  # Type: pd.DataFrame
    orders = dbt.ref("orders")

    return customers.merge(orders, on="customer_id")
```

### Option 2: Manual Configuration

For more control, use the generic `DbtObject` and configure manually:

```python
# models/manual_config.py
"""Manual configuration approach."""

import polars as pl
from dbt.adapters.depp.typing import DbtObject, SessionObject, PolarsDataFrame

def model(dbt: "DbtObject", session: "SessionObject") -> pl.DataFrame:
    dbt.config(library="polars")

    customers: "PolarsDataFrame" = dbt.ref("customers")
    orders: "PolarsDataFrame" = dbt.ref("orders")

    return customers.join(orders, on="customer_id")
```

## Type Reference

### Core Types

#### DataFrame Types

```python
# Individual library types
PandasDataFrame = pd.DataFrame
PolarsDataFrame = Union[pl.DataFrame, pl.LazyFrame]

# Generic DataFrame type
DataFrame = Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]

# Generic type variable for functions
DataFrameT = TypeVar("DataFrameT", pd.DataFrame, pl.DataFrame, pl.LazyFrame)
```

#### DBT Object Types

```python
# Generic dbt object - requires manual configuration
class DbtObject(Protocol):
    def ref(self, model_name: str, ...) -> DataFrame: ...
    def source(self, source_name: str, table_name: str) -> DataFrame: ...
    def config(self, library: Literal["polars"] | Literal["pandas"]) -> DbtConfig: ...

# Library-specific objects - automatic configuration
class PolarsDbt(DbtObject, Protocol):
    def ref(self, model_name: str, ...) -> PolarsDataFrame: ...
    def source(self, source_name: str, table_name: str) -> PolarsDataFrame: ...

class PandasDbt(DbtObject, Protocol):
    def ref(self, model_name: str, ...) -> PandasDataFrame: ...
    def source(self, source_name: str, table_name: str) -> PandasDataFrame: ...
```

#### Session Object

```python
class SessionObject(Protocol):
    def execute(self, query: str) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...
```

## Advanced Examples

### Complex Polars Model with Type Safety

```python
# models/advanced_polars_analysis.py
"""Advanced customer segmentation using Polars with full type safety."""

import polars as pl
from datetime import datetime, timedelta
from dbt.adapters.depp.typing import PolarsDbt, SessionObject

def model(dbt: PolarsDbt, session: SessionObject) -> pl.DataFrame:
    # All references are typed as pl.DataFrame | pl.LazyFrame
    customers = dbt.ref("dim_customers")
    orders = dbt.ref("fct_orders")
    products = dbt.ref("dim_products")

    # Build complex analysis with type safety
    customer_metrics = (
        orders
        .join(customers, on="customer_id")
        .join(products, on="product_id")
        .filter(pl.col("order_date") >= datetime.now() - timedelta(days=365))
        .group_by(["customer_id", "customer_segment"])
        .agg([
            pl.col("order_value").sum().alias("total_spent"),
            pl.col("order_id").count().alias("order_count"),
            pl.col("order_value").mean().alias("avg_order_value"),
            pl.col("product_category").n_unique().alias("categories_purchased"),
            pl.col("order_date").max().alias("last_order_date"),
            pl.col("order_date").min().alias("first_order_date"),
        ])
        .with_columns([
            pl.col("total_spent").rank(method="ordinal", descending=True).alias("spending_rank"),
            (pl.col("last_order_date") - pl.col("first_order_date")).dt.total_days().alias("customer_lifetime_days"),
            (pl.col("total_spent") / pl.col("order_count")).alias("calculated_avg_order")
        ])
        .filter(pl.col("order_count") >= 2)  # Exclude one-time buyers
    )

    return customer_metrics
```

### Pandas Model with Statistical Analysis

```python
# models/statistical_analysis.py
"""Statistical analysis using pandas with type safety."""

import pandas as pd
import numpy as np
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt.adapters.depp.typing import PandasDbt, SessionObject

def model(dbt: "PandasDbt", session: "SessionObject") -> pd.DataFrame:
    # All references are typed as pd.DataFrame
    sales_data = dbt.ref("fct_sales")
    product_data = dbt.ref("dim_products")

    # Merge and analyze with full type support
    merged_data = sales_data.merge(
        product_data,
        on="product_id",
        how="inner"
    )

    # Statistical analysis
    stats_by_category = merged_data.groupby("product_category").agg({
        "sales_amount": ["sum", "mean", "std", "count"],
        "quantity_sold": ["sum", "mean"],
        "profit_margin": ["mean", "median"]
    }).round(2)

    # Flatten column names
    stats_by_category.columns = [
        f"{col[1]}_{col[0]}" if col[1] else col[0]
        for col in stats_by_category.columns
    ]

    # Add derived metrics
    stats_by_category["coefficient_of_variation"] = (
        stats_by_category["std_sales_amount"] / stats_by_category["mean_sales_amount"]
    )

    return stats_by_category.reset_index()
```

### Mixed Operations with Session

```python
# models/database_integration.py
"""Model that combines DataFrame operations with direct SQL."""

import polars as pl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt.adapters.depp.typing import PolarsDbt, SessionObject

def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    # Get base data
    base_customers = dbt.ref("customers")

    # Execute raw SQL for complex aggregation
    complex_query = """
    SELECT
        region,
        COUNT(*) as customer_count,
        AVG(lifetime_value) as avg_lifetime_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) as median_ltv
    FROM {{ ref('customer_lifetime_values') }}
    GROUP BY region
    """

    regional_stats = session.execute(complex_query)
    regional_df = pl.DataFrame(regional_stats.fetchall())

    # Combine with Polars operations
    result = (
        base_customers
        .join(regional_df, on="region", how="left")
        .with_columns([
            (pl.col("lifetime_value") / pl.col("avg_lifetime_value")).alias("ltv_ratio"),
            pl.when(pl.col("lifetime_value") > pl.col("median_ltv"))
            .then(pl.lit("high_value"))
            .otherwise(pl.lit("standard"))
            .alias("customer_tier")
        ])
    )

    return result
```

## IDE Setup

### VS Code Configuration

For optimal type checking in VS Code, add to your `pyproject.toml`:

```toml
[tool.pyright]
typeCheckingMode = "strict"
pythonVersion = "3.12"
pythonPlatform = "All"

[tool.mypy]
python_version = "3.12"
strict = true
warn_return_any = true
warn_unused_configs = true
```

### Type Checking Commands

```bash
# Run type checking
uv run mypy models/
uv run pyright models/

# Run with your IDE's Python language server for real-time feedback
```

## Best Practices

### 1. Use TYPE_CHECKING imports

Always import types within `TYPE_CHECKING` blocks to avoid runtime imports:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt.adapters.depp.typing import PolarsDbt, SessionObject
```

### 2. Prefer automatic configuration

Use `PolarsDbt` or `PandasDbt` instead of manual `dbt.config()` calls:

```python
# Good
def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    return dbt.ref("customers")

# Less ideal
def model(dbt: "DbtObject", session: "SessionObject") -> pl.DataFrame:
    dbt.config(library="polars")
    return dbt.ref("customers")
```

### 3. Add return type annotations

Always specify your model's return type:

```python
def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    # Clear contract about what this model returns
    return dbt.ref("customers")
```

### 4. Use descriptive variable names with types

```python
# Good - clear intent
customers_df: pl.DataFrame = dbt.ref("customers")
monthly_sales: pl.DataFrame = dbt.ref("monthly_aggregated_sales")

# Less clear
df1 = dbt.ref("customers")
data = dbt.ref("monthly_aggregated_sales")
```

### 5. Document complex transformations

```python
def model(dbt: "PolarsDbt", session: "SessionObject") -> pl.DataFrame:
    """Customer lifetime value calculation with recency, frequency, monetary analysis."""

    orders = dbt.ref("orders")
    customers = dbt.ref("customers")

    # RFM Analysis: Recency, Frequency, Monetary
    rfm_scores = (
        orders
        .group_by("customer_id")
        .agg([
            (pl.col("order_date").max() - pl.date(2024, 1, 1)).dt.total_days().alias("recency"),
            pl.col("order_id").count().alias("frequency"),
            pl.col("order_value").sum().alias("monetary")
        ])
        # Add percentile-based scoring
        .with_columns([
            pl.col("recency").rank(method="ordinal").alias("recency_rank"),
            pl.col("frequency").rank(method="ordinal", descending=True).alias("frequency_rank"),
            pl.col("monetary").rank(method="ordinal", descending=True).alias("monetary_rank")
        ])
    )

    return customers.join(rfm_scores, on="customer_id")
```

## Troubleshooting

### Common Type Issues

**Issue**: `dbt.ref()` returns `Any` instead of proper DataFrame type
**Solution**: Use library-specific dbt objects (`PolarsDbt`/`PandasDbt`) instead of generic `DbtObject`.


**Issue**: Type checker complains about DataFrame operations
**Solution**: Use proper return type annotations and ensure your DataFrame library imports are correct:

```python
import polars as pl
from dbt.adapters.depp.typing import PolarsDbt, SessionObject

def model(dbt: PolarsDbt, session: SessionObject) -> pl.DataFrame:
    df = dbt.ref("customers")  # Now properly typed as PolarsDataFrame
    return df.filter(pl.col("active") == True)  # Type checker understands Polars methods
```