from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...src.dbt.adapters.depp.typing import PandasDbt, SessionObject


def model(dbt: "PandasDbt", session: "SessionObject"):
    """
    Creates a simple customer table with sample data.

    This model generates basic customer information including IDs, names,
    regions, and total spending amounts for testing purposes.
    """
    # Create a simple dataframe with customer data
    data = {
        "customer_id": [1, 2, 3, 4, 5],
        "customer_name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "region": ["North", "South", "East", "West", "North"],
        "total_spent": [1500.00, 2300.00, 1800.00, 3200.00, 900.00],
    }

    df = pd.DataFrame(data)
    return df
