import pandas as pd

def model(dbt, session):
    # Create a simple dataframe with customer data
    data = {
        'customer_id': [1, 2, 3, 4, 5],
        'customer_name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'region': ['North', 'South', 'East', 'West', 'North'],
        'total_spent': [1500.00, 2300.00, 1800.00, 3200.00, 900.00]
    }

    df = pd.DataFrame(data)
    return df