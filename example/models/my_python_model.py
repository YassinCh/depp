import pandas as pd

def model(dbt, session):
    """
    A simple Python dbt model that creates a DataFrame with some sample data.
    """
    # Create sample data
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'department': ['Engineering', 'Marketing', 'Sales', 'Engineering', 'Marketing']
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    return df