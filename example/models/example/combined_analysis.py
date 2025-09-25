import pandas as pd

def model(dbt, session):
    # Reference the SQL model (products)
    products_df = dbt.ref("base_sql_model")

    # Reference the Python model (customers)
    customers_df = dbt.ref("base_python_model")

    # Reference a source table (raw orders)
    raw_orders_df = dbt.source("raw_data", "raw_orders")

    # Example analysis: combine all three datasets
    # Calculate total product value
    products_df['total_value'] = products_df['price'] * products_df['quantity']

    # Add a summary column to customers
    customers_df['customer_category'] = pd.cut(
        customers_df['total_spent'],
        bins=[0, 1000, 2000, float('inf')],
        labels=['Bronze', 'Silver', 'Gold']
    )

    # Create a summary dataframe combining insights from all sources
    summary_data = {
        'metric': [
            'Total Products',
            'Total Product Value',
            'Average Product Price',
            'Total Customers',
            'Average Customer Spending',
            'Source Orders Count'
        ],
        'value': [
            len(products_df),
            products_df['total_value'].sum(),
            products_df['price'].mean(),
            len(customers_df),
            customers_df['total_spent'].mean(),
            len(raw_orders_df) if not raw_orders_df.empty else 0
        ]
    }

    summary_df = pd.DataFrame(summary_data)

    # Add metadata
    summary_df['analysis_timestamp'] = pd.Timestamp.now()
    summary_df['data_sources'] = 'sql_model, python_model, source_table'

    return summary_df