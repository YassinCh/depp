import random
import string
from datetime import datetime, timedelta

import polars as pl


def generate_sample_data(num_records: int = 100000) -> pl.DataFrame:
    """Generate sample product data with 100K records."""

    # Random seed for reproducibility
    random.seed(42)

    # Generate product names
    product_categories = [
        "Electronics",
        "Clothing",
        "Food",
        "Books",
        "Toys",
        "Sports",
        "Home",
        "Garden",
        "Beauty",
        "Auto",
    ]
    product_types = [
        "Premium",
        "Standard",
        "Basic",
        "Deluxe",
        "Pro",
        "Elite",
        "Classic",
        "Modern",
        "Vintage",
        "Limited",
    ]

    product_names = []
    prices = []
    quantities = []

    for i in range(num_records):
        # Generate product name
        category = random.choice(product_categories)
        type_name = random.choice(product_types)
        product_id = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=5)
        )
        product_name = f"{type_name} {category} {product_id}"
        product_names.append(product_name)

        # Generate price (ranging from 10 to 500, with some below 50 to test filter)
        # 70% chance to be above 50 to ensure good amount passes filter
        if random.random() < 0.7:
            price = round(random.uniform(50.01, 500.00), 2)
        else:
            price = round(random.uniform(10.00, 50.00), 2)
        prices.append(price)

        # Generate quantity (1 to 100)
        quantity = random.randint(1, 100)
        quantities.append(quantity)

    # Create DataFrame
    df = pl.DataFrame(
        {
            "product_id": list(range(1, num_records + 1)),
            "product_name": product_names,
            "price": prices,
            "quantity": quantities,
            "created_date": [
                datetime.now() - timedelta(days=random.randint(0, 365))
                for _ in range(num_records)
            ],
        }
    )

    return df


def model(dbt, session) -> pl.DataFrame:
    """DBT model that generates 100K records and applies transformations."""
    dbt.config(library="polars")
    products_df = generate_sample_data(100000)

    # Apply transformations
    result = products_df.with_columns(
        [
            (pl.col("price") * pl.col("quantity")).alias("total_value"),
            pl.col("product_name").str.to_uppercase().alias("product_name_upper"),
        ]
    )
    return result
