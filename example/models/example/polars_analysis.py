import polars as pl


def model(dbt, session) -> pl.DataFrame:
    dbt.config(library="polars")

    products_df: pl.DataFrame = dbt.ref("base_sql_model")
    print(type(products_df))
    result = products_df.filter(pl.col("price") > 50).with_columns(
        [
            (pl.col("price") * pl.col("quantity")).alias("total_value"),
            pl.col("product_name").str.to_uppercase().alias("product_name_upper"),
        ]
    )

    return result
