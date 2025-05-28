from pyspark.sql import DataFrame
from pyspark.sql.functions import round, year, sum
from typing import Dict
from .constants import *

class DataTransformer:
    def __init__(
        self,
        silver_tables: Dict[str, DataFrame],
    ):
        self.orders_df = silver_tables[ORDERS_KEY]
        self.customers_df = silver_tables[CUSTOMERS_KEY]
        self.products_df = silver_tables[PRODUCTS_KEY]


    """
    Creates an enriched orders DataFrame by joining orders, customers, and products data.

    This function performs inner joins between the orders, customers, and products DataFrames
    on their respective keys. 
    It also rounds the profit column to two decimal places and 
    selects relevant columns from each DataFrame.

    Returns:
        DataFrame: An enriched DataFrame containing order details along with customer and product information.
    """
    def _create_enriched_orders(self) -> DataFrame:
        try:
            return (
                self.orders_df
                .join(self.customers_df, on="customer_id", how="inner")
                .join(self.products_df, on="product_id", how="inner")
                .select(
                    self.orders_df["*"],
                    round(self.orders_df["profit"], 2).alias("rounded_profit"),
                    self.customers_df["customer_name"],
                    self.customers_df["country"],
                    self.products_df["category"],
                    self.products_df["sub_category"]
                )
            )
        except Exception as e:
            raise Exception(f"Failed to create aggregated profit DataFrame: {str(e)}")


    """
    Aggregates the profit data from the enriched orders DataFrame.

    This function takes an enriched orders DataFrame, extracts the year from the order date,
    and groups the data by order year, product category, sub-category, and customer name.
    It then calculates the total profit for each group, rounding it to two decimal places.

    Args:
        enriched_orders_df (DataFrame): The enriched orders DataFrame containing order, customer, and product details.

    Returns:
        DataFrame: A DataFrame with aggregated profit data grouped by order year, category, sub-category, and customer name.
    """
    def _create_aggregated_profit(self, enriched_orders_df: DataFrame) -> DataFrame:
        try:
            return (
                enriched_orders_df
                .withColumn("order_year", year("order_date"))
                .groupBy("order_year", "category", "sub_category", "customer_name")
                .agg(round(sum("rounded_profit"), 2).alias("total_profit"))
            )
        except Exception as e:
            raise Exception(f"Data transformation process failed: {str(e)}")


    def process(self) -> Dict[str, DataFrame]:
        enriched_df = self._create_enriched_orders()
        aggregated_df = self._create_aggregated_profit(enriched_df)

        return {
           ENRICHED_ORDERS_KEY : enriched_df,
           AGGREGATED_PROFIT_KEY : aggregated_df
        }
