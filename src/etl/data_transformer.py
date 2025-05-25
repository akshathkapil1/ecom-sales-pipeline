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

    def _create_enriched_orders(self) -> DataFrame:
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

    def _create_aggregated_profit(self, enriched_orders_df: DataFrame) -> DataFrame:
        return (
            enriched_orders_df
            .withColumn("order_year", year("order_date"))
            .groupBy("order_year", "category", "sub_category", "customer_name")
            .agg(round(sum("rounded_profit"), 2).alias("total_profit"))
        )

    def process(self) -> Dict[str, DataFrame]:
        enriched_df = self._create_enriched_orders()
        aggregated_df = self._create_aggregated_profit(enriched_df)

        return {
           ENRICHED_ORDERS_KEY : enriched_df,
           AGGREGATED_PROFIT_KEY : aggregated_df
        }
