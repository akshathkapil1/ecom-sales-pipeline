from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, to_date, when
from typing import Dict
from constants import *

class DataStandardizer:

    def __init__(self, dataframes: Dict[str, DataFrame]):
        self.dataframes = dataframes

    def _standardize_column_names(self, df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            new_col_name = col_name.strip().replace(" ", "_").replace("-", "_").lower()
            df = df.withColumnRenamed(col_name, new_col_name)
        return df

    def _clean_products(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("product_id", trim(col("product_id")))
              .withColumn("category", trim(col("category")))
              .withColumn("sub_category", trim(col("sub_category")))
              .withColumn("product_name", trim(col("product_name")))
              .withColumn("state", trim(col("state")))
              .withColumn("price_per_product", 
                          when(col("price_per_product").rlike("^\d+(\.\d+)?$"), 
                               col("price_per_product").cast("double"))
                          .otherwise(0.0))
              .na.drop(subset=["product_id", "category", "sub_category", "product_name"])
        )

    def _clean_customers(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("customer_id", trim(col("customer_id")))
              .withColumn("customer_name", trim(col("customer_name")))
              .withColumn("email", lower(trim(col("email"))))
              .withColumn("phone", trim(col("phone")))
              .withColumn("address", trim(col("address")))
              .withColumn("segment", trim(col("segment")))
              .withColumn("country", trim(col("country")))
              .withColumn("city", trim(col("city")))
              .withColumn("state", trim(col("state")))
              .withColumn("postal_code", trim(col("postal_code")))
              .withColumn("region", trim(col("region")))
              .na.drop(subset=["customer_id", "customer_name"])
        )

    def _clean_orders(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("customer_id", trim(col("customer_id")))
              .withColumn("discount", 
                          when((col("discount") >= 0) & (col("discount") <= 1), col("discount"))
                          .otherwise(0.0))
              .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
              .withColumn("order_id", trim(col("order_id")))
              .withColumn("price", when(col("price").isNotNull(), col("price").cast("double")).otherwise(0.0))
              .withColumn("product_id", trim(col("product_id")))
              .withColumn("profit", when(col("profit").isNotNull(), col("profit").cast("double")).otherwise(0.0))
              .withColumn("quantity", when(col("quantity").isNotNull(), col("quantity").cast("int")).otherwise(0))
              .withColumn("ship_date", to_date(col("ship_date"), "yyyy-MM-dd"))
              .withColumn("ship_mode", trim(col("ship_mode")))
              .na.drop(subset=["customer_id", "order_id", "product_id"])
        )

    def process(self) -> Dict[str, DataFrame]:
        standardized_cleaned = {}
        for name, df in self.dataframes.items():
            df = self._standardize_column_names(df)
            if name == PRODUCTS_KEY:
                df = self._clean_products(df)
            elif name == CUSTOMERS_KEY:
                df = self._clean_customers(df)
            elif name == ORDERS_KEY:
                df = self._clean_orders(df)
            standardized_cleaned[name] = df
        return standardized_cleaned
