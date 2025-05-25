from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from constants import *

class DataExtractor():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self) -> Dict[str, DataFrame]:

        products_df = (
            self.spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(PRODUCTS_PATH)
        )

        customers_df = (
            self.spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(CUSTOMERS_PATH)
        )

        orders_df = (
            self.spark.read.format("json")
            .option("multiline", "true")
            .option("inferSchema", "true")
            .load(ORDERS_PATH)
        )

        return {
            PRODUCTS_KEY: products_df,
            CUSTOMERS_KEY: customers_df,
            ORDERS_KEY: orders_df
        }

