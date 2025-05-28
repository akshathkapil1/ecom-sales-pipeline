from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from .constants import *

class DataExtractor():
    def __init__(self, spark: SparkSession):
        self.spark = spark


    """
    Extracts data from various sources and returns them as a dictionary of DataFrames.

    Returns:
        Dict[str, DataFrame]: A dictionary containing the following keys and their corresponding DataFrames:
            - PRODUCTS_KEY: DataFrame containing product data from a CSV file.
            - CUSTOMERS_KEY: DataFrame containing customer data from an Excel file.
            - ORDERS_KEY: DataFrame containing order data from a JSON file.

    Raises:
        FileNotFoundError: If any of the files cannot be read from their respective paths.
    """
    def extract(self) -> Dict[str, DataFrame]:
        try:
            products_df = (
                self.spark.read.format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(PRODUCTS_PATH)
            )
        except Exception as e:
            raise FileNotFoundError(f"Failed to read products CSV file from path {PRODUCTS_PATH}: {str(e)}")

        try:
            customers_df = (
                self.spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("sheetName", "Worksheet")
                .option("inferSchema", "true")
                .load(CUSTOMERS_PATH)
            )
        except Exception as e:
            raise FileNotFoundError(f"Failed to read customers Excel file from path {CUSTOMERS_PATH}: {str(e)}")

        try:
            orders_df = (
                self.spark.read.format("json")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load(ORDERS_PATH)
            )
        except Exception as e:
            raise FileNotFoundError(f"Failed to read orders JSON file from path {ORDERS_PATH}: {str(e)}")

        return {
            PRODUCTS_KEY: products_df,
            CUSTOMERS_KEY: customers_df,
            ORDERS_KEY: orders_df
        }

