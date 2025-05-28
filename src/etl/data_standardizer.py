import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, to_date, when, coalesce, regexp_replace, udf
from typing import Dict
from pyspark.sql.types import StringType
from .constants import *

class DataStandardizer:

    def __init__(self, dataframes: Dict[str, DataFrame]):
        self.dataframes = dataframes
        self._clean_phone_udf = udf(self._clean_phone, StringType())

    
    """
    Cleans and formats a phone number string.

    This method removes extensions, dots, and non-digit characters from the phone number.
    It formats valid 10-digit phone numbers as 'XXX-XXX-XXXX'. If the phone number
    contains more than 10 digits, it formats the first 10 digits in the same manner.
    Returns None for invalid phone numbers.

    Args:
        phone (str): The phone number string to be cleaned.

    Returns:
        str: The cleaned and formatted phone number, or None if invalid.
    """
    @staticmethod
    def _clean_phone(phone: str) -> str:
        if not phone:
            return None
        # Remove extensions, dots, x, etc.
        phone = re.sub(r"[xX#\.].*", "", phone)
        # Remove any non-digit characters
        digits = re.sub(r"[^\d]", "", phone)
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        elif len(digits) > 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:10]}"
        return None  # invalid phone
    

    """
    Standardizes column names by converting them to lowercase and replacing spaces or hyphens with underscores.

    Args:
        df (DataFrame): The DataFrame whose column names are to be standardized.

    Returns:
        DataFrame: A new DataFrame with standardized column names.
    """
    def _standardize_column_names(self, df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            new_col_name = col_name.strip().replace(" ", "_").replace("-", "_").lower()
            df = df.withColumnRenamed(col_name, new_col_name)
        return df
    
    
    """
    Cleans and formats product-related data in the DataFrame.

    - This method trims whitespace from product-related columns 
    - ensures the price_per_product is a valid double.
    - drops rows with null values in essential columns like product_id, category, sub_category, and product_name.

    Args:
        df (DataFrame): The DataFrame containing product data to be cleaned.

    Returns:
        DataFrame: A new DataFrame with cleaned product data.
    """
    def _clean_products(self, df: DataFrame) -> DataFrame:
        try:
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
        except Exception as e:
            print(f"Error cleaning products: {str(e)}")
            return df
    

    """
    Cleans and formats customer-related data in the DataFrame.

    - This method trims whitespace from customer-related columns 
    - removes white spaces and non-alphabetic characters from customer names
    - ensures the postal code is a valid integer. 
    - It drops rows with null values in essential columns.
    - Clean phone numbers using the _clean_phone method.

    Args:
        df (DataFrame): The DataFrame containing customer data to be cleaned.

    Returns:
        DataFrame: A new DataFrame with cleaned customer data.
    """
    def _clean_customers(self, df: DataFrame) -> DataFrame:
        try:
            return (
                df.withColumn("customer_id", trim(col("customer_id")))
                .withColumn("customer_name",regexp_replace(trim(col("customer_name")), r"[^a-zA-Z\s]", ""))
                .withColumn("customer_name", regexp_replace(col("customer_name"), "\\s+", " "))
                .withColumn("email", lower(trim(col("email"))))
                .withColumn("phone", trim(self._clean_phone_udf(col("phone"))))
                .withColumn("address", trim(col("address")))
                .withColumn("segment", trim(col("segment")))
                .withColumn("country", trim(col("country")))
                .withColumn("city", trim(col("city")))
                .withColumn("state", trim(col("state")))
                .withColumn("postal_code", when(col("postal_code").cast("int").isNotNull(), col("postal_code").cast("int")).otherwise(None))
                .withColumn("region", trim(col("region")))
                .na.drop(subset=["customer_id", "customer_name"])
            )
        except Exception as e:
            print(f"Error cleaning customers: {str(e)}")
            return df


    """
    Cleans and formats order-related data in the DataFrame.

    - Trims whitespace from order-related columns.
    - Ensures the discount is between 0 and 1, otherwise sets it to 0.0.
    - Parses the order_date and ship_date columns to date format handling multiple ambigous formats
    - Ensures price and profit are valid doubles, otherwise sets them to 0.0.
    - Ensures quantity is a valid integer, otherwise sets it to 0.
    - Drops rows with null values in essential columns like customer_id, order_id, and product_id

    Args:
        df (DataFrame): The DataFrame containing order data to be cleaned.

    Returns:
        DataFrame: A new DataFrame with cleaned order data.
    """
    def _clean_orders(self, df: DataFrame) -> DataFrame:
        try:
            return (
                df.withColumn("customer_id", trim(col("customer_id")))
                .withColumn("discount", 
                            when((col("discount") >= 0) & (col("discount") <= 1), col("discount"))
                            .otherwise(0.0))
                .withColumn("order_date", coalesce(
                    to_date(col("order_date"), "d/M/yyyy"), 
                    to_date(col("order_date"), "dd/M/yyyy"),
                    to_date(col("order_date"), "d/MM/yyyy"),
                    to_date(col("order_date"), "dd/MM/yyyy")))
                .withColumn("order_id", trim(col("order_id")))
                .withColumn("price", when(col("price").isNotNull(), col("price").cast("double")).otherwise(0.0))
                .withColumn("product_id", trim(col("product_id")))
                .withColumn("profit", when(col("profit").isNotNull(), col("profit").cast("double")).otherwise(0.0))
                .withColumn("quantity", when(col("quantity").isNotNull(), col("quantity").cast("int")).otherwise(0))
                .withColumn("ship_date", coalesce(
                    to_date(col("ship_date"), "d/M/yyyy"), 
                    to_date(col("ship_date"), "dd/M/yyyy"),
                    to_date(col("ship_date"), "d/MM/yyyy"),
                    to_date(col("ship_date"), "dd/MM/yyyy")))
                .withColumn("ship_mode", trim(col("ship_mode")))
                .na.drop(subset=["customer_id", "order_id", "product_id"])
            )
        except Exception as e:
            print(f"Error cleaning orders: {str(e)}")
            return df


    """
    Processes and standardizes the dataframes.

    This method standardizes column names and cleans the dataframes based on their type.
    It handles products, customers, and orders dataframes by applying respective cleaning methods.
    If an error occurs during processing, it logs the error and returns the original dataframe.

    Returns:
        Dict[str, DataFrame]: A dictionary with the standardized and cleaned dataframes.
    """
    def process(self) -> Dict[str, DataFrame]:
        standardized_cleaned = {}
        for name, df in self.dataframes.items():
            try:
                df = self._standardize_column_names(df)
                if name == PRODUCTS_KEY:
                    df = self._clean_products(df)
                elif name == CUSTOMERS_KEY:
                    df = self._clean_customers(df)
                elif name == ORDERS_KEY:
                    df = self._clean_orders(df)
                standardized_cleaned[name] = df
            except  Exception as e:
                print(f"Error processing {name} DataFrame: {str(e)}")
                standardized_cleaned[name] = df
        return standardized_cleaned
