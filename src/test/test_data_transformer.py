import pytest

from pyspark.sql import SparkSession
from pyspark.sql import Row

import src.etl.constants
import src.etl.data_standardizer
import src.etl.data_transformer

from src.etl.constants import *
from src.etl.data_transformer import DataTransformer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.getActiveSession()
    
@pytest.fixture
def sample_dataframes(spark):
    orders = spark.createDataFrame([
        Row(order_id="O1", customer_id="C1", product_id="P1", profit=100.1267, order_date="2023-04-10"),
        Row(order_id="O2", customer_id="C2", product_id="P2", profit=50.4589, order_date="2023-05-15")
    ])

    customers = spark.createDataFrame([
        Row(customer_id="C1", customer_name="Alice", country="US"),
        Row(customer_id="C2", customer_name="Bob", country="UK")
    ])

    products = spark.createDataFrame([
        Row(product_id="P1", category="Electronics", sub_category="Phones"),
        Row(product_id="P2", category="Furniture", sub_category="Chairs")
    ])

    return {
        ORDERS_KEY: orders,
        CUSTOMERS_KEY: customers,
        PRODUCTS_KEY: products
    }

def test_data_transformer_process(sample_dataframes):
    transformer = DataTransformer(sample_dataframes)
    result = transformer.process()

    enriched_df = result[ENRICHED_ORDERS_KEY]
    aggregated_df = result[AGGREGATED_PROFIT_KEY]

    # Assert enriched orders have expected columns
    enriched_columns = enriched_df.columns
    assert "rounded_profit" in enriched_columns
    assert "customer_name" in enriched_columns
    assert "country" in enriched_columns
    assert "category" in enriched_columns
    assert "sub_category" in enriched_columns

    enriched_rows = enriched_df.collect()
    assert len(enriched_rows) == 2
    assert enriched_rows[0]["rounded_profit"] == round(sample_dataframes[ORDERS_KEY].collect()[0]["profit"], 2)

    # Assert aggregated profit calculation
    aggregated_rows = aggregated_df.collect()
    assert len(aggregated_rows) == 2

    row1 = next(r for r in aggregated_rows if r["customer_name"] == "Alice")
    assert row1["order_year"] == 2023
    assert row1["total_profit"] == round(100.1267, 2)

    row2 = next(r for r in aggregated_rows if r["customer_name"] == "Bob")
    assert row2["total_profit"] == round(50.4589, 2)
