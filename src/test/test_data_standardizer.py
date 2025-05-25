import pytest
from pyspark.sql import SparkSession, Row
import src.etl.constants
import src.etl.data_standardizer

from src.etl.constants import *
from src.etl.data_standardizer import DataStandardizer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture
def raw_dataframes(spark):
    product_df = spark.createDataFrame([
        Row(product_id=" P001 ", category="Tech", sub_category="Phones", product_name=" iPhone ", state="CA", price_per_product="999.99"),
        Row(product_id=" ", category="Furniture", sub_category="Chairs", product_name="", state="NY", price_per_product="abc")
    ])

    customer_df = spark.createDataFrame([
        Row(customer_id=" C001 ", customer_name="John@Doe!!", email=" John@EXAMPLE.Com ", phone="123", address=" 123 St ", segment="Consumer",
            country="US", city="LA", state="CA", postal_code="90001", region="West"),
        Row(customer_id=None, customer_name=None, email="test@example.com", phone="456", address=" 456 Ave ", segment="Corporate",
            country="US", city="NY", state="NY", postal_code="abc", region="East")
    ])

    order_df = spark.createDataFrame([
        Row(customer_id=" C001 ", discount=0.1, order_date="1/1/2020", order_id=" O001 ", price="100", product_id=" P001 ",
            profit="20", quantity="2", ship_date="2/1/2020", ship_mode="Standard"),
        Row(customer_id=" C002 ", discount=2.0, order_date="01/01/2020", order_id=None, price=None, product_id=None,
            profit=None, quantity=None, ship_date=None, ship_mode="Express")
    ])

    return {
        PRODUCTS_KEY: product_df,
        CUSTOMERS_KEY: customer_df,
        ORDERS_KEY: order_df
    }


def test_data_standardizer(raw_dataframes):
    standardizer = DataStandardizer(raw_dataframes)
    cleaned_dataframes = standardizer.process()

    # Check keys
    assert set(cleaned_dataframes.keys()) == {PRODUCTS_KEY, CUSTOMERS_KEY, ORDERS_KEY}

    # Check cleaned product DataFrame
    product_df = cleaned_dataframes[PRODUCTS_KEY]
    assert "product_id" in product_df.columns
    assert product_df.count() == 1  # second row should be dropped (empty product_id/product_name)

    # Check cleaned customer DataFrame
    customer_df = cleaned_dataframes[CUSTOMERS_KEY]
    assert "customer_name" in customer_df.columns
    cleaned_names = [r["customer_name"] for r in customer_df.select("customer_name").collect()]
    assert cleaned_names[0] == "John Doe"
    assert customer_df.count() == 1  # second row dropped (missing id and name)

    # Check cleaned order DataFrame
    order_df = cleaned_dataframes[ORDERS_KEY]
    assert order_df.count() == 1  # second row dropped (missing customer_id, order_id, product_id)
    assert order_df.filter(order_df.discount > 1).count() == 0  # invalid discount replaced with 0.0
