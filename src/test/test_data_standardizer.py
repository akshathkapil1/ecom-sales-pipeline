import pytest
from pyspark.sql import SparkSession, Row
import src.etl.constants
import src.etl.data_standardizer

from src.etl.constants import *
from src.etl.data_standardizer import DataStandardizer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.getActiveSession()


def test_clean_products_edge_cases(spark):
    raw_df = spark.createDataFrame([
        Row(product_id=" 123 ", category=" Furniture ", sub_category="Chairs", product_name=" Chair ", state=" NY ", price_per_product="99.99"),
        Row(product_id="  ", category="Tech", sub_category="Phones", product_name="iPhone", state="CA", price_per_product="not_a_number"),
        Row(product_id=None, category=None, sub_category=None, product_name=None, state=None, price_per_product=None),
        Row(product_id="456", category="Office", sub_category="Supplies", product_name="Paper", state="IL", price_per_product="100"),
        Row(product_id="789", category="Furniture", sub_category="Tables", product_name="Desk", state="TX", price_per_product="200.5")
    ])
    
    standardizer = DataStandardizer({})
    cleaned_df = standardizer._clean_products(raw_df)

    result = cleaned_df.collect()
    assert len(result) == 3  # Drops rows with null critical fields record 2 and 3 should be dropped
    assert result[0].product_id == "123" # extra white spaces should be removed
    assert result[0].category == "Furniture" # extra white spaces should be removed
    assert result[0].price_per_product == 99.99 # product prices standardized to double
    assert result[1].price_per_product == 100.0 # product prices standardized to double from int


def test_clean_customers_edge_cases(spark):
    raw_df = spark.createDataFrame([
        Row(customer_id=" C1 ", customer_name="John@#$ Doe", email=" John@Email.COM ", phone="123.456.7890", address=" 123 st ", segment=" Consumer ", country="US", city="NY", state="NY", postal_code="10001", region="East"),
        Row(customer_id="C2", customer_name="   ", email="x", phone="(999)-999-9999 x123", address="Addr", segment="Seg", country="US", city="City", state="ST", postal_code="abc", region="North"),
        Row(customer_id=None, customer_name=None, email=None, phone=None, address=None, segment=None, country=None, city=None, state=None, postal_code=None, region=None),
        Row(customer_id="C3", customer_name="Jane", email="   jane@doe.com  ", phone="9876543210", address="456 st", segment="Seg", country="US", city="Boston", state="MA", postal_code="02134", region="East"),
        Row(customer_id="C4", customer_name="Alice Johnson", email="ALICE@MAIL.NET", phone="+1-800-123-4567", address="789 ave", segment="Seg", country="US", city="Chicago", state="IL", postal_code="60616", region="Midwest"),
    ])

    standardizer = DataStandardizer({})
    cleaned_df = standardizer._clean_customers(raw_df)
    result = cleaned_df.collect()

    assert len(result) == 4  # One row dropped due to nulls (Row 3)
    assert result[0].customer_name == "John Doe" # 'John@#$ Doe' non-numeric characters removed from customer_name
    assert result[0].email == "john@email.com"
    assert result[0].phone == "123-456-7890" # 123.456.7890 phone number cleaned
    assert result[1].postal_code is None  # 'abc' Non-numeric postal code 
    assert result[2].phone == "987-654-3210" # '9876543210' phone number formatted
    assert result[3].phone == "800-123-4567" # '+1-800-123-4567' phone number formatted


def test_clean_orders_edge_cases(spark):
    raw_df = spark.createDataFrame([
        Row(customer_id="C1", order_id="O1", product_id="P1", discount=0.15, order_date="1/1/2024", price="59.99", profit="15.50", quantity="2", ship_date="3/1/2024", ship_mode=" Standard "),
        Row(customer_id="C2", order_id="O2", product_id="P2", discount=1.2, order_date="15/1/2024", price="bad", profit="bad", quantity=None, ship_date="invalid", ship_mode="Express"),
        Row(customer_id="C3", order_id="O3", product_id="P3", discount=-0.1, order_date="31/2/2024", price=None, profit=None, quantity=None, ship_date=None, ship_mode=None),
        Row(customer_id=None, order_id=None, product_id=None, discount=None, order_date=None, price=None, profit=None, quantity=None, ship_date=None, ship_mode=None),
        Row(customer_id="C5", order_id="O5", product_id="P5", discount=0.3, order_date="1/11/2024", price="100", profit="25", quantity="5", ship_date="5/1/2024", ship_mode=" First Class ")
    ])

    standardizer = DataStandardizer({})
    cleaned_df = standardizer._clean_orders(raw_df)
    result = cleaned_df.collect()

    assert len(result) == 4  # One row dropped due to critical nulls (Row 3)
    assert result[0].discount == 0.15 

    # order dates in a variety of formats 
    assert result[0].order_date is not None # '1/1/2024' d/M/yyyy format
    assert result[1].order_date is not None # '15/1/2024' dd/M/yyyy format
    assert result[2].order_date is not None # '31/2/2024' dd/MM/yyyy format
    assert result[3].order_date is not None # '1/11/2024' d/MM/yyyy format

    assert result[1].discount == 0.0 # discount > 1 set to 0.0 for value = 1.2
    assert result[1].price == 0.0 # 'bad' string price value set to 0.0
    assert result[1].profit == 0.0 # 'bad' string price value set to 0.0
    assert result[1].ship_date is None # 'invalid' not a date format

    assert result[2].discount == 0.0 # discount > 1 set to 0.0 for value = -0.1
    assert result[3].quantity == 5
    assert result[2].ship_date is not None # '15/1/2024' dd/M/yyyy format
