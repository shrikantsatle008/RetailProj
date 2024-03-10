
from lib.DataReader import read_customers,read_orders,count_orders_state,filter_orders_generic
from lib.DataManipulation import filter_closed_orders
from lib.ConfigReader import get_app_config



def test_read_customers_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

def test_read_orders_df(spark):
    
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

def test_filter_closed_orders(spark):
    
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

def test_read_app_config():
    config= get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"
def test_count_orders_state(spark,expected_results):
    customers_df = read_customers(spark,"LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_results.collect()


def test_check_closed_count(spark):
    orders_df = read_orders(spark,"LOCAAL")
    filtered_count = filter_orders_generic(orders_df,"CLOSED").count()
    assert filtered_count == 7556