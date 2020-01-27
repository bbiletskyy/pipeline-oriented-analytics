import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    yield spark
    spark.stop()

